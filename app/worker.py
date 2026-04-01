import base64
import time
import random
from datetime import datetime, timezone

from app.db import SessionLocal
from app.models import RequestLog, ProviderSetting, AppSetting, GroupPromotion
from app.services.evolution import send_group_text, send_document_base64, send_group_document_base64
from app.config import settings
from app.utils.curp import provider_label_for_type, is_chain
from app.utils.provider_format import provider2_command
from app.services.provider3 import Provider3Client, decode_pdf_base64
from app.services.provider4 import Provider4Client
from app.queue import redis_conn

from zoneinfo import ZoneInfo

PROVIDER4_TEST_GROUPS = set()


def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mx_now():
    return datetime.now(ZoneInfo("America/Monterrey"))


def provider3_keepalive_job():
    db = SessionLocal()

    try:
        phpsessid = _get_app_setting(
            db,
            "PROVIDER3_PHPSESSID",
            settings.PROVIDER3_PHPSESSID
        )

        if not phpsessid:
            print("KEEPALIVE_SKIP_NO_SID", flush=True)
            return {"ok": False, "error": "no_sid"}

        client = Provider3Client(phpsessid=phpsessid)

        warm = client.warm_session(with_user_check=False)
        print("KEEPALIVE_WARM =", warm, flush=True)

        result = client.keepalive(jitter_seconds=(0.2, 1.2))
        print("KEEPALIVE_OK =", result, flush=True)

        try:
            licenses = client.get_licenses()
            print("KEEPALIVE_LICENSES =", licenses, flush=True)
        except Exception as lic_exc:
            print("KEEPALIVE_LICENSES_ERROR =", str(lic_exc), flush=True)

        return {"ok": True}

    except Exception as e:
        print("KEEPALIVE_ERROR", str(e), flush=True)
        return {"ok": False, "error": str(e)}

    finally:
        db.close()
        

def _get_or_create_provider(db, provider_name: str, default_enabled: bool):
    row = db.query(ProviderSetting).filter(ProviderSetting.provider_name == provider_name).first()
    if row:
        return row

    row = ProviderSetting(
        provider_name=provider_name,
        is_enabled=default_enabled,
        created_at=_utc_now_naive(),
        updated_at=_utc_now_naive(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _get_app_setting(db, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if not row or row.value is None:
        return default
    return row.value.strip()
    

def _enabled_providers(db) -> list[str]:
    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)
    p3 = _get_or_create_provider(db, "PROVIDER3", False)
    p4 = _get_or_create_provider(db, "PROVIDER4", False)

    enabled = []
    if p1.is_enabled:
        enabled.append("PROVIDER1")
    if p2.is_enabled:
        enabled.append("PROVIDER2")
    if p3.is_enabled:
        enabled.append("PROVIDER3")
    if p4.is_enabled:
        enabled.append("PROVIDER4")

    return enabled


def _pick_provider_name(db, request_id: int, source_group_id: str | None = None) -> str:
    enabled = _enabled_providers(db)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    # Provider4 solo para grupos de prueba
    if source_group_id and source_group_id in PROVIDER4_TEST_GROUPS and "PROVIDER4" in enabled:
        return "PROVIDER4"

    # Para todos los demás, excluir PROVIDER4 del reparto normal
    enabled_without_p4 = [p for p in enabled if p != "PROVIDER4"]

    if not enabled_without_p4:
        # Si solo está habilitado PROVIDER4 pero el grupo no es de prueba,
        # evitamos que lo tome otro grupo.
        raise RuntimeError("PROVIDER4_ONLY_ALLOWED_FOR_TEST_GROUPS")

    if len(enabled_without_p4) == 1:
        return enabled_without_p4[0]

    idx = (request_id - 1) % len(enabled_without_p4)
    return enabled_without_p4[idx]


def _pick_provider1_group(act_type: str, request_id: int) -> str:
    act_type = (act_type or "").upper().strip()

    nacimiento_groups = [
        settings.PROVIDER_GROUP_NACIMIENTO_1,
        settings.PROVIDER_GROUP_NACIMIENTO_2,
        settings.PROVIDER_GROUP_NACIMIENTO_3,
    ]
    nacimiento_groups = [g for g in nacimiento_groups if g]

    especiales_group = settings.PROVIDER_GROUP_ESPECIALES

    if act_type == "NACIMIENTO":
        if not nacimiento_groups:
            raise RuntimeError("NO_BIRTH_PROVIDER_GROUPS_CONFIGURED")
        idx = (request_id - 1) % len(nacimiento_groups)
        return nacimiento_groups[idx]

    if not especiales_group:
        raise RuntimeError("NO_SPECIAL_PROVIDER_GROUP_CONFIGURED")

    return especiales_group


def _pick_provider_group(provider_name: str, act_type: str, request_id: int) -> str | None:
    if provider_name == "PROVIDER1":
        return _pick_provider1_group(act_type, request_id)

    if provider_name == "PROVIDER2":
        provider2_groups = [
            settings.PROVIDER2_GROUP_1,
            settings.PROVIDER2_GROUP_2,
        ]
        provider2_groups = [g for g in provider2_groups if g]

        if not provider2_groups:
            raise RuntimeError("PROVIDER2_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider2_groups)
        return provider2_groups[idx]

    if provider_name == "PROVIDER3":
        return None

    if provider_name == "PROVIDER4":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _build_provider_message(provider_name: str, term: str, act_type: str) -> str | None:
    if provider_name == "PROVIDER1":
        provider_type = provider_label_for_type(act_type)
        return f"{term} {provider_type}"

    if provider_name == "PROVIDER2":
        return provider2_command(term, act_type)

    if provider_name == "PROVIDER3":
        return None

    if provider_name == "PROVIDER4":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _provider3_flags(act_type: str) -> dict:
    act_type = (act_type or "").upper().strip()

    is_folio = "FOL" in act_type

    return {
        "folio1": is_folio,
        "folio2": False,
        "reverso": True,
        "margen": True,
    }


def _provider3_tipo_acta(act_type: str) -> str:
    act_type = (act_type or "").upper().strip()

    mapping = {
        "NACIMIENTO": "nacimiento",
        "NACIMIENTO FOLIO": "nacimiento",
        "MATRIMONIO": "matrimonio",
        "MATRIMONIO FOLIO": "matrimonio",
        "DEFUNCION": "defuncion",
        "DEFUNCION FOLIO": "defuncion",
        "DIVORCIO": "divorcio",
        "DIVORCIO FOLIO": "divorcio",
    }
    return mapping.get(act_type, "nacimiento")


def _provider4_tipo_acta(act_type: str) -> str:
    act_type = (act_type or "").upper().strip()

    mapping = {
        "NACIMIENTO": "nacimiento",
        "NACIMIENTO FOLIO": "nacimiento",
        "MATRIMONIO": "matrimonio",
        "MATRIMONIO FOLIO": "matrimonio",
        "DEFUNCION": "defuncion",
        "DEFUNCION FOLIO": "defuncion",
        "DIVORCIO": "divorcio",
        "DIVORCIO FOLIO": "divorcio",
    }
    return mapping.get(act_type, "nacimiento")


def _process_provider3(req, db):
    phpsessid = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)

    masked = ""
    if phpsessid:
        if len(phpsessid) <= 8:
            masked = "*" * len(phpsessid)
        else:
            masked = phpsessid[:4] + ("*" * (len(phpsessid) - 8)) + phpsessid[-4:]

    print("PROVIDER3_PHPSESSID_MASKED =", masked, flush=True)

    client = Provider3Client(phpsessid=phpsessid)
    flags = _provider3_flags(req.act_type)

    def _run_request():
        if is_chain(req.curp):
            return client.generar_por_cadena(
                cadena=req.curp,
                folio1=flags["folio1"],
                folio2=flags["folio2"],
                reverso=flags["reverso"],
                margen=flags["margen"],
            )
        else:
            tipo_acta = _provider3_tipo_acta(req.act_type)
            return client.generar_por_curp(
                curp=req.curp,
                tipo_acta=tipo_acta,
                folio1=flags["folio1"],
                folio2=flags["folio2"],
                reverso=flags["reverso"],
                margen=flags["margen"],
            )

    try:
        result = _run_request()
    except RuntimeError as e:
        err = str(e)

        if err.startswith("PROVIDER3_RATE_LIMIT"):
            print("PROVIDER3_RATE_LIMIT_RETRYING", flush=True)
            time.sleep(2)
            result = _run_request()
        else:
            raise

    pdf_b64 = result.get("pdf") or ""
    if not pdf_b64:
        raise RuntimeError(f"PROVIDER3_NO_PDF: {result}")

    pdf_bytes = decode_pdf_base64(pdf_b64)

    return {
        "remaining": result.get("remaining"),
        "raw_result": result,
        "pdf_bytes": pdf_bytes,
    }


def _process_provider4(req, db):

    if PROVIDER4_TEST_GROUPS and req.source_group_id not in PROVIDER4_TEST_GROUPS:
        raise RuntimeError("PROVIDER4_NOT_ALLOWED_GROUP")

    client = Provider4Client()

    tipoa = _provider4_tipo_acta(req.act_type)
    inc_folio = "FOLIO" in (req.act_type or "").upper().strip()

    pdf_bytes = client.process_and_download(
        term=req.curp,
        tipoa=tipoa,
        inc_folio=inc_folio,
        is_chain=is_chain(req.curp),
    )

    return {
        "pdf_bytes": pdf_bytes,
    }
    

def _handle_group_promotion_after_done(req, db):
    if not req.source_group_id:
        return

    promo = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid == req.source_group_id,
            GroupPromotion.is_active == True
        )
        .with_for_update()
        .first()
    )

    if not promo:
        return

    promo.used_actas = (promo.used_actas or 0) + 1
    promo.updated_at = _utc_now_naive()

    available = max(0, (promo.total_actas or 0) - (promo.used_actas or 0))

    msg = None

    if available <= 0 and not promo.warning_sent_0:
        msg = (
            f"❌ *Paquete agotado*\n\n"
            f"Tu paquete promocional de *{promo.total_actas} actas* ha sido consumido en su totalidad.\n\n"
            f"El grupo quedará bloqueado automáticamente hasta nueva recarga o activación.\n\n"
            f"Quedamos atentos."
        )
        promo.warning_sent_0 = True
        promo.is_active = False

    elif available <= 10 and not promo.warning_sent_10:
        msg = (
            f"🚨 *Saldo crítico*\n\n"
            f"Tu paquete promocional cuenta actualmente con solo *{available} actas disponibles*.\n\n"
            f"Es importante realizar tu recarga cuanto antes para no quedarte sin servicio.\n\n"
            f"Quedamos atentos."
        )
        promo.warning_sent_10 = True

    elif available <= 50 and not promo.warning_sent_50:
        msg = (
            f"⚠️ *Aviso importante de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available} actas disponibles*.\n\n"
            f"Para evitar interrupciones en el servicio, te recomendamos realizar tu recarga a la brevedad.\n\n"
            f"Quedamos atentos."
        )
        promo.warning_sent_50 = True

    elif available <= 100 and not promo.warning_sent_100:
        msg = (
            f"⚠️ *Aviso de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available} actas disponibles*.\n\n"
            f"Te recomendamos considerar tu próxima recarga con anticipación para evitar interrupciones.\n\n"
            f"Quedamos atentos."
        )
        promo.warning_sent_100 = True

    elif available <= 200 and not promo.warning_sent_200:
        msg = (
            f"ℹ️ *Aviso de saldo*\n\n"
            f"Actualmente cuentas con *{available} actas disponibles* de tu paquete promocional.\n\n"
            f"Quedamos atentos."
        )
        promo.warning_sent_200 = True

    db.commit()

    if available <= 0:
        try:
            from app.main import block_group
            block_group(req.source_group_id)
        except Exception as block_exc:
            print("PROMOTION_AUTO_BLOCK_ERROR =", str(block_exc), flush=True)

    if msg:
        notify_key = f"promo_notify:{req.source_group_id}:{available}"
        first_notify = redis_conn.set(notify_key, "1", ex=1800, nx=True)

        if first_notify:
            send_group_text(req.source_group_id, msg)
        else:
            print("PROMOTION_NOTIFY_DUPLICATE_IGNORED =", notify_key, flush=True)


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        process_started_ts = time.perf_counter()

        req.status = "PROCESSING"
        req.updated_at = _utc_now_naive()
        db.commit()

        provider_name = _pick_provider_name(db, req.id, req.source_group_id)
        provider_group_id = _pick_provider_group(provider_name, req.act_type, req.id)
        text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

        req.provider_name = provider_name
        req.provider_group_id = provider_group_id
        req.provider_message = text_to_provider
        req.updated_at = _utc_now_naive()
        db.commit()

        print("WORKER_PROVIDER_NAME =", provider_name, flush=True)
        print("WORKER_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
        print("WORKER_TEXT_TO_PROVIDER =", text_to_provider, flush=True)

        if provider_name in ("PROVIDER1", "PROVIDER2"):
            send_group_text(provider_group_id, text_to_provider)
            return

        if provider_name == "PROVIDER3":
            provider3_result = _process_provider3(req, db)
        
            pdf_bytes = provider3_result["pdf_bytes"]
            safe_media_b64 = base64.b64encode(pdf_bytes).decode()
        
            total_seconds = max(0.0, time.perf_counter() - process_started_ts)
        
            if total_seconds >= 60:
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                tiempo = f"{minutes} min {seconds:.2f} segundos"
            else:
                tiempo = f"{total_seconds:.2f} segundos"
        
            NO_TIME_CAPTION_GROUPS = {
                "120363408668441985@g.us",
                "120363421166637606@g.us",
            }
        
            caption_text = ""
            if req.source_group_id not in NO_TIME_CAPTION_GROUPS:
                caption_text = f"⏱️ Tiempo de proceso: {tiempo}"
        
            print("PROVIDER3_CAPTION =", caption_text, flush=True)
        
            delivery_key = f"provider3_delivery:{req.id}:{req.curp}:{req.source_group_id or req.requester_wa_id}"
        
            # Si ya se entregó antes, no volver a mandar
            if redis_conn.exists(delivery_key):
                print("PROVIDER3_DUPLICATE_DELIVERY_IGNORED =", delivery_key, flush=True)
                return
        
            print(
                "PROVIDER3_DELIVERING_TO =",
                {
                    "req_id": req.id,
                    "source_group_id": req.source_group_id,
                    "requester_wa_id": req.requester_wa_id,
                    "curp": req.curp,
                },
                flush=True,
            )
        
            send_ok = False
        
            for attempt in range(3):
                try:
                    if req.source_group_id:
                        send_group_document_base64(
                            req.source_group_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text
                        )
                    else:
                        send_document_base64(
                            req.requester_wa_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text
                        )
        
                    send_ok = True
                    print(f"PROVIDER3_SEND_OK_ATTEMPT_{attempt+1} =", req.id, flush=True)
                    break
        
                except Exception as e:
                    print(f"PDF_SEND_ATTEMPT_{attempt+1}_ERROR =", str(e), flush=True)
                    if attempt == 2:
                        raise
                    time.sleep(2)
        
            if not send_ok:
                raise RuntimeError("PROVIDER3_PDF_SEND_FAILED")
        
            # Marcar duplicado solo después de envío exitoso
            redis_conn.set(delivery_key, "1", ex=3600)
        
            req.provider_media_url = "BASE64_PROVIDER3"
            req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = _utc_now_naive()
            db.commit()
        
            try:
                _handle_group_promotion_after_done(req, db)
            except Exception as promo_exc:
                print("PROMOTION_UPDATE_ERROR =", str(promo_exc), flush=True)
        
            return

        if provider_name == "PROVIDER4":
            provider4_result = _process_provider4(req, db)
        
            pdf_bytes = provider4_result["pdf_bytes"]
            safe_media_b64 = base64.b64encode(pdf_bytes).decode()
        
            total_seconds = max(0.0, time.perf_counter() - process_started_ts)
        
            if total_seconds >= 60:
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                tiempo = f"{minutes} min {seconds:.2f} segundos"
            else:
                tiempo = f"{total_seconds:.2f} segundos"
        
            NO_TIME_CAPTION_GROUPS = {
                "120363408668441985@g.us",
                "120363421166637606@g.us",
            }
        
            caption_text = ""
            if req.source_group_id not in NO_TIME_CAPTION_GROUPS:
                caption_text = f"⏱️ Tiempo de proceso: {tiempo}"
        
            print("PROVIDER4_CAPTION =", caption_text, flush=True)
        
            delivery_key = f"provider4_delivery:{req.id}:{req.curp}:{req.source_group_id or req.requester_wa_id}"
        
            if redis_conn.exists(delivery_key):
                print("PROVIDER4_DUPLICATE_DELIVERY_IGNORED =", delivery_key, flush=True)
                return
        
            send_ok = False
        
            for attempt in range(3):
                try:
                    if req.source_group_id:
                        send_group_document_base64(
                            req.source_group_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text
                        )
                    else:
                        send_document_base64(
                            req.requester_wa_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text
                        )
        
                    send_ok = True
                    print(f"PROVIDER4_SEND_OK_ATTEMPT_{attempt+1} =", req.id, flush=True)
                    break
        
                except Exception as e:
                    print(f"PROVIDER4_SEND_ATTEMPT_{attempt+1}_ERROR =", str(e), flush=True)
                    if attempt == 2:
                        raise
                    time.sleep(2)
        
            if not send_ok:
                raise RuntimeError("PROVIDER4_PDF_SEND_FAILED")
        
            redis_conn.set(delivery_key, "1", ex=3600)
        
            req.provider_media_url = "BASE64_PROVIDER4"
            req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = _utc_now_naive()
            db.commit()
        
            try:
                _handle_group_promotion_after_done(req, db)
            except Exception as promo_exc:
                print("PROMOTION_UPDATE_ERROR =", str(promo_exc), flush=True)
        
            return

        raise RuntimeError("UNKNOWN_PROVIDER")

    except Exception as e:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        err = str(e)
    
        if req:
            req.updated_at = _utc_now_naive()
    
            if err.startswith("PROVIDER3_NO_RECORD:"):
                req.status = "ERROR"
                req.error_message = err
                db.commit()
            
                msg = (
                    f"❌ No hay registros disponibles.\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Verificar que la CURP esté certificada en RENAPO"
                )
    
                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
                    
                return

            if err.startswith("PROVIDER3_RATE_LIMIT"):
                req.status = "ERROR"
                req.error_message = err
                db.commit()

                msg = (
                    f"⏳ El proveedor está saturado por demasiadas solicitudes.\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Intenta nuevamente en unos minutos"
                )

                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
    
                return

            if (
                err.startswith("PROVIDER3_SESSION_INVALID_OR_EXPIRED:")
                or err.startswith("PROVIDER3_NO_CREDITS:")
            ):
                req.status = "ERROR"
                req.error_message = err
                db.commit()
    
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Reenviar nuevamente en unos minutos"
                )
    
                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
    
                return

            req.status = "ERROR"
            req.error_message = err
            db.commit()
        raise
        
    finally:
        db.close()
