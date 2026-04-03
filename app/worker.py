import base64
import time
import re
import random
from datetime import datetime, timezone

from app.db import SessionLocal
from sqlalchemy.orm import Session

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


CURP_RE = re.compile(
    r"^[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d$",
    re.IGNORECASE
)


def _is_curp_term(value: str | None) -> bool:
    v = (value or "").strip().upper()
    return bool(CURP_RE.match(v))


def _fallback_to_provider3_web(req, db, process_started_ts):
    req.provider_name = "PROVIDER3"
    req.provider_group_id = None
    req.provider_message = None
    req.updated_at = _utc_now_naive()
    db.commit()

    print("FALLBACK_TO_PROVIDER3_WEB =", {"req_id": req.id, "curp": req.curp, "act_type": req.act_type}, flush=True)

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

    caption_text = f"⏱️ Tiempo de proceso: {tiempo}"

    filename = (
        f"{req.curp}_FOLIO.pdf"
        if "FOLIO" in (req.act_type or "").upper()
        else f"{req.curp}.pdf"
    )

    if req.source_group_id:
        send_group_document_base64(
            req.source_group_id,
            safe_media_b64,
            filename=filename,
            caption=caption_text
        )
    else:
        send_document_base64(
            req.requester_wa_id,
            safe_media_b64,
            filename=filename,
            caption=caption_text
        )

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


def _promo_client_key(group_jid: str | None, promo_name: str | None = None, client_key: str | None = None) -> str:
    return (client_key or promo_name or group_jid or "").strip().upper()


def _get_client_promotions(db: Session, source_group_id: str) -> list:
    base = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == source_group_id)
        .with_for_update()
        .first()
    )
    if not base:
        return []

    key = _promo_client_key(
        base.group_jid,
        getattr(base, "promo_name", None),
        getattr(base, "client_key", None),
    )

    rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.client_key == key)
        .with_for_update()
        .all()
    )
    return rows or [base]


def _notify_client_groups(rows: list, message: str):
    sent = set()
    for row in rows:
        gid = (row.group_jid or "").strip()
        if gid and gid not in sent:
            try:
                send_group_text(gid, message)
                sent.add(gid)
            except Exception as e:
                print("PROMO_NOTIFY_GROUP_ERROR =", gid, str(e), flush=True)


def _block_client_groups(rows: list):
    from app.main import block_group

    for row in rows:
        gid = (row.group_jid or "").strip()
        if gid:
            try:
                block_group(gid)
            except Exception as e:
                print("PROMO_AUTO_BLOCK_ERROR =", gid, str(e), flush=True)


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


def _pick_provider_name(
    db,
    request_id: int,
    source_group_id: str | None = None,
    term: str | None = None,
) -> str:
    enabled = _enabled_providers(db)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    if not _is_curp_term(term):
        if "PROVIDER3" in enabled:
            return "PROVIDER3"
        raise RuntimeError("NO_PROVIDER_FOR_CHAIN_OR_CODE")

    if PROVIDER4_TEST_GROUPS:
        if (
            source_group_id
            and source_group_id in PROVIDER4_TEST_GROUPS
            and "PROVIDER4" in enabled
        ):
            return "PROVIDER4"

        enabled = [p for p in enabled if p != "PROVIDER4"]

        if not enabled:
            raise RuntimeError("NO_PROVIDER_ENABLED")

    if len(enabled) == 1:
        return enabled[0]

    idx = (request_id - 1) % len(enabled)
    return enabled[idx]


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

    if not _is_curp_term(req.curp):
        raise RuntimeError("PROVIDER4_NOT_CURP")

    if PROVIDER4_TEST_GROUPS and req.source_group_id not in PROVIDER4_TEST_GROUPS:
        raise RuntimeError("PROVIDER4_NOT_ALLOWED_GROUP")

    client = Provider4Client()

    tipoa = _provider4_tipo_acta(req.act_type)
    inc_folio = "FOLIO" in (req.act_type or "").upper().strip()

    pdf_bytes = client.process_and_download(
        term=req.curp,
        tipoa=tipoa,
        inc_folio=inc_folio,
        is_chain=False,
    )

    return {
        "pdf_bytes": pdf_bytes,
    }
    

def _handle_group_promotion_after_done(req, db):
    if not req.source_group_id:
        return

    current = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid == req.source_group_id,
            GroupPromotion.is_active == True
        )
        .first()
    )

    if not current:
        return

    shared_key = (current.shared_key or "").strip()

    if shared_key:
        rows = (
            db.query(GroupPromotion)
            .filter(
                GroupPromotion.shared_key == shared_key,
                GroupPromotion.is_active == True
            )
            .all()
        )
    else:
        rows = [current]

    if not rows:
        return

    leader = rows[0]

    total_before = int(leader.total_actas or 0)
    used_before = int(leader.used_actas or 0)
    available_before = max(0, total_before - used_before)

    used_after = used_before + 1
    available_after = max(0, total_before - used_after)

    for row in rows:
        row.total_actas = total_before
        row.used_actas = used_after
        row.updated_at = _utc_now_naive()

    msg = None
    notify_level = None

    crossed_0 = available_before > 0 and available_after <= 0 and not bool(getattr(leader, "warning_sent_0", False))
    crossed_10 = available_before > 10 and available_after <= 10 and not bool(getattr(leader, "warning_sent_10", False))
    crossed_50 = available_before > 50 and available_after <= 50 and not bool(getattr(leader, "warning_sent_50", False))
    crossed_100 = available_before > 100 and available_after <= 100 and not bool(getattr(leader, "warning_sent_100", False))
    crossed_200 = available_before > 200 and available_after <= 200 and not bool(getattr(leader, "warning_sent_200", False))

    if crossed_0:
        msg = (
            f"❌ *Paquete agotado*\n\n"
            f"Tu paquete promocional ha sido consumido en su totalidad.\n"
            f"Saldo disponible: *0 actas*.\n\n"
            f"{'Todos los grupos asociados a esta bolsa quedarán bloqueados automáticamente hasta nueva recarga.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "0"
        for row in rows:
            row.warning_sent_0 = True
            row.is_active = False

    elif crossed_10:
        msg = (
            f"🚨 *Saldo crítico*\n\n"
            f"Tu paquete promocional cuenta actualmente con solo *{available_after} actas disponibles*.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "10"
        for row in rows:
            row.warning_sent_10 = True

    elif crossed_50:
        msg = (
            f"⚠️ *Aviso importante de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "50"
        for row in rows:
            row.warning_sent_50 = True

    elif crossed_100:
        msg = (
            f"⚠️ *Aviso de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "100"
        for row in rows:
            row.warning_sent_100 = True

    elif crossed_200:
        msg = (
            f"ℹ️ *Aviso de saldo*\n\n"
            f"Actualmente cuentas con *{available_after} actas disponibles* en tu paquete promocional.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa.' if shared_key else ''}"
        )
        notify_level = "200"
        for row in rows:
            row.warning_sent_200 = True

    db.commit()

    if crossed_0:
        try:
            _block_client_groups(rows if shared_key else [current])
        except Exception as e:
            print("PROMOTION_BLOCK_ERROR =", str(e), flush=True)

    if msg and notify_level:
        notify_scope = shared_key if shared_key else current.group_jid
        notify_key = f"promo_notify:{notify_scope}:{notify_level}"
        first_notify = redis_conn.set(notify_key, "1", ex=1800, nx=True)

        if first_notify:
            if shared_key:
                _notify_client_groups(rows, msg)
            else:
                try:
                    send_group_text(current.group_jid, msg)
                except Exception as e:
                    print("PROMOTION_SINGLE_GROUP_NOTIFY_ERROR =", str(e), flush=True)
        else:
            print("PROMOTION_NOTIFY_DUPLICATE_IGNORED =", notify_key, flush=True)


def _start_provider3_flow(req, db):
    provider_name = "PROVIDER3"
    provider_group_id = _pick_provider_group(provider_name, req.act_type, req.id)
    text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

    req.provider_name = provider_name
    req.provider_group_id = provider_group_id
    req.provider_message = text_to_provider
    req.updated_at = _utc_now_naive()
    db.commit()

    print("FALLBACK_PROVIDER_NAME =", provider_name, flush=True)
    print("FALLBACK_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
    print("FALLBACK_PROVIDER_TEXT =", text_to_provider, flush=True)

    send_group_text(provider_group_id, text_to_provider)


def _validate_pdf_matches_term(pdf_bytes: bytes, term: str) -> bool:
    term = (term or "").strip().upper()
    if not term:
        return True

    try:
        text = pdf_bytes.decode("latin1", errors="ignore").upper()
    except Exception:
        return True

    if not text or len(text.strip()) < 100:
        return True

    normalized_text = re.sub(r"[^A-Z0-9]", "", text)
    normalized_term = re.sub(r"[^A-Z0-9]", "", term)

    if normalized_term and normalized_term in normalized_text:
        return True

    found_curps = set(
        re.findall(
            r"[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d",
            normalized_text,
            flags=re.IGNORECASE,
        )
    )

    if normalized_term in found_curps:
        return True

    if found_curps and normalized_term not in found_curps:
        print("PROVIDER4_VALIDATE_EXPECTED_CURP =", normalized_term, flush=True)
        print("PROVIDER4_VALIDATE_FOUND_CURPS =", sorted(found_curps), flush=True)
        return False

    return True


def _validate_act_type_pdf(pdf_bytes: bytes, act_type: str | None) -> bool:
    try:
        text = pdf_bytes.decode(errors="ignore").upper()
    except Exception:
        return True

    act_type = (act_type or "").upper()

    # Si no hay texto suficiente en el PDF, no bloquear
    if not text or len(text.strip()) < 100:
        return True

    if "NAC" in act_type:
        if "MATRIMONIO" in text or "DIVORCIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return True

    if "MAT" in act_type:
        if "NACIMIENTO" in text or "DIVORCIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return True

    if "DIV" in act_type:
        if "NACIMIENTO" in text or "MATRIMONIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return True

    if "DEF" in act_type:
        if "NACIMIENTO" in text or "MATRIMONIO" in text or "DIVORCIO" in text:
            return False
        return True

    return True


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

        provider_name = _pick_provider_name(db, req.id, req.source_group_id, req.curp)
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
            provider4_started_ts = time.perf_counter()
        
            try:
                provider4_result = _process_provider4(req, db)

                pdf_bytes = provider4_result["pdf_bytes"]
                
                if not _validate_act_type_pdf(pdf_bytes, req.act_type):
                    raise RuntimeError("PROVIDER4_WRONG_ACT_TYPE")
                
                if not _validate_pdf_matches_term(pdf_bytes, req.curp):
                    print("PROVIDER4_VALIDATE_FAIL_REQ_CURP =", req.curp, flush=True)
                    raise RuntimeError(f"PROVIDER4_WRONG_CURP_IN_PDF:{req.curp}")
        
            except Exception as p4_exc:
                p4_err = str(p4_exc)
                p4_elapsed = time.perf_counter() - provider4_started_ts
                enabled = _enabled_providers(db)
        
                if p4_err.startswith("PROVIDER4_NOT_CURP"):
                    if "PROVIDER3" not in enabled:
                        msg = (
                            "⚠️ *Formato no disponible actualmente*\n\n"
                            "Las consultas por *cadena o código de verificación* "
                            "no están disponibles en este momento.\n\n"
                            "Intenta nuevamente más tarde o realiza la búsqueda por *CURP*."
                        )
        
                        if req.source_group_id:
                            send_group_text(req.source_group_id, msg)
                        else:
                            from app.services.evolution import send_text
                            send_text(req.requester_wa_id, msg)
        
                        req.status = "ERROR"
                        req.error_message = "NO_PROVIDER_FOR_CHAIN_OR_CODE"
                        req.updated_at = _utc_now_naive()
                        db.commit()
                        return
        
                    print("PROVIDER4_NOT_CURP_FALLBACK_TO_PROVIDER3 =", req.curp, flush=True)
                    _fallback_to_provider3_web(req, db, process_started_ts)
                    return
        
                fallback_errors = (
                    p4_err.startswith("PROVIDER4_BACKEND_FAILED:")
                    or p4_err.startswith("PROVIDER4_VGET_FAILED:")
                    or p4_err.startswith("PROVIDER4_HISTORY_FAILED:")
                    or p4_err.startswith("PROVIDER4_NO_PDF_LINK_FOR:")
                    or p4_err.startswith("PROVIDER4_NO_FOLIO_LINK_FOR:")
                    or p4_err.startswith("PROVIDER4_DOWNLOAD_FAILED:")
                    or p4_err.startswith("PROVIDER4_FOLIO_DOWNLOAD_FAILED:")
                    or p4_err.startswith("PROVIDER4_WRONG_ACT_TYPE")
                    or p4_err.startswith("PROVIDER4_WRONG_CURP_IN_PDF")
                    or "Read timed out" in p4_err
                )
        
                should_fallback = (
                    p4_err.startswith("PROVIDER4_WRONG_ACT_TYPE")
                    or p4_err.startswith("PROVIDER4_WRONG_CURP_IN_PDF")
                    or (fallback_errors and p4_elapsed >= 90)
                )
        
                if should_fallback:
                    if "PROVIDER3" not in enabled:
                        msg = (
                            "⚠️ *Proveedor temporalmente no disponible*\n\n"
                            "La búsqueda no pudo completarse correctamente en este momento.\n\n"
                            "Intenta nuevamente más tarde."
                        )
        
                        if req.source_group_id:
                            send_group_text(req.source_group_id, msg)
                        else:
                            from app.services.evolution import send_text
                            send_text(req.requester_wa_id, msg)
        
                        req.status = "ERROR"
                        req.error_message = f"PROVIDER4_FALLBACK_NO_PROVIDER3:{p4_err}"
                        req.updated_at = _utc_now_naive()
                        db.commit()
                        return
        
                    print(
                        "PROVIDER4_FALLBACK_TO_PROVIDER3 =",
                        {"req_id": req.id, "elapsed": p4_elapsed, "err": p4_err},
                        flush=True,
                    )
                    _fallback_to_provider3_web(req, db, process_started_ts)
                    return
        
                raise
        
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
        
            filename = (
                f"{req.curp}_FOLIO.pdf"
                if "FOLIO" in (req.act_type or "").upper()
                else f"{req.curp}.pdf"
            )
        
            for attempt in range(3):
                try:
                    if req.source_group_id:
                        send_group_document_base64(
                            req.source_group_id,
                            safe_media_b64,
                            filename=filename,
                            caption=caption_text
                        )
                    else:
                        send_document_base64(
                            req.requester_wa_id,
                            safe_media_b64,
                            filename=filename,
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

            if err == "NO_PROVIDER_FOR_CHAIN_OR_CODE":
                req.status = "ERROR"
                req.error_message = err
                db.commit()

                msg = (
                    "⚠️ *Formato no disponible actualmente*\n\n"
                    "Las consultas por *cadena o código de verificación* "
                    "no están disponibles en este momento.\n\n"
                    "Intenta nuevamente más tarde o realiza la búsqueda por *CURP*."
                )

                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)

                return

            if err == "NO_PROVIDER_ENABLED":
                req.status = "ERROR"
                req.error_message = err
                db.commit()

                msg = (
                    "⚠️ *Proveedores no disponibles*\n\n"
                    "En este momento no hay proveedores activos para procesar la solicitud.\n\n"
                    "Intenta nuevamente más tarde."
                )

                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)

                return
    
            if (
                err.startswith("PROVIDER3_NO_RECORD:")
                or err.startswith("PROVIDER4_NO_RECORD:")
            ):
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
