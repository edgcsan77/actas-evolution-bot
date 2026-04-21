import base64
import time
import threading
import re
import random
from datetime import datetime, timezone

from app.db import SessionLocal
from sqlalchemy.orm import Session

from app.models import RequestLog, ProviderSetting, AppSetting, GroupPromotion
from app.services.evolution import send_group_text, send_document_base64, send_group_document_base64
from app.config import settings
from app.utils.curp import provider_label_for_type, is_chain
from app.services.provider3 import Provider3Client, decode_pdf_base64
from app.services.provider4 import Provider4Client
from app.services.provider7 import Provider7Client
from app.queue import redis_conn
from app.provider_status_cache import refresh_providers_status

from zoneinfo import ZoneInfo

from io import BytesIO
from pypdf import PdfReader


PROVIDER4_TEST_GROUPS = set()
PROVIDER7_TEST_GROUPS = set()


def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mx_now():
    return datetime.now(ZoneInfo("America/Monterrey"))


CURP_RE = re.compile(
    r"^[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d$",
    re.IGNORECASE
)


def providers_status_loop():
    while True:
        try:
            refresh_providers_status()
        except Exception as e:
            print("PROVIDERS_STATUS_LOOP_ERROR =", str(e), flush=True)
        time.sleep(600)

threading.Thread(target=providers_status_loop, daemon=True).start()


def _notify_support_error(req, err: str, extra_msg: str = ""):
    support_group = (getattr(settings, "SOPORTE_ACTAS_GROUP", "") or "").strip()
    if not support_group:
        return

    try:
        msg = (
            "🚨 *ERROR SOPORTE ACTAS*\n\n"
            f"Solicitud ID: {getattr(req, 'id', 'N/D')}\n"
            f"Dato: {getattr(req, 'curp', 'N/D')}\n"
            f"Tipo: {getattr(req, 'act_type', 'N/D')}\n"
            f"Proveedor: {getattr(req, 'provider_name', 'N/D')}\n"
            f"Grupo origen: {getattr(req, 'source_group_id', 'N/D')}\n"
            f"Solicitante: {getattr(req, 'requester_wa_id', 'N/D')}\n"
            f"Error: {err}\n"
        )

        if extra_msg:
            msg += f"\nDetalle: {extra_msg}\n"

        send_group_text(support_group, msg)
    except Exception as support_exc:
        print("SUPPORT_ERROR_NOTIFY_FAILED =", str(support_exc), flush=True)


def _is_curp_term(value: str | None) -> bool:
    v = (value or "").strip().upper()
    return bool(CURP_RE.match(v))


def _is_provider4_eligible(term: str | None, act_type: str | None) -> bool:
    if not _is_curp_term(term):
        return False
    return True


def _group_individual_limit_reached(row: GroupPromotion | None) -> bool:
    if not row:
        return False

    limit_actas = int(row.shared_group_limit_actas or 0)
    used_actas = int(row.shared_group_used_actas or 0)

    if limit_actas <= 0:
        return False

    return used_actas >= limit_actas


def _group_individual_remaining(row: GroupPromotion | None) -> int | None:
    if not row:
        return None

    limit_actas = int(row.shared_group_limit_actas or 0)
    used_actas = int(row.shared_group_used_actas or 0)

    if limit_actas <= 0:
        return None

    return max(0, limit_actas - used_actas)


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
    p5 = _get_or_create_provider(db, "PROVIDER5", False)
    p6 = _get_or_create_provider(db, "PROVIDER6", False)
    p7 = _get_or_create_provider(db, "PROVIDER7", False)

    enabled = []
    if p1.is_enabled:
        enabled.append("PROVIDER1")
    if p2.is_enabled:
        enabled.append("PROVIDER2")
    if p3.is_enabled:
        enabled.append("PROVIDER3")
    if p4.is_enabled:
        enabled.append("PROVIDER4")
    if p5.is_enabled:
        enabled.append("PROVIDER5")
    if p6.is_enabled:
        enabled.append("PROVIDER6")
    if p7.is_enabled:
        enabled.append("PROVIDER7")

    return enabled


def _pick_provider_name(
    db,
    request_id: int,
    source_group_id: str | None = None,
    term: str | None = None,
    act_type: str | None = None,
) -> str:
    enabled = _enabled_providers(db)

    print("PICK_PROVIDER_ENABLED =", enabled, flush=True)
    print("PICK_PROVIDER_SOURCE_GROUP_ID =", repr(source_group_id), flush=True)
    print("PICK_PROVIDER_TERM =", repr(term), flush=True)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    # FORZAR PROVIDER7 PARA GRUPO DE PRUEBA
    if (
        source_group_id
        and source_group_id in PROVIDER7_TEST_GROUPS
        and "PROVIDER7" in enabled
    ):
        print("FORZANDO PROVIDER7 =", source_group_id, flush=True)
        return "PROVIDER7"

    if not _is_provider4_eligible(term, act_type):
        enabled = [p for p in enabled if p != "PROVIDER4"]

        if not enabled:
            raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")

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

    # Quitar PROVIDER7 de la rotación para grupos normales
    if PROVIDER7_TEST_GROUPS and "PROVIDER7" in enabled:
        enabled = [p for p in enabled if p != "PROVIDER7"]

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

    especiales_group = (settings.PROVIDER_GROUP_ESPECIALES or "").strip()
    foliadas_group = (settings.PROVIDER_GROUP_FOLIADAS or "").strip()

    if "FOLIO" in act_type or "FOLIAD" in act_type or " FOL " in f" {act_type} ":
        if not foliadas_group:
            raise RuntimeError("NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED")
        return foliadas_group

    if act_type.startswith("NACIMIENTO") or act_type.startswith("NAC"):
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

    if provider_name == "PROVIDER5":
        provider5_groups = [
            settings.PROVIDER5_GROUP_1,
            settings.PROVIDER5_GROUP_2,
        ]
        provider5_groups = [g for g in provider5_groups if g]

        if not provider5_groups:
            raise RuntimeError("PROVIDER5_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider5_groups)
        return provider5_groups[idx]

    if provider_name == "PROVIDER6":
        provider6_groups = [
            settings.PROVIDER6_GROUP_1,
            settings.PROVIDER6_GROUP_2,
        ]
        provider6_groups = [g for g in provider6_groups if g]

        if not provider6_groups:
            raise RuntimeError("PROVIDER6_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider6_groups)
        return provider6_groups[idx]

    if provider_name == "PROVIDER7":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _build_provider_message(provider_name: str, term: str, act_type: str) -> str | None:
    if provider_name in ("PROVIDER1", "PROVIDER2", "PROVIDER5", "PROVIDER6"):
        if is_chain(term):
            return f"{term}"
        provider_type = provider_label_for_type(act_type)
        return f"{term} {provider_type}"

    if provider_name == "PROVIDER3":
        return None

    if provider_name == "PROVIDER4":
        return None

    if provider_name == "PROVIDER7":
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
    

def _process_provider7(req, db):
    access_token = _get_app_setting(db, "PROVIDER7_ACCESS_TOKEN", settings.PROVIDER7_ACCESS_TOKEN)
    jsessionid = _get_app_setting(db, "PROVIDER7_JSESSIONID", settings.PROVIDER7_JSESSIONID)
    oficialia = _get_app_setting(db, "PROVIDER7_OFICIALIA", str(settings.PROVIDER7_OFICIALIA))
    rfc_usuario = _get_app_setting(db, "PROVIDER7_RFC_USUARIO", settings.PROVIDER7_RFC_USUARIO)

    client = Provider7Client(
        access_token=access_token,
        jsessionid=jsessionid,
        oficialia=oficialia,
        rfc_usuario=rfc_usuario,
    )

    result = client.generar_pdf_bytes(
        term=req.curp,
        act_type=req.act_type,
        agregar_marco_frontal=True,
        agregar_reverso_estado=True,
    )

    return {
        "pdf_bytes": result["pdf_bytes"],
        "estado": result["estado"],
        "sexo": result["sexo"],
        "cadena": result["cadena"],
    }


def _handle_group_promotion_after_done(req, db):
    if not req.source_group_id:
        return

    source_group_id = (req.source_group_id or "").strip()

    # 1) Bloquear primero la fila del grupo actual
    current = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid == source_group_id,
            GroupPromotion.is_active == True
        )
        .with_for_update()
        .first()
    )

    if not current:
        return

    shared_key = (current.shared_key or "").strip()

    # 2) Si es bolsa compartida, bloquear TODAS las filas de esa bolsa
    #    en orden fijo para evitar lost updates y lecturas inconsistentes
    if shared_key:
        rows = (
            db.query(GroupPromotion)
            .filter(
                GroupPromotion.shared_key == shared_key,
                GroupPromotion.is_active == True
            )
            .order_by(GroupPromotion.id.asc())
            .with_for_update()
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

    current_group_row = None

    # 3) Incremento atómico dentro del lock
    for row in rows:
        row.total_actas = total_before
        row.used_actas = used_after

        if (row.group_jid or "").strip() == source_group_id:
            current_group_row = row

            # Solo en bolsa compartida tiene sentido este contador individual
            if shared_key:
                row.shared_group_used_actas = int(row.shared_group_used_actas or 0) + 1

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
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa compartida.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "10"
        for row in rows:
            row.warning_sent_10 = True

    elif crossed_50:
        msg = (
            f"⚠️ *Aviso importante de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa compartida.\n\n' if shared_key else ''}"
            f"Quedamos atentos."
        )
        notify_level = "50"
        for row in rows:
            row.warning_sent_50 = True

    elif crossed_100:
        msg = (
            f"⚠️ *Aviso de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{'Este aviso aplica para todos los grupos asociados a esta bolsa compartida.\n\n' if shared_key else ''}"
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

    individual_limit_msg = None
    reached_individual_limit = False

    # 4) Revisar límite individual solo en bolsa compartida
    if shared_key and current_group_row:
        limit_actas = int(current_group_row.shared_group_limit_actas or 0)
        used_group = int(current_group_row.shared_group_used_actas or 0)

        if limit_actas > 0 and used_group >= limit_actas:
            reached_individual_limit = True
            individual_limit_msg = (
                f"⚠️ *Límite individual alcanzado*\n\n"
                f"Este grupo alcanzó su límite individual dentro de la bolsa compartida.\n"
                f"Límite del grupo: *{limit_actas} actas*.\n"
                f"Consumidas por este grupo: *{used_group}*.\n\n"
                f"El grupo quedará bloqueado automáticamente, "
                f"pero la bolsa compartida general puede seguir disponible para los demás grupos."
            )

    db.commit()

    # 5) Acciones posteriores al commit
    if reached_individual_limit and current_group_row and current_group_row.group_jid:
        try:
            from app.main import block_group
            block_group(current_group_row.group_jid)
        except Exception as e:
            print("SHARED_GROUP_LIMIT_BLOCK_AFTER_DONE_ERROR =", str(e), flush=True)

        try:
            send_group_text(current_group_row.group_jid, individual_limit_msg)
        except Exception as e:
            print("SHARED_GROUP_LIMIT_NOTIFY_AFTER_DONE_ERROR =", str(e), flush=True)

    if crossed_0:
        try:
            _block_client_groups(rows if shared_key else [current])
        except Exception as e:
            print("PROMOTION_BLOCK_ERROR =", str(e), flush=True)

    if msg and notify_level:
        notify_scope = shared_key if shared_key else current.group_jid

        if notify_level == "0":
            try:
                if shared_key:
                    _notify_client_groups(rows, msg)
                else:
                    send_group_text(current.group_jid, msg)
            except Exception as e:
                print("PROMOTION_NOTIFY_LEVEL_0_ERROR =", str(e), flush=True)
            return

        notify_key = f"promo_notify:{notify_scope}:{notify_level}"
        first_notify = redis_conn.set(notify_key, "1", ex=1800, nx=True)

        if first_notify:
            if shared_key:
                try:
                    _notify_client_groups(rows, msg)
                except Exception as e:
                    print("PROMOTION_SHARED_GROUP_NOTIFY_ERROR =", str(e), flush=True)
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

    send_group_text(provider_group_id, text_to_provider, settings.EVOLUTION_PROVIDER_INSTANCE)


def _extract_pdf_visible_text(pdf_bytes: bytes) -> str:
    parts = []

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        for page in reader.pages:
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            if txt:
                parts.append(txt)
    except Exception as e:
        print("PDF_TEXT_EXTRACT_ERROR =", str(e), flush=True)

    text = "\n".join(parts).upper().strip()

    # Fallback muy secundario: bytes crudos solo si no hubo texto legible
    if not text:
        try:
            text = pdf_bytes.decode("latin1", errors="ignore").upper()
        except Exception:
            text = ""

    return text


def _normalize_alnum(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def _find_curps_in_text(text: str) -> list[str]:
    if not text:
        return []

    pattern = r"[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
    found = re.findall(pattern, text, flags=re.IGNORECASE)

    # únicos, preservando orden
    unique = []
    seen = set()
    for item in found:
        curp = item.upper()
        if curp not in seen:
            seen.add(curp)
            unique.append(curp)

    return unique


def _validate_pdf_matches_term(pdf_bytes: bytes, term: str, act_type: str | None = None) -> bool:
    expected = _normalize_alnum(term)
    if not expected:
        return True

    text = _extract_pdf_visible_text(pdf_bytes)
    if not text or len(text.strip()) < 30:
        print("PROVIDER_VALIDATE_TEXT_TOO_SHORT", flush=True)
        return False

    found_curps = _find_curps_in_text(text)
    act_type_up = (act_type or "").upper().strip()

    print("PROVIDER_VALIDATE_EXPECTED_CURP =", expected, flush=True)
    print("PROVIDER_VALIDATE_FOUND_CURPS =", found_curps, flush=True)
    print("PROVIDER_VALIDATE_ACT_TYPE =", act_type_up, flush=True)

    if found_curps:
        if expected not in found_curps:
            return False

        # En matrimonio es normal que aparezcan varias CURP
        if "MATRIMONIO" in act_type_up:
            return True

        # En otros tipos, si hay varias CURP distintas, se rechaza
        if len(found_curps) > 1:
            print("PROVIDER_VALIDATE_MULTIPLE_CURPS_REJECTED = TRUE", flush=True)
            return False

        return True

    normalized_text = _normalize_alnum(text)
    if expected in normalized_text:
        return True

    return False


def _validate_act_type_pdf(pdf_bytes: bytes, act_type: str | None) -> bool:
    text = _extract_pdf_visible_text(pdf_bytes)
    if not text or len(text.strip()) < 30:
        print("PROVIDER_VALIDATE_ACT_TEXT_TOO_SHORT", flush=True)
        return False

    text = text.upper()
    act_type = (act_type or "").upper()

    if "NAC" in act_type:
        if "ACTA DE NACIMIENTO" in text:
            return True
        if "MATRIMONIO" in text or "DIVORCIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return False

    if "MAT" in act_type:
        if "ACTA DE MATRIMONIO" in text:
            return True
        if "NACIMIENTO" in text or "DIVORCIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return False

    if "DIV" in act_type:
        if "ACTA DE DIVORCIO" in text:
            return True
        if "NACIMIENTO" in text or "MATRIMONIO" in text or "DEFUNCION" in text or "DEFUNCIÓN" in text:
            return False
        return False

    if "DEF" in act_type:
        if "ACTA DE DEFUNCION" in text or "ACTA DE DEFUNCIÓN" in text:
            return True
        if "NACIMIENTO" in text or "MATRIMONIO" in text or "DIVORCIO" in text:
            return False
        return False

    return False


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

        if req.source_group_id:
            promo_row = (
                db.query(GroupPromotion)
                .filter(
                    GroupPromotion.group_jid == req.source_group_id,
                    GroupPromotion.is_active == True
                )
                .first()
            )

            if promo_row and (promo_row.shared_key or "").strip():
                if _group_individual_limit_reached(promo_row):
                    remaining_shared = max(
                        0,
                        int(promo_row.total_actas or 0) - int(promo_row.used_actas or 0)
                    )

                    msg = (
                        f"⚠️ *Límite individual alcanzado*\n\n"
                        f"Este grupo ya consumió su límite individual dentro de la bolsa compartida.\n"
                        f"Límite del grupo: *{int(promo_row.shared_group_limit_actas or 0)} actas*.\n"
                        f"Consumidas por este grupo: *{int(promo_row.shared_group_used_actas or 0)}*.\n"
                        f"Saldo disponible en la bolsa general: *{remaining_shared} actas*."
                    )

                    try:
                        send_group_text(req.source_group_id, msg, req.instance_name)
                    except Exception as notify_exc:
                        print("SHARED_GROUP_LIMIT_NOTIFY_ERROR =", str(notify_exc), flush=True)

                    try:
                        from app.main import block_group
                        block_group(req.source_group_id)
                    except Exception as block_exc:
                        print("SHARED_GROUP_LIMIT_BLOCK_ERROR =", str(block_exc), flush=True)

                    req.status = "ERROR"
                    req.error_message = "SHARED_GROUP_LIMIT_REACHED"
                    req.updated_at = _utc_now_naive()
                    db.commit()
                    return

        provider_name = _pick_provider_name(
            db,
            req.id,
            req.source_group_id,
            req.curp,
            req.act_type,
        )
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

        if provider_name in ("PROVIDER1", "PROVIDER2", "PROVIDER5", "PROVIDER6"):
            print("PROVIDER_SEND_TO_PROVIDER =", req.id, time.time(), flush=True)
        
            send_ok = False
            last_err = None
        
            for attempt in range(3):
                try:
                    resp_json = send_group_text(provider_group_id, text_to_provider, settings.EVOLUTION_PROVIDER_INSTANCE)
                    send_ok = True
            
                    provider_sent_msg_id = (
                        (resp_json or {}).get("key", {}).get("id")
                        or (resp_json or {}).get("data", {}).get("key", {}).get("id")
                        or (resp_json or {}).get("id")
                        or ""
                    )
            
                    if provider_sent_msg_id:
                        req.provider_message_id = provider_sent_msg_id
                        req.updated_at = _utc_now_naive()
                        db.commit()
            
                    print(f"PROVIDER_SEND_OK_ATTEMPT_{attempt+1} =", req.id, flush=True)
                    print("PROVIDER_SENT_MSG_ID =", provider_sent_msg_id, flush=True)
                    break
                except Exception as e:
                    last_err = str(e)
                    print(f"PROVIDER_SEND_ATTEMPT_{attempt+1}_ERROR =", last_err, flush=True)
                    if attempt < 2:
                        time.sleep(1.5)
        
            if send_ok:
                return
        
            req.status = "ERROR"
            req.error_message = f"{provider_name}_SEND_FAILED"
            req.updated_at = _utc_now_naive()
            db.commit()
        
            msg = (
                f"⚠️ Solicitud sin éxito en Registro Civil\n"
                f"Dato: {req.curp}\n"
                f"Tipo: {req.act_type}\n\n"
                f"Reenviar nuevamente en unos minutos"
            )
        
            try:
                instance = req.instance_name or "docifybot3"

                if req.source_group_id:
                    send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)
                    
            except Exception as notify_exc:
                print("CLIENT_NOTIFY_AFTER_PROVIDER_SEND_FAIL_ERROR =", str(notify_exc), flush=True)
        
            _notify_support_error(req, f"{provider_name}_SEND_FAILED", last_err or "")
            return

        if provider_name == "PROVIDER3":
            try:
                provider3_result = _process_provider3(req, db)
            except Exception as e:
                err = str(e)

                if err.startswith("PROVIDER3_NO_RECORD"):
                    err_up = err.upper()
                
                    transient_p3 = (
                        "503" in err_up
                        or "SATURADO" in err_up
                        or "FILA LLENA" in err_up
                        or "INTENTE MAS TARDE" in err_up
                        or "INTENTE MÁS TARDE" in err_up
                        or "TIMEOUT" in err_up
                    )
                
                    if not transient_p3:
                        raise
                
                print("PROVIDER3_GENERATION_FAILED =", err, flush=True)
                print("PROVIDER3_FALLBACK_TO_PROVIDER1 =", req.id, req.curp, flush=True)

                enabled = _enabled_providers(db)

                if "PROVIDER1" not in enabled:
                    raise RuntimeError(f"NO_PROVIDER_ENABLED | ORIGIN={err}")
        
                provider_name = "PROVIDER1"
                provider_group_id = _pick_provider_group(provider_name, req.act_type, req.id)
                text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)
        
                req.provider_name = provider_name
                req.provider_group_id = provider_group_id
                req.provider_message = text_to_provider
                req.status = "PROCESSING"
                req.error_message = None
                req.updated_at = _utc_now_naive()
                db.commit()
        
                send_group_text(provider_group_id, text_to_provider, settings.EVOLUTION_PROVIDER_INSTANCE)
        
                print(
                    "PROVIDER3_FALLBACK_SENT_TO_PROVIDER1 =",
                    {
                        "req_id": req.id,
                        "provider_group_id": provider_group_id,
                        "text": text_to_provider,
                    },
                    flush=True,
                )
                return
        
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
                
                if not _validate_pdf_matches_term(pdf_bytes, req.curp, req.act_type):
                    print("PROVIDER4_VALIDATE_FAIL_REQ_CURP =", req.curp, flush=True)
                    raise RuntimeError(f"PROVIDER4_WRONG_CURP_IN_PDF:{req.curp}")
        
            except Exception as p4_exc:
                p4_err = str(p4_exc)
                p4_elapsed = time.perf_counter() - provider4_started_ts
                enabled = _enabled_providers(db)
            
                if (
                    p4_err.startswith("PROVIDER4_WRONG_CURP_IN_PDF")
                    or p4_err.startswith("PROVIDER4_WRONG_ACT_TYPE")
                ):
                    msg = (
                        f"⚠️ Solicitud sin éxito en Registro Civil\n"
                        f"Dato: {req.curp}\n"
                        f"Tipo: {req.act_type}\n\n"
                        f"Reenviar nuevamente en unos minutos"
                    )
            
                    if req.source_group_id:
                        send_group_text(req.source_group_id, msg, req.instance_name)
                    else:
                        from app.services.evolution import send_text
                        send_text(req.requester_wa_id, msg, req.instance_name)
            
                    req.status = "ERROR"
                    req.error_message = p4_err
                    req.updated_at = _utc_now_naive()
                    db.commit()
            
                    _notify_support_error(req, p4_err, "PDF cruzado o tipo incorrecto devuelto por Provider4")
                    return
            
                fallback_errors = (
                    p4_err.startswith("PROVIDER4_BACKEND_FAILED:")
                    or p4_err.startswith("PROVIDER4_VGET_FAILED:")
                    or p4_err.startswith("PROVIDER4_HISTORY_FAILED:")
                    or p4_err.startswith("PROVIDER4_HISTORY_NOT_CONFIRMED_PDF:")
                    or p4_err.startswith("PROVIDER4_HISTORY_NOT_CONFIRMED_FOLIO:")
                    or p4_err.startswith("PROVIDER4_NO_PDF_LINK_FOR:")
                    or p4_err.startswith("PROVIDER4_NO_FOLIO_LINK_FOR:")
                    or p4_err.startswith("PROVIDER4_DOWNLOAD_FAILED:")
                    or p4_err.startswith("PROVIDER4_FOLIO_DOWNLOAD_FAILED:")
                    or p4_err.startswith("PROVIDER4_WRONG_ACT_TYPE")
                    or p4_err.startswith("PROVIDER4_WRONG_CURP_IN_PDF")
                    or "Read timed out" in p4_err
                )
            
                should_fallback = (
                    p4_err.startswith("PROVIDER4_NO_FORM_ACTION")
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
                            send_group_text(req.source_group_id, msg, req.instance_name)
                        else:
                            from app.services.evolution import send_text
                            send_text(req.requester_wa_id, msg, req.instance_name)
            
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

        if provider_name == "PROVIDER7":
            try:
                provider7_result = _process_provider7(req, db)
        
            except Exception as e:
                err = str(e)
        
                if err.startswith("PROVIDER7_CURP_NO_RESULTS"):
                    req.status = "ERROR"
                    req.error_message = err[:1000]
                    req.updated_at = _utc_now_naive()
                    db.commit()
        
                    msg = (
                        f"❌ No hay registros disponibles.\n"
                        f"Dato: {req.curp}\n"
                        f"Tipo: {req.act_type}\n\n"
                        f"Verificar que la CURP esté certificada en RENAPO"
                    )
        
                    try:
                        instance = req.instance_name or "docifybot3"
        
                        if req.source_group_id:
                            send_group_text(req.source_group_id, msg, instance)
                        else:
                            from app.services.evolution import send_text
                            send_text(req.requester_wa_id, msg, instance)
        
                    except Exception as notify_exc:
                        print("CLIENT_NOTIFY_AFTER_PROVIDER7_NO_RESULTS_ERROR =", str(notify_exc), flush=True)
        
                    return
        
                req.status = "ERROR"
                req.error_message = err[:1000]
                req.updated_at = _utc_now_naive()
                db.commit()
        
                try:
                    instance = req.instance_name or "docifybot3"
                    msg = (
                        f"⚠️ Solicitud sin éxito en Registro Civil\n"
                        f"Dato: {req.curp}\n"
                        f"Tipo: {req.act_type}\n\n"
                        f"Reenviar nuevamente en unos minutos"
                    )
        
                    if req.source_group_id:
                        send_group_text(req.source_group_id, msg, instance)
                    else:
                        from app.services.evolution import send_text
                        send_text(req.requester_wa_id, msg, instance)
        
                except Exception as notify_exc:
                    print("CLIENT_NOTIFY_AFTER_PROVIDER7_FAIL_ERROR =", str(notify_exc), flush=True)
        
                _notify_support_error(req, "PROVIDER7_ERROR", err)
                return
        
            pdf_bytes = provider7_result["pdf_bytes"]
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
        
            req.provider_media_url = "BASE64_PROVIDER7"
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

            if err == "NO_PROVIDER_FOR_SPECIAL_FORMAT":
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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                _notify_support_error(req, err, msg)
                return

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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                _notify_support_error(req, err, msg)
                return

            if err.startswith("NO_PROVIDER_ENABLED"):
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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                _notify_support_error(req, err, msg)
                return
    
            if (
                err.startswith("PROVIDER3_NO_RECORD")
                or err.startswith("PROVIDER4_NO_RECORD")
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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                #_notify_support_error(req, err, msg)
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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                _notify_support_error(req, err, msg)
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
                    send_group_text(req.source_group_id, msg, req.instance_name)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, req.instance_name)

                _notify_support_error(req, err, msg)
                return

            req.status = "ERROR"
            req.error_message = err
            db.commit()
            _notify_support_error(req, err, "ERROR NO CONTROLADO EN WORKER")
        raise
        
    finally:
        db.close()
