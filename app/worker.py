import base64
import time
import threading
import re
import random
from datetime import datetime, timezone
from decimal import Decimal

from app.db import SessionLocal
from sqlalchemy.orm import Session

from app.models import RequestLog, ProviderSetting, AppSetting, GroupPromotion, ApiClient, ApiCreditLog
from app.services.evolution import send_group_text, send_document_base64, send_group_document_base64
from app.config import settings
from app.utils.curp import provider_label_for_type, is_chain
from app.services.provider3 import Provider3Client, decode_pdf_base64
from app.services.provider4 import Provider4Client
from app.services.provider7 import Provider7Client
from rq import get_current_job
from app.queue import redis_conn, slow_request_queue
from app.provider_status_cache import refresh_providers_status
from app.utils.bot_limits import increment_bot_used_and_maybe_block

from zoneinfo import ZoneInfo

from io import BytesIO
from pypdf import PdfReader

PROVIDER4_TEST_GROUPS = set()
PROVIDER7_TEST_GROUPS = set()

SLOW_PROVIDER_QUEUE_NAME = "actas_slow"
SLOW_PROVIDERS = {"PROVIDER4", "PROVIDER10", "PROVIDER11"}


def _current_queue_name() -> str:
    try:
        job = get_current_job(connection=redis_conn)
        return (getattr(job, "origin", "") or "").strip()
    except Exception as e:
        print("CURRENT_QUEUE_NAME_ERROR =", str(e), flush=True)
        return ""


def _should_reroute_to_slow(provider_name: str | None) -> bool:
    return (provider_name or "").strip().upper() in SLOW_PROVIDERS
    

BOT_PROVIDER_MODE_KEY_PREFIX = "BOT_PROVIDER_MODE:"
DEFAULT_BOT_PROVIDER_MODE = {
    "docifybot8maya": "GLOBAL_POOL",
}


def _norm_instance(instance_name: str | None) -> str:
    return (instance_name or "").strip().lower()


def _bot_provider_mode(db, instance_name: str | None) -> str:
    inst = _norm_instance(instance_name)
    if not inst:
        return "GLOBAL_POOL"

    default = DEFAULT_BOT_PROVIDER_MODE.get(inst, "GLOBAL_POOL")
    mode = _get_app_setting(db, f"{BOT_PROVIDER_MODE_KEY_PREFIX}{inst}", default)
    return (mode or default or "GLOBAL_POOL").strip().upper()


def _is_personal_provider_mode(mode: str | None) -> bool:
    return (mode or "").strip().upper().startswith("PERSONAL:")


def _provider_from_mode(mode: str | None) -> str | None:
    mode = (mode or "").strip().upper()

    if ":" not in mode:
        return None

    prefix, provider_name = mode.split(":", 1)
    provider_name = provider_name.strip().upper()

    if provider_name in {
        "PROVIDER1",
        "PROVIDER2",
        "PROVIDER3",
        "PROVIDER4",
        "PROVIDER5",
        "PROVIDER6",
        "PROVIDER7",
        "PROVIDER8",
        "PROVIDER9",
        "PROVIDER10",
        "PROVIDER11",
        "MAYAPROVIDER",
    }:
        return provider_name

    return None


def _request_is_no_accounting(req, db) -> bool:
    mode = _bot_provider_mode(db, getattr(req, "instance_name", None))
    mode_provider = _provider_from_mode(mode)

    return (
        _is_personal_provider_mode(mode)
        and mode_provider
        and (getattr(req, "provider_name", "") or "").strip().upper() == mode_provider
    )


def _current_mode_is_personal(db, instance_name: str | None) -> bool:
    mode = _bot_provider_mode(db, instance_name)
    return _is_personal_provider_mode(mode)


NO_FAIL_NOTIFY_GROUPS = {
    "120363427267191472@g.us"
}

NO_EXTRA_TEXT_GROUPS = {
    "120363427267191472@g.us"
}

def should_notify_failure(group_id: str | None) -> bool:
    if not group_id:
        return True
    return group_id not in NO_FAIL_NOTIFY_GROUPS


def should_send_extra_text(group_id: str | None) -> bool:
    if not group_id:
        return True
    return group_id not in NO_EXTRA_TEXT_GROUPS
    

def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mx_now():
    return datetime.now(ZoneInfo("America/Monterrey"))


def _fmt_seconds(seconds: float) -> str:
    seconds = max(0.0, float(seconds or 0))

    if seconds >= 60:
        minutes = int(seconds // 60)
        rest = seconds % 60
        return f"{minutes} min {rest:.2f} segundos"

    return f"{seconds:.2f} segundos"


def _request_total_seconds(req, fallback_started_ts: float | None = None) -> float:
    now_utc = _utc_now_naive()

    created_at = getattr(req, "created_at", None)

    if created_at:
        return max(0.0, (now_utc - created_at).total_seconds())

    if fallback_started_ts is not None:
        return max(0.0, time.perf_counter() - fallback_started_ts)

    return 0.0


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


PROVIDER_LABELS_SUPPORT = {
    "PROVIDER1": "ADMIN DIGITAL",
    "PROVIDER2": "ACTAS DEL SURESTE",
    "PROVIDER3": "AUSTRAM WEB",
    "PROVIDER4": "LAZARO WEB 1",
    "PROVIDER5": "LUIS SID",
    "PROVIDER6": "ACTAS ESCALANTE",
    "PROVIDER7": "MESINO SID",
    "PROVIDER8": "ANGEL",
    "PROVIDER9": "EMILIANO",
    "PROVIDER10": "LAZARO WEB 2",
    "PROVIDER11": "LAZARO WEB 3",
    "MAYAPROVIDER": "PROVEEDOR DE MAYA",
}


SUPPORT_ERROR_LABELS_ES = {
    # Errores generales de selección/configuración
    "NO_PROVIDER_ENABLED": "No hay proveedores activos disponibles para procesar la solicitud.",
    "NO_PROVIDER_FOR_SPECIAL_FORMAT": "No hay proveedor disponible para este tipo/formato de solicitud.",
    "UNKNOWN_PROVIDER": "Proveedor desconocido o no configurado.",
    "NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED": "No hay grupo configurado para actas foliadas.",
    "NO_BIRTH_PROVIDER_GROUP_CONFIGURED": "No hay grupo configurado para nacimientos.",
    "NO_SPECIAL_PROVIDER_GROUP_CONFIGURED": "No hay grupo configurado para actas especiales.",
    "NO_PROVIDER6_FOLIADAS_GROUP_CONFIGURED": "No hay grupo de foliadas configurado para ACTAS ESCALANTE.",
    "NO_PROVIDER6_ESPECIALES_GROUP_CONFIGURED": "No hay grupo de especiales configurado para ACTAS ESCALANTE.",
    "NO_PROVIDER6_NACIMIENTO_GROUP_CONFIGURED": "No hay grupo de nacimiento configurado para ACTAS ESCALANTE.",
    "PROVIDER2_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para ACTAS DEL SURESTE.",
    "PROVIDER5_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para LUIS SID.",
    "PROVIDER8_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para ANGEL.",
    "PROVIDER9_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para EMILIANO.",
    "MAYAPROVIDER_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para el proveedor de MAYA.",
    "PROVIDER6_ACT_TYPE_NOT_ALLOWED": "ACTAS ESCALANTE no acepta este tipo de acta. Solo debe recibir CADENA, NACIMIENTO y FOLIADA.",

    # Errores de PDF/validación
    "WRONG_ACT_TYPE_PDF_PENDING_RETRY": "El proveedor envió un PDF de otro tipo de acta. La solicitud sigue en proceso para esperar el PDF correcto.",
    "WRONG_CURP_IN_PDF": "El PDF recibido no corresponde al dato solicitado.",
    "PROVIDER8_POSTPROCESS_ERROR": "Error al procesar el PDF recibido del proveedor ANGEL.",
    "SHARED_GROUP_LIMIT_REACHED": "El grupo alcanzó su límite individual de actas.",

    # Provider 3
    "PROVIDER3_NO_PDF": "AUSTRAM WEB no devolvió un PDF válido.",
    "PROVIDER3_PDF_SEND_FAILED": "No se pudo enviar el PDF generado por AUSTRAM WEB.",

    # Provider 7 / otros
    "PROVIDER7_ERROR": "Error al procesar la solicitud con MESINO SID.",
    "DELIVERY_FAILED": "No se pudo entregar el PDF al cliente por WhatsApp.",
}


def _support_provider_label(provider_name: str | None) -> str:
    p = (provider_name or "").strip().upper()
    return PROVIDER_LABELS_SUPPORT.get(p, p or "N/D")


def _support_provider_from_error(err: str | None) -> str:
    text = (err or "").strip().upper()

    # Detecta MAYAPROVIDER aunque no venga guardado todavía en req.provider_name.
    if "MAYAPROVIDER" in text:
        return "MAYAPROVIDER"

    m = re.search(r"\bPROVIDER(?:_)?(10|11|[1-9])\b", text)
    if m:
        return f"PROVIDER{m.group(1)}"

    return ""


def _should_skip_support_error(req, err: str | None) -> bool:
    provider_name = (getattr(req, "provider_name", "") or "").strip().upper()
    instance_name = (getattr(req, "instance_name", "") or "").strip().lower()
    err_text = (err or "").strip().upper()

    # No enviar NADA de MAYAPROVIDER al grupo de soporte.
    # Cubre:
    # - req.provider_name = MAYAPROVIDER
    # - err = MAYAPROVIDER_GROUPS_NOT_CONFIGURED
    # - err = MAYAPROVIDER_SEND_FAILED
    # - instancia docifybot8maya usando proveedor personal
    if provider_name == "MAYAPROVIDER":
        return True

    if "MAYAPROVIDER" in err_text:
        return True

    if instance_name == "docifybot8maya":
        return True

    return False


def _split_error_code_and_detail(err: str | None) -> tuple[str, str]:
    raw = (err or "").strip()
    if not raw:
        return "", ""

    if ":" in raw:
        code, detail = raw.split(":", 1)
        return code.strip(), detail.strip()

    if " | " in raw:
        code, detail = raw.split(" | ", 1)
        return code.strip(), detail.strip()

    return raw.strip(), ""


def _clean_error_code(code: str | None) -> str:
    code_up = (code or "").strip().upper()

    # Algunos errores llegan como PROVIDER1_WRONG_CURP_IN_PDF.
    # Para traducirlos mejor quitamos el prefijo del proveedor.
    code_up = re.sub(r"^PROVIDER(?:10|11|[1-9])_", "", code_up)

    # Y también MAYAPROVIDER_...
    code_up = re.sub(r"^MAYAPROVIDER_", "", code_up)

    return code_up


def _humanize_support_code(err: str | None) -> str:
    raw = (err or "").strip()
    if not raw:
        return "Error no especificado."

    original_code, detail = _split_error_code_and_detail(raw)
    code_up = (original_code or "").strip().upper()
    code_clean = _clean_error_code(code_up)

    provider_from_error = _support_provider_from_error(code_up)
    provider_label = _support_provider_label(provider_from_error) if provider_from_error else ""

    # 1) Traducción directa del error completo.
    if code_up in SUPPORT_ERROR_LABELS_ES:
        msg = SUPPORT_ERROR_LABELS_ES[code_up]
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    # 2) Traducción quitando prefijo PROVIDER1_, PROVIDER4_, etc.
    if code_clean in SUPPORT_ERROR_LABELS_ES:
        msg = SUPPORT_ERROR_LABELS_ES[code_clean]
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    # 3) Patrones generales por sufijo.
    if code_clean == "SEND_FAILED":
        msg = f"No se pudo enviar la solicitud al proveedor {provider_label or 'seleccionado'}."
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    if code_clean == "GROUPS_NOT_CONFIGURED":
        return f"No hay grupos configurados para el proveedor {provider_label or 'seleccionado'}."

    if code_clean == "WRONG_ACT_TYPE":
        return f"El proveedor {provider_label or 'seleccionado'} devolvió un PDF de otro tipo de acta."

    if code_clean == "WRONG_CURP_IN_PDF":
        return f"El proveedor {provider_label or 'seleccionado'} devolvió un PDF que no corresponde al dato solicitado."

    if code_clean == "WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF":
        return f"El proveedor {provider_label or 'seleccionado'} devolvió un PDF que no corresponde a la cadena/folio solicitado."

    if code_clean == "NOT_CURP_OR_CHAIN":
        return f"El proveedor {provider_label or 'seleccionado'} no pudo procesar el dato porque no parece CURP ni cadena válida."

    if code_clean == "NOT_ALLOWED_GROUP":
        return f"El proveedor {provider_label or 'seleccionado'} no está permitido para este grupo."

    if code_clean == "PDF_SEND_FAILED":
        return f"El PDF fue generado, pero no se pudo entregar al cliente desde el proveedor {provider_label or 'seleccionado'}."

    if code_clean == "NO_PDF":
        msg = f"El proveedor {provider_label or 'seleccionado'} no devolvió PDF."
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    if code_clean.startswith("FAILED_FALLBACK_TO_"):
        fallback_provider = code_clean.replace("FAILED_FALLBACK_TO_", "").strip()
        fallback_label = _support_provider_label(fallback_provider)
        return f"Falló el proveedor inicial y también falló el respaldo hacia {fallback_label}."

    if code_clean == "FALLBACK_NO_PROVIDER_AVAILABLE":
        return "Falló el proveedor inicial y no hubo otro proveedor disponible para respaldo."

    if code_clean in {
        "BACKEND_FAILED",
        "VGET_FAILED",
        "HISTORY_FAILED",
        "HISTORY_NOT_CONFIRMED_PDF",
        "HISTORY_NOT_CONFIRMED_FOLIO",
        "NO_PDF_LINK_FOR",
        "NO_FOLIO_LINK_FOR",
        "DOWNLOAD_FAILED",
        "FOLIO_DOWNLOAD_FAILED",
    }:
        readable = code_clean.replace("_", " ").lower()
        msg = f"Error del proveedor {provider_label or 'seleccionado'}: {readable}."
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    # 4) Patrones genéricos para errores futuros.
    if "TIMEOUT" in code_up or "TIMED OUT" in raw.upper() or "READ TIMED OUT" in raw.upper():
        return f"La solicitud excedió el tiempo máximo de espera. Código técnico: {code_up}"

    if "NOT_CONFIGURED" in code_up:
        readable = code_up.replace("_", " ").lower()
        return f"Falta configuración en el sistema: {readable}."

    if "WRONG" in code_up and "PDF" in code_up:
        return f"El PDF recibido no pasó una validación del sistema. Código técnico: {code_up}"

    if "FAILED" in code_up:
        readable = code_up.replace("_", " ").lower()
        msg = f"Ocurrió una falla durante el proceso: {readable}."
        if detail:
            msg += f"\nDetalle técnico: {detail}"
        return msg

    if "INVALID" in code_up:
        readable = code_up.replace("_", " ").lower()
        return f"El dato o respuesta fue detectado como inválido: {readable}."

    # 5) Último fallback: NUNCA deja el error vacío ni rompe el aviso.
    readable = code_up.replace("_", " ").lower()
    if detail:
        return f"Error del sistema: {readable}.\nDetalle técnico: {detail}"

    return f"Error del sistema: {readable}."


def _support_extra_es(extra_msg: str | None) -> str:
    text = (extra_msg or "").strip()
    if not text:
        return ""

    replacements = {
        "filename=": "archivo=",
        "expected_act_type=": "tipo_esperado=",
        "expected_curp=": "dato_esperado=",
        "provider=": "proveedor=",
        "provider_name=": "proveedor=",
        "group=": "grupo=",
        "group_id=": "grupo_id=",
        "source_group_id=": "grupo_origen=",
        "request_id=": "solicitud_id=",
        "status_code=": "codigo_http=",
        "response=": "respuesta=",
        "body=": "respuesta=",
        "error=": "detalle_error=",
        "timeout=": "tiempo_espera=",
        "filename:": "archivo:",
        "expected_act_type:": "tipo_esperado:",
        "expected_curp:": "dato_esperado:",
        "NO se notificó al cliente para evitar falso error": "No se notificó al cliente para evitar un falso error",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Cambia códigos PROVIDER dentro del detalle por nombre real.
    for code, label in PROVIDER_LABELS_SUPPORT.items():
        text = text.replace(code, label)

    return text


def _notify_support_error(req, err: str, extra_msg: str = ""):
    if _should_skip_support_error(req, err):
        print("SUPPORT_ERROR_SKIPPED =", {
            "req_id": getattr(req, "id", None),
            "provider_name": getattr(req, "provider_name", None),
            "instance_name": getattr(req, "instance_name", None),
            "err": err,
            "reason": "MAYAPROVIDER_OR_PRIVATE_PROVIDER",
        }, flush=True)
        return

    support_group = (getattr(settings, "SOPORTE_ACTAS_GROUP", "") or "").strip()
    if not support_group:
        return

    try:
        provider_name = (getattr(req, "provider_name", "") or "").strip().upper()

        # Si req.provider_name viene vacío, intenta detectarlo desde el error.
        if not provider_name:
            provider_name = _support_provider_from_error(err)

        msg = (
            "🚨 *ERROR SOPORTE ACTAS*\n\n"
            f"Solicitud ID: {getattr(req, 'id', 'N/D')}\n"
            f"Dato: {getattr(req, 'curp', 'N/D')}\n"
            f"Tipo: {getattr(req, 'act_type', 'N/D')}\n"
            f"Proveedor: {_support_provider_label(provider_name)}\n"
            f"Grupo origen: {getattr(req, 'source_group_id', 'N/D')}\n"
            f"Solicitante: {getattr(req, 'requester_wa_id', 'N/D')}\n"
            f"Error: {_humanize_support_code(err)}\n"
        )

        extra_msg_es = _support_extra_es(extra_msg)
        if extra_msg_es:
            msg += f"\nDetalle: {extra_msg_es}\n"

        support_instance = getattr(settings, "SOPORTE_ACTAS_INSTANCE", None) or "docifybot8"
        print("SOPORTE_ACTAS_SEND_INSTANCE =", support_instance, flush=True)
        send_group_text(support_group, msg, support_instance)

    except Exception as support_exc:
        print("SUPPORT_ERROR_NOTIFY_FAILED =", str(support_exc), flush=True)


def _is_curp_term(value: str | None) -> bool:
    v = (value or "").strip().upper()
    return bool(CURP_RE.match(v))


def _is_provider4_eligible(term: str | None, act_type: str | None) -> bool:
    term = (term or "").strip()
    act_type_up = (act_type or "").upper().strip()

    if _is_curp_term(term):
        return True

    if is_chain(term) or bool(re.fullmatch(r"\d{15,25}", term)):
        return True

    return False


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

    total_seconds = _request_total_seconds(req, process_started_ts)
    caption_text = f"⏱️ Tiempo total: {_fmt_seconds(total_seconds)}"

    filename = (
        f"{req.curp}_FOLIO.pdf"
        if "FOLIO" in (req.act_type or "").upper()
        else f"{req.curp}.pdf"
    )

    instance = req.instance_name or "docifybot8"

    if _store_api_pdf_result(req, db, safe_media_b64, filename, "BASE64_PROVIDER3_API"):
        return

    if req.source_group_id:
        send_group_document_base64(
            req.source_group_id,
            safe_media_b64,
            filename=filename,
            caption=caption_text,
            instance_name=instance
        )
    else:
        send_document_base64(
            req.requester_wa_id,
            safe_media_b64,
            filename=filename,
            caption=caption_text,
            instance_name=instance
        )

    req.provider_media_url = "BASE64_PROVIDER3"
    req.pdf_url = None
    req.status = "DONE"
    req.error_message = None
    req.updated_at = _utc_now_naive()
    db.commit()
    
    _after_done_accounting(req, db)


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


def _notify_client_groups(rows: list, message: str, instance_name: str | None = None):
    sent = set()
    instance = instance_name or "docifybot8"
    for row in rows:
        gid = (row.group_jid or "").strip()
        if gid and gid not in sent:
            try:
                send_group_text(gid, message, instance)
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

                row.is_active = False
                row.warning_sent_0 = True
                row.updated_at = _utc_now_naive()

                print("PROMO_AUTO_BLOCK_OK =", gid, flush=True)

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
        


def _is_api_request(req) -> bool:
    return bool(getattr(req, "api_client_id", None))


def _handle_api_charge_after_done(req, db):
    if not _is_api_request(req):
        return

    if (req.status or "").upper() != "DONE":
        return

    if getattr(req, "api_charged", False):
        print("API_CHARGE_ALREADY_DONE =", req.id, flush=True)
        return

    client = (
        db.query(ApiClient)
        .filter(ApiClient.id == req.api_client_id)
        .with_for_update()
        .first()
    )

    if not client:
        print("API_CHARGE_CLIENT_NOT_FOUND =", req.api_client_id, flush=True)
        return

    price = Decimal(str(req.api_price or client.price_per_done or 5))

    client.credit_balance = Decimal(str(client.credit_balance or 0)) - price
    client.updated_at = _utc_now_naive()

    req.api_charged = True
    req.api_price = price
    req.updated_at = _utc_now_naive()

    db.add(ApiCreditLog(
        api_client_id=client.id,
        request_log_id=req.id,
        amount=-price,
        type="CHARGE",
        note=f"Acta DONE request_id={req.id}",
        created_at=_utc_now_naive(),
    ))

    db.commit()

    print("API_CHARGED_DONE =", {
        "req_id": req.id,
        "api_client_id": client.id,
        "amount": str(price),
        "balance": str(client.credit_balance),
    }, flush=True)


def _store_api_pdf_result(req, db, safe_media_b64: str, filename: str, provider_media_label: str):
    if not _is_api_request(req):
        return False

    raw = (safe_media_b64 or "").strip()
    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]
    raw = raw.replace("\n", "").replace("\r", "").strip()

    req.api_result_base64 = raw
    req.api_result_filename = filename or f"{req.curp}.pdf"
    req.provider_media_url = provider_media_label
    req.pdf_url = None
    req.status = "DONE"
    req.error_message = None
    req.updated_at = _utc_now_naive()
    db.commit()

    _handle_api_charge_after_done(req, db)

    print("API_PDF_STORED_DONE =", {
        "req_id": req.id,
        "filename": req.api_result_filename,
        "b64_len": len(raw),
    }, flush=True)

    return True


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
    p8 = _get_or_create_provider(db, "PROVIDER8", False)
    p9 = _get_or_create_provider(db, "PROVIDER9", False)
    p10 = _get_or_create_provider(db, "PROVIDER10", False)
    p11 = _get_or_create_provider(db, "PROVIDER11", False)
    p12 = _get_or_create_provider(db, "MAYAPROVIDER", False)

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
    if p8.is_enabled:
        enabled.append("PROVIDER8")
    if p9.is_enabled:
        enabled.append("PROVIDER9")
    if p10.is_enabled:
        enabled.append("PROVIDER10")
    if p11.is_enabled:
        enabled.append("PROVIDER11")
    if p12.is_enabled:
        enabled.append("MAYAPROVIDER")

    return enabled


def _is_folio_type(act_type: str | None) -> bool:
    t = str(act_type or "").upper().strip()
    return any(x in t for x in [
        "FOLIO",
        "FOLIADO",
        "FOLIADA",
        "FOLIADOS",
        "FOLIADAS",
    ])


def _is_provider6_blocked_act_type(act_type: str | None) -> bool:
    t = (act_type or "").upper().strip()

    # Escalante NO debe recibir estos tipos, ni aunque vengan como FOLIO.
    return any(x in t for x in [
        "MATRIMONIO",
        "MAT",
        "DEFUNCION",
        "DEFUNCIÓN",
        "DEF",
        "DIVORCIO",
        "DIV",
    ])


def _is_provider6_allowed_request(term: str | None, act_type: str | None) -> bool:
    t = (act_type or "").upper().strip()

    # Primero bloquear MAT / DEF / DIV.
    # Esto evita MATRIMONIO FOLIO, DEFUNCION FOLIO, DIVORCIO FOLIO.
    if _is_provider6_blocked_act_type(t):
        return False

    # CADENA sí puede entrar a Escalante.
    if is_chain(term):
        return True

    # FOLIADA sí puede entrar a Escalante.
    if _is_folio_act(t):
        return True

    # NACIMIENTO sí puede entrar a Escalante.
    if t.startswith("NACIMIENTO") or t.startswith("NAC"):
        return True

    return False


def _pick_provider_by_weight(db: Session, enabled: list[str]) -> str:
    rows = (
        db.query(ProviderSetting)
        .filter(
            ProviderSetting.provider_name.in_(enabled),
            ProviderSetting.is_enabled == True,
        )
        .all()
    )

    weights = {}
    for r in rows:
        weights[r.provider_name] = max(0.0, float(r.weight or 0))

    total = sum(weights.get(p, 0.0) for p in enabled)

    print("PICK_PROVIDER_WEIGHTS =", weights, "TOTAL =", total, flush=True)

    # Si no hay pesos configurados, conserva tu rotación normal
    if total <= 0:
        return ""

    chosen = random.choices(
        enabled,
        weights=[weights.get(p, 0.0) for p in enabled],
        k=1,
    )[0]

    return chosen


def _pick_provider_name(
    db,
    request_id: int,
    source_group_id: str | None = None,
    term: str | None = None,
    act_type: str | None = None,
    instance_name: str | None = None,
) -> str:
    mode = _bot_provider_mode(db, instance_name)
    forced_provider = _provider_from_mode(mode)

    print("BOT_PROVIDER_MODE =", instance_name, mode, flush=True)

    # PERSONAL:MAYAPROVIDER => fuerza proveedor personal y NO cuenta
    # GLOBAL:PROVIDERX      => fuerza proveedor global y SÍ cuenta
    if forced_provider:
        print("BOT_PROVIDER_FORCED =", forced_provider, flush=True)
    
        if forced_provider == "PROVIDER4" and not _is_provider4_eligible(term, act_type):
            raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")
    
        if forced_provider == "PROVIDER6" and not _is_provider6_allowed_request(term, act_type):
            raise RuntimeError("PROVIDER6_ACT_TYPE_NOT_ALLOWED")
    
        return forced_provider

    # GLOBAL_POOL => usa el pool normal del panel principal y SÍ cuenta
    enabled = sorted(_enabled_providers(db))

    print("PICK_PROVIDER_ENABLED =", enabled, flush=True)
    print("PICK_PROVIDER_SOURCE_GROUP_ID =", repr(source_group_id), flush=True)
    print("PICK_PROVIDER_TERM =", repr(term), flush=True)
    print("PICK_PROVIDER_ACT_TYPE =", repr(act_type), flush=True)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    # PROVIDER7 forzado solo para grupos de prueba
    if (
        source_group_id
        and source_group_id in PROVIDER7_TEST_GROUPS
        and "PROVIDER7" in enabled
    ):
        print("FORZANDO PROVIDER7 =", source_group_id, flush=True)
        return "PROVIDER7"

    # PROVIDER4, PROVIDER10 y PROVIDER11 solo si son elegibles para backend tipo Lázaro
    if not _is_provider4_eligible(term, act_type):
        enabled = [p for p in enabled if p not in ("PROVIDER4", "PROVIDER10", "PROVIDER11")]
        print("PROVIDER4_PROVIDER10_PROVIDER11_REMOVED_NOT_ELIGIBLE =", enabled, flush=True)

        if not enabled:
            raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")

    # PROVIDER6 / ACTAS ESCALANTE:
    # Solo debe recibir CADENA, NACIMIENTO y FOLIADA.
    if "PROVIDER6" in enabled and not _is_provider6_allowed_request(term, act_type):
        enabled = [p for p in enabled if p != "PROVIDER6"]
        print("PROVIDER6_REMOVED_NOT_ALLOWED_ACT_TYPE =", {
            "enabled": enabled,
            "term": term,
            "act_type": act_type,
        }, flush=True)
    
        if not enabled:
            raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")

    # PROVIDER4 forzado solo en grupos test
    if PROVIDER4_TEST_GROUPS:
        if (
            source_group_id
            and source_group_id in PROVIDER4_TEST_GROUPS
            and "PROVIDER4" in enabled
        ):
            print("FORZANDO PROVIDER4_TEST_GROUP =", source_group_id, flush=True)
            return "PROVIDER4"

        enabled = [p for p in enabled if p != "PROVIDER4"]
        print("PROVIDER4_REMOVED_NON_TEST_GROUP =", enabled, flush=True)

        if not enabled:
            raise RuntimeError("NO_PROVIDER_ENABLED")

    # Si hay grupos test de provider7, fuera del pool normal
    if PROVIDER7_TEST_GROUPS and "PROVIDER7" in enabled:
        enabled = [p for p in enabled if p != "PROVIDER7"]
        print("PROVIDER7_REMOVED_NON_TEST_GROUP =", enabled, flush=True)

        if not enabled:
            raise RuntimeError("NO_PROVIDER_ENABLED")

    if len(enabled) == 1:
        print("PICK_PROVIDER_SINGLE =", enabled[0], flush=True)
        return enabled[0]
    
    weighted_chosen = _pick_provider_by_weight(db, enabled)
    
    if weighted_chosen:
        print("PICK_PROVIDER_WEIGHTED_CHOSEN =", weighted_chosen, flush=True)
        return weighted_chosen
    
    # fallback: si todos los pesos están en 0, conserva tu rotación anterior
    idx = (request_id - 1) % len(enabled)
    chosen = enabled[idx]
    
    print("PICK_PROVIDER_NORMAL_CHOSEN =", chosen, flush=True)
    return chosen


def _is_folio_act(act_type: str | None) -> bool:
    act_type_up = (act_type or "").upper().strip()
    return (
        "FOLI" in act_type_up
        or " FOL " in f" {act_type_up} "
    )


def _pick_provider1_group(term: str | None, act_type: str, request_id: int) -> str:
    act_type_up = (act_type or "").upper().strip()

    nacimiento_group_1 = (settings.PROVIDER_GROUP_NACIMIENTO_1 or "").strip()
    nacimiento_group_2 = (settings.PROVIDER_GROUP_NACIMIENTO_2 or "").strip()

    especiales_group = (settings.PROVIDER_GROUP_ESPECIALES or "").strip()
    foliadas_group = (settings.PROVIDER_GROUP_FOLIADAS or "").strip()

    is_nacimiento = act_type_up.startswith("NACIMIENTO") or act_type_up.startswith("NAC")
    is_cadena_req = is_chain(term)
    is_folio_req = _is_folio_act(act_type_up)
    is_curp_req = _is_curp_term(term)

    # 1. FOLIADAS -> grupo foliadas
    if is_folio_req:
        if not foliadas_group:
            raise RuntimeError("NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED")
        return foliadas_group

    # 2. TODAS LAS CADENAS -> grupo foliadas
    if is_cadena_req:
        if not foliadas_group:
            raise RuntimeError("NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED")
        return foliadas_group

    # 3. NACIMIENTO POR CURP -> repartir entre grupo 1 y grupo 2
    if is_nacimiento and is_curp_req:
        nacimiento_groups = [
            group
            for group in (nacimiento_group_1, nacimiento_group_2)
            if group
        ]

        if not nacimiento_groups:
            raise RuntimeError("NO_BIRTH_PROVIDER_GROUP_CONFIGURED")

        return nacimiento_groups[(request_id - 1) % len(nacimiento_groups)]

    # 4. NACIMIENTO que NO sea CURP -> grupo 1 normal
    if is_nacimiento:
        if not nacimiento_group_1:
            raise RuntimeError("NO_BIRTH_PROVIDER_GROUP_CONFIGURED")
        return nacimiento_group_1

    # 5. MAT / DEF / DIV normales -> grupo especiales
    if not especiales_group:
        raise RuntimeError("NO_SPECIAL_PROVIDER_GROUP_CONFIGURED")
    return especiales_group


def _pick_provider6_group(term: str | None, act_type: str, request_id: int) -> str:
    act_type_up = (act_type or "").upper().strip()

    nacimiento_group_1 = (settings.PROVIDER6_GROUP_1_NACIMIENTO or "").strip()
    nacimiento_group_2 = (settings.PROVIDER6_GROUP_2_NACIMIENTO or "").strip()
    especiales_group = (settings.PROVIDER6_GROUP_ESPECIALES or "").strip()
    foliadas_group = (settings.PROVIDER6_GROUP_FOLIADAS or "").strip()

    is_folio_req = _is_folio_act(act_type_up)
    is_cadena_req = is_chain(term)
    is_nacimiento_req = act_type_up.startswith("NACIMIENTO") or act_type_up.startswith("NAC")

    # Escalante NO recibe matrimonio, defunción ni divorcio.
    if not _is_provider6_allowed_request(term, act_type_up):
        raise RuntimeError("PROVIDER6_ACT_TYPE_NOT_ALLOWED")

    # 1. FOLIADAS -> grupo foliadas
    if is_folio_req:
        if not foliadas_group:
            raise RuntimeError("NO_PROVIDER6_FOLIADAS_GROUP_CONFIGURED")
        return foliadas_group

    # 2. CADENAS -> grupo especiales
    # Nota: aquí se usa PROVIDER6_GROUP_ESPECIALES como grupo para cadenas.
    if is_cadena_req:
        if not especiales_group:
            raise RuntimeError("NO_PROVIDER6_ESPECIALES_GROUP_CONFIGURED")
        return especiales_group

    # 3. NACIMIENTO -> grupos nacimiento 1 y 2
    if is_nacimiento_req:
        nacimiento_groups = [
            group
            for group in (nacimiento_group_1, nacimiento_group_2)
            if group
        ]

        if not nacimiento_groups:
            raise RuntimeError("NO_PROVIDER6_NACIMIENTO_GROUP_CONFIGURED")

        return nacimiento_groups[(request_id - 1) % len(nacimiento_groups)]

    # Por seguridad, cualquier otro tipo queda bloqueado.
    raise RuntimeError("PROVIDER6_ACT_TYPE_NOT_ALLOWED")


def _pick_provider_group(
    provider_name: str,
    term: str | None,
    act_type: str,
    request_id: int
) -> str | None:
    if provider_name == "PROVIDER1":
        return _pick_provider1_group(term, act_type, request_id)

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

    if provider_name in ("PROVIDER4", "PROVIDER10", "PROVIDER11"):
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
        return _pick_provider6_group(term, act_type, request_id)

    if provider_name == "PROVIDER7":
        return None

    if provider_name == "PROVIDER8":
        provider8_groups = [
            settings.PROVIDER8_GROUP_1,
            settings.PROVIDER8_GROUP_2,
        ]
        provider8_groups = [g for g in provider8_groups if g]

        if not provider8_groups:
            raise RuntimeError("PROVIDER8_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider8_groups)
        return provider8_groups[idx]

    if provider_name == "PROVIDER9":
        provider9_groups = [
            settings.PROVIDER9_GROUP_1,
            settings.PROVIDER9_GROUP_2,
        ]
        provider9_groups = [g for g in provider9_groups if g]

        if not provider9_groups:
            raise RuntimeError("PROVIDER9_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider9_groups)
        return provider9_groups[idx]

    if provider_name == "MAYAPROVIDER":
        provider11_groups = [
            settings.MAYAPROVIDER_GROUP_1,
            settings.MAYAPROVIDER_GROUP_2,
        ]
        provider11_groups = [g for g in provider11_groups if g]
    
        if not provider11_groups:
            raise RuntimeError("MAYAPROVIDER_GROUPS_NOT_CONFIGURED")
    
        idx = (request_id - 1) % len(provider11_groups)
        return provider11_groups[idx]

    raise RuntimeError("UNKNOWN_PROVIDER")


def _build_provider_message(provider_name: str, term: str, act_type: str) -> str | None:
    if provider_name in ("PROVIDER1", "PROVIDER2", "PROVIDER5", "PROVIDER6", "PROVIDER8", "PROVIDER9", "MAYAPROVIDER"):
        if is_chain(term):
            return f"{term}"
        provider_type = provider_label_for_type(act_type)
        return f"{term} {provider_type}"

    if provider_name == "PROVIDER3":
        return None

    if provider_name in ("PROVIDER4", "PROVIDER10", "PROVIDER11"):
        return None

    if provider_name == "PROVIDER7":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _provider_sender_instance(provider_name: str, req) -> str:
    provider_name = (provider_name or "").strip().upper()

    if provider_name == "MAYAPROVIDER":
        return req.instance_name or settings.EVOLUTION_INSTANCE

    return settings.EVOLUTION_PROVIDER_INSTANCE


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


def _process_provider4(req, db, provider_name: str = "PROVIDER4"):

    provider_name = (provider_name or "PROVIDER4").strip().upper()

    term = (req.curp or "").strip()
    chain_mode = is_chain(term) or bool(re.fullmatch(r"\d{15,25}", term))

    print(f"{provider_name}_PROCESS_TERM =", term, flush=True)
    print(f"{provider_name}_PROCESS_CHAIN_MODE =", chain_mode, flush=True)

    if not _is_curp_term(term) and not chain_mode:
        raise RuntimeError(f"{provider_name}_NOT_CURP_OR_CHAIN")

    # La restricción de grupos test solo aplica al Provider4 original.
    if provider_name == "PROVIDER4" and PROVIDER4_TEST_GROUPS and req.source_group_id not in PROVIDER4_TEST_GROUPS:
        raise RuntimeError("PROVIDER4_NOT_ALLOWED_GROUP")

    hid_key = f"{provider_name}_HID"

    setting = (
        db.query(ProviderSetting)
        .filter(ProviderSetting.provider_name == hid_key)
        .first()
    )

    default_hid_map = {
        "PROVIDER10": "D0cuExprRServ2",
        "PROVIDER11": "D0cuExprRServ3",
    }
    
    default_hid = default_hid_map.get(provider_name)
    hid = setting.value if setting and setting.value else default_hid

    print(f"{provider_name}_HID_KEY =", hid_key, flush=True)
    print(f"{provider_name}_HID_USING =", hid, flush=True)

    client = Provider4Client(hid=hid)

    tipoa = _provider4_tipo_acta(req.act_type)
    inc_folio = "FOLIO" in (req.act_type or "").upper().strip()

    try:
        pdf_bytes = client.process_and_download(
            term=term,
            tipoa=tipoa,
            inc_folio=inc_folio,
            is_chain=chain_mode,
        )

    except Exception as e:
        err = str(e)

        # Provider4Client internamente todavía puede lanzar errores PROVIDER4_*.
        # Si quien está procesando es PROVIDER10, normalizamos el prefijo
        # para que el fallback y los logs lo reconozcan como PROVIDER10.
        if provider_name != "PROVIDER4" and err.startswith("PROVIDER4_"):
            err = f"{provider_name}_{err[len('PROVIDER4_'):]}"

        raise RuntimeError(err) from e

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
    if shared_key:
        used_before = max(
            int(leader.used_actas or 0),
            sum(int(r.shared_group_used_actas or 0) for r in rows)
        )
    else:
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

    extra_shared_msg_long = ""
    extra_shared_msg_short = ""
    extra_shared_msg_block = ""
    
    if shared_key:
        extra_shared_msg_long = "Este aviso aplica para todos los grupos asociados a esta bolsa compartida.\n\n"
        extra_shared_msg_short = "Este aviso aplica para todos los grupos asociados a esta bolsa."
        extra_shared_msg_block = "Todos los grupos asociados a esta bolsa quedarán bloqueados automáticamente hasta nueva recarga.\n\n"

    crossed_0 = available_after <= 0
    crossed_10 = available_before > 10 and available_after <= 10 and not bool(getattr(leader, "warning_sent_10", False))
    crossed_50 = available_before > 50 and available_after <= 50 and not bool(getattr(leader, "warning_sent_50", False))
    crossed_100 = available_before > 100 and available_after <= 100 and not bool(getattr(leader, "warning_sent_100", False))
    crossed_200 = available_before > 200 and available_after <= 200 and not bool(getattr(leader, "warning_sent_200", False))

    if crossed_0:
        msg = (
            "❌ *Paquete agotado*\n\n"
            "Tu paquete promocional ha sido consumido en su totalidad.\n"
            "Saldo disponible: *0 actas*.\n\n"
            f"{extra_shared_msg_block}"
            "Quedamos atentos."
        )
        notify_level = "0"
        for row in rows:
            row.warning_sent_0 = True
            row.is_active = False

    elif crossed_10:
        msg = (
            "🚨 *Saldo crítico*\n\n"
            f"Tu paquete promocional cuenta actualmente con solo *{available_after} actas disponibles*.\n\n"
            f"{extra_shared_msg_long}"
            "Quedamos atentos."
        )
        notify_level = "10"
        for row in rows:
            row.warning_sent_10 = True

    elif crossed_50:
        msg = (
            "⚠️ *Aviso importante de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{extra_shared_msg_long}"
            "Quedamos atentos."
        )
        notify_level = "50"
        for row in rows:
            row.warning_sent_50 = True

    elif crossed_100:
        msg = (
            "⚠️ *Aviso de saldo*\n\n"
            f"Tu paquete promocional cuenta actualmente con *{available_after} actas disponibles*.\n\n"
            f"{extra_shared_msg_long}"
            "Quedamos atentos."
        )
        notify_level = "100"
        for row in rows:
            row.warning_sent_100 = True

    elif crossed_200:
        msg = (
            "ℹ️ *Aviso de saldo*\n\n"
            f"Actualmente cuentas con *{available_after} actas disponibles* en tu paquete promocional.\n\n"
            f"{extra_shared_msg_short}"
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
            instance = req.instance_name or "docifybot8"
            send_group_text(current_group_row.group_jid, individual_limit_msg, instance)
        except Exception as e:
            print("SHARED_GROUP_LIMIT_NOTIFY_AFTER_DONE_ERROR =", str(e), flush=True)

    if crossed_0:
        try:
            _block_client_groups(rows if shared_key else [current])
            db.commit()
        except Exception as e:
            db.rollback()
            print("PROMOTION_BLOCK_ERROR =", str(e), flush=True)

    if msg and notify_level:
        notify_scope = shared_key if shared_key else current.group_jid

        if notify_level == "0":
            try:
                if shared_key:
                    instance = req.instance_name or "docifybot8"
                    _notify_client_groups(rows, msg, instance)
                else:
                    instance = req.instance_name or "docifybot8"
                    send_group_text(current.group_jid, msg, instance)
            except Exception as e:
                print("PROMOTION_NOTIFY_LEVEL_0_ERROR =", str(e), flush=True)
            return

        notify_key = f"promo_notify:{notify_scope}:{notify_level}"
        first_notify = redis_conn.set(notify_key, "1", ex=1800, nx=True)

        if first_notify:
            if shared_key:
                try:
                    instance = req.instance_name or "docifybot8"
                    _notify_client_groups(rows, msg, instance)
                except Exception as e:
                    print("PROMOTION_SHARED_GROUP_NOTIFY_ERROR =", str(e), flush=True)
            else:
                try:
                    instance = req.instance_name or "docifybot8"
                    send_group_text(current.group_jid, msg, instance)
                except Exception as e:
                    print("PROMOTION_SINGLE_GROUP_NOTIFY_ERROR =", str(e), flush=True)
        else:
            print("PROMOTION_NOTIFY_DUPLICATE_IGNORED =", notify_key, flush=True)


def _start_provider3_flow(req, db):
    provider_name = "PROVIDER3"
    provider_group_id = _pick_provider_group(provider_name, req.curp, req.act_type, req.id)
    text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

    req.provider_name = provider_name
    req.provider_group_id = provider_group_id
    req.provider_message = text_to_provider
    req.updated_at = _utc_now_naive()
    db.commit()

    print("FALLBACK_PROVIDER_NAME =", provider_name, flush=True)
    print("FALLBACK_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
    print("FALLBACK_PROVIDER_TEXT =", text_to_provider, flush=True)

    sender_instance = _provider_sender_instance(provider_name, req) 
    send_group_text(provider_group_id, text_to_provider, sender_instance)


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


def _validate_pdf_contains_electronic_id_or_code(pdf_bytes: bytes, value: str) -> bool:
    expected = _normalize_alnum(value)
    if not expected:
        return True

    text = _extract_pdf_visible_text(pdf_bytes)
    if not text or len(text.strip()) < 30:
        print("PROVIDER_VALIDATE_ELECTRONIC_ID_TEXT_TOO_SHORT", flush=True)
        return False

    normalized_text = _normalize_alnum(text)

    found = expected in normalized_text

    print("PROVIDER_VALIDATE_EXPECTED_ELECTRONIC_ID_OR_CODE =", expected, flush=True)
    print("PROVIDER_VALIDATE_ELECTRONIC_ID_OR_CODE_FOUND =", found, flush=True)

    return found


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

    # Si se detectan CURPs visibles, la esperada debe aparecer.
    # Ya NO rechazar solo porque existan varias.
    if found_curps:
        if expected not in found_curps:
            return False

        if len(found_curps) > 1:
            print("PROVIDER_VALIDATE_MULTIPLE_CURPS_ALLOWED = TRUE", flush=True)

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


def _after_done_accounting(req, db):
    if _is_api_request(req):
        _handle_api_charge_after_done(req, db)
        print("API_SKIP_BOT_LIMIT_AND_PROMOS =", req.id, flush=True)
        return

    if _request_is_no_accounting(req, db):
        print(
            "PRIVATE_PROVIDER_SKIP_ACCOUNTING_WORKER =",
            {
                "req_id": req.id,
                "instance_name": req.instance_name,
                "provider_name": req.provider_name,
                "source_group_id": req.source_group_id,
            },
            flush=True,
        )
        return

    try:
        if req.instance_name:
            used, limit_value, blocked_now = increment_bot_used_and_maybe_block(
                db,
                req.instance_name
            )
            print("BOT_USED_AFTER_DONE =", used, flush=True)
            print("BOT_LIMIT =", limit_value, flush=True)
            print("BOT_BLOCKED_NOW =", blocked_now, flush=True)
        else:
            print("BOT_INSTANCE_MISSING_FOR_REQ =", req.id, flush=True)

    except Exception as bot_limit_exc:
        print("BOT_LIMIT_UPDATE_ERROR =", str(bot_limit_exc), flush=True)

    try:
        _handle_group_promotion_after_done(req, db)
    except Exception as promo_exc:
        print("PROMOTION_UPDATE_ERROR =", str(promo_exc), flush=True)


NO_TIME_CAPTION_GROUPS = {
    "120363408668441985@g.us",
    "120363421166637606@g.us",
    "120363427267191472@g.us",
}


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
        print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)

        process_started_ts = time.perf_counter()

        req.status = "PROCESSING"
        req.updated_at = _utc_now_naive()
        db.commit()

        if req.source_group_id and not _current_mode_is_personal(db, req.instance_name):
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
                        instance = req.instance_name or "docifybot8"
                        send_group_text(req.source_group_id, msg, instance)
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
            req.instance_name,
        )
        provider_group_id = _pick_provider_group(provider_name, req.curp, req.act_type, req.id)
        text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

        req.provider_name = provider_name
        req.provider_group_id = provider_group_id
        req.provider_message = text_to_provider
        req.updated_at = _utc_now_naive()
        db.commit()

        print("WORKER_PROVIDER_NAME =", provider_name, flush=True)
        print("WORKER_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
        print("WORKER_TEXT_TO_PROVIDER =", text_to_provider, flush=True)

        current_queue = _current_queue_name()

        print(
            "WORKER_CURRENT_QUEUE =",
            {
                "request_id": req.id,
                "provider_name": provider_name,
                "queue": current_queue,
            },
            flush=True,
        )

        if _should_reroute_to_slow(provider_name) and current_queue != SLOW_PROVIDER_QUEUE_NAME:
            req.status = "QUEUED"
            req.updated_at = _utc_now_naive()
            db.commit()

            job = slow_request_queue.enqueue(process_request, req.id)

            print(
                "REQUEST_REROUTED_TO_SLOW_QUEUE =",
                {
                    "request_id": req.id,
                    "provider_name": provider_name,
                    "from_queue": current_queue,
                    "to_queue": SLOW_PROVIDER_QUEUE_NAME,
                    "job_id": job.id,
                },
                flush=True,
            )

            return

        if provider_name in ("PROVIDER1", "PROVIDER2", "PROVIDER5", "PROVIDER6", "PROVIDER8", "PROVIDER9", "MAYAPROVIDER"):
            print("PROVIDER_SEND_TO_PROVIDER =", req.id, time.time(), flush=True)
        
            send_ok = False
            last_err = None
        
            for attempt in range(3):
                try:
                    sender_instance = _provider_sender_instance(provider_name, req)
                    print("PROVIDER_SENDER_INSTANCE =", sender_instance, flush=True)
                    
                    resp_json = send_group_text(provider_group_id, text_to_provider, sender_instance)
                    
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
                instance = req.instance_name or "docifybot8"

                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
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
                provider_group_id = _pick_provider_group(provider_name, req.curp, req.act_type, req.id)
                text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)
        
                req.provider_name = provider_name
                req.provider_group_id = provider_group_id
                req.provider_message = text_to_provider
                req.status = "PROCESSING"
                req.error_message = None
                req.updated_at = _utc_now_naive()
                db.commit()
        
                sender_instance = _provider_sender_instance(provider_name, req) 
                send_group_text(provider_group_id, text_to_provider, sender_instance)
        
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
        
            total_seconds = _request_total_seconds(req, process_started_ts)

            caption_text = ""
            if req.source_group_id not in NO_TIME_CAPTION_GROUPS:
                caption_text = f"⏱️ Tiempo total: {_fmt_seconds(total_seconds)}"
        
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

            instance = req.instance_name or "docifybot8"

            if _store_api_pdf_result(req, db, safe_media_b64, filename, f"BASE64_{provider_name}_API"):
                return

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print("PROVIDER3_SEND_INSTANCE =", instance, flush=True)
        
            for attempt in range(3):
                try:
                    if req.source_group_id:
                        send_group_document_base64(
                            req.source_group_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text,
                            instance_name=instance
                        )
                    else:
                        send_document_base64(
                            req.requester_wa_id,
                            safe_media_b64,
                            filename=f"{req.curp}.pdf",
                            caption=caption_text,
                            instance_name=instance
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

            _after_done_accounting(req, db)
        
            return

        if provider_name in ("PROVIDER4", "PROVIDER10", "PROVIDER11"):
            provider4_started_ts = time.perf_counter()
        
            try:
                provider4_result = _process_provider4(req, db, provider_name=provider_name)

                pdf_bytes = provider4_result["pdf_bytes"]

                term = (req.curp or "").strip()
                chain_mode = is_chain(term) or bool(re.fullmatch(r"\d{15,25}", term))
                
                print(f"{provider_name}_WORKER_TERM =", term, flush=True)
                print(f"{provider_name}_WORKER_CHAIN_MODE =", chain_mode, flush=True)
                
                # Para cadena NO se valida tipo de acta.
                # La cadena ya identifica el acta; puede ser NAC/MAT/DEF/DIV aunque req.act_type diga NACIMIENTO.
                if not chain_mode:
                    if not _validate_act_type_pdf(pdf_bytes, req.act_type):
                        raise RuntimeError(f"{provider_name}_WRONG_ACT_TYPE")
                
                if chain_mode:
                    if not _validate_pdf_contains_electronic_id_or_code(pdf_bytes, term):
                        print(f"{provider_name}_VALIDATE_FAIL_REQ_ELECTRONIC_ID_OR_CODE =", term, flush=True)
                        raise RuntimeError(f"{provider_name}_WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF:{term}")
                else:
                    if not _validate_pdf_matches_term(pdf_bytes, term, req.act_type):
                        print(f"{provider_name}_VALIDATE_FAIL_REQ_CURP =", term, flush=True)
                        raise RuntimeError(f"{provider_name}_WRONG_CURP_IN_PDF:{term}")
        
            except Exception as p4_exc:
                p4_err = str(p4_exc)
                p4_elapsed = time.perf_counter() - provider4_started_ts
                enabled = _enabled_providers(db)
            
                if (
                    p4_err.startswith(f"{provider_name}_WRONG_CURP_IN_PDF")
                    or p4_err.startswith(f"{provider_name}_WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF")
                    or p4_err.startswith(f"{provider_name}_WRONG_ACT_TYPE")
                ):
                    msg = (
                        f"⚠️ Solicitud sin éxito en Registro Civil\n"
                        f"Dato: {req.curp}\n"
                        f"Tipo: {req.act_type}\n\n"
                        f"Reenviar nuevamente en unos minutos"
                    )

                    instance = req.instance_name or "docifybot8"

                    if req.source_group_id:
                        if should_send_extra_text(req.source_group_id):
                            send_group_text(req.source_group_id, msg, instance)
                    else:
                        from app.services.evolution import send_text
                        send_text(req.requester_wa_id, msg, instance)
            
                    req.status = "ERROR"
                    req.error_message = p4_err
                    req.updated_at = _utc_now_naive()
                    db.commit()
            
                    _notify_support_error(req, p4_err, f"PDF cruzado o tipo incorrecto devuelto por {provider_name}")
                    return
            
                fallback_errors = (
                    p4_err.startswith(f"{provider_name}_BACKEND_FAILED:")
                    or p4_err.startswith(f"{provider_name}_VGET_FAILED:")
                    or p4_err.startswith(f"{provider_name}_HISTORY_FAILED:")
                    or p4_err.startswith(f"{provider_name}_HISTORY_NOT_CONFIRMED_PDF:")
                    or p4_err.startswith(f"{provider_name}_HISTORY_NOT_CONFIRMED_FOLIO:")
                    or p4_err.startswith(f"{provider_name}_NO_PDF_LINK_FOR:")
                    or p4_err.startswith(f"{provider_name}_NO_FOLIO_LINK_FOR:")
                    or p4_err.startswith(f"{provider_name}_DOWNLOAD_FAILED:")
                    or p4_err.startswith(f"{provider_name}_FOLIO_DOWNLOAD_FAILED:")
                    or p4_err.startswith(f"{provider_name}_WRONG_ACT_TYPE")
                    or p4_err.startswith(f"{provider_name}_WRONG_CURP_IN_PDF")
                    or p4_err.startswith(f"{provider_name}_WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF")
                    or "Read timed out" in p4_err
                )
            
                should_fallback = (
                    p4_err.startswith(f"{provider_name}_NO_FORM_ACTION")
                    or (fallback_errors and p4_elapsed >= 90)
                )
            
                if should_fallback:
                    if _current_mode_is_personal(db, req.instance_name):
                        print("PERSONAL_MODE_NO_GLOBAL_FALLBACK =", req.id, flush=True)
                        raise
                
                    whatsapp_fallbacks = [
                        p for p in enabled
                        if p in {
                            "PROVIDER1",
                            "PROVIDER2",
                            "PROVIDER5",
                            "PROVIDER6",
                            "PROVIDER8",
                            "PROVIDER9",
                        }
                        and p != provider_name
                    ]
                
                    if whatsapp_fallbacks:
                        fallback_provider = _pick_provider_by_weight(db, whatsapp_fallbacks) or whatsapp_fallbacks[0]
                
                        req.provider_name = fallback_provider
                        req.provider_group_id = _pick_provider_group(
                            fallback_provider,
                            req.curp,
                            req.act_type,
                            req.id,
                        )
                        req.provider_message = _build_provider_message(
                            fallback_provider,
                            req.curp,
                            req.act_type,
                        )
                        req.status = "PROCESSING"
                        req.error_message = f"{provider_name}_FAILED_FALLBACK_TO_{fallback_provider}:{p4_err[:500]}"
                        req.updated_at = _utc_now_naive()
                        db.commit()
                
                        print(
                            "LAZARO_FAILED_FALLBACK_TO_WHATSAPP =",
                            {
                                "req_id": req.id,
                                "failed_provider": provider_name,
                                "fallback_provider": fallback_provider,
                                "provider_group_id": req.provider_group_id,
                                "provider_message": req.provider_message,
                                "elapsed": round(p4_elapsed, 2),
                                "err": p4_err[:300],
                            },
                            flush=True,
                        )
                
                        send_group_text(
                            req.provider_group_id,
                            req.provider_message,
                            _provider_sender_instance(fallback_provider, req),
                        )
                
                        print(
                            "LAZARO_FALLBACK_SENT_TO_WHATSAPP_PROVIDER =",
                            {
                                "req_id": req.id,
                                "fallback_provider": fallback_provider,
                            },
                            flush=True,
                        )
                
                        return
                
                    if "PROVIDER3" not in enabled:
                        msg = (
                            "⚠️ *Proveedor temporalmente no disponible*\n\n"
                            "La búsqueda no pudo completarse correctamente en este momento.\n\n"
                            "Intenta nuevamente más tarde."
                        )
                
                        instance = req.instance_name or "docifybot8"
                        if req.source_group_id:
                            if should_send_extra_text(req.source_group_id):
                                send_group_text(req.source_group_id, msg, instance)
                        else:
                            from app.services.evolution import send_text
                            send_text(req.requester_wa_id, msg, instance)
                
                        req.status = "ERROR"
                        req.error_message = f"{provider_name}_FALLBACK_NO_PROVIDER_AVAILABLE:{p4_err}"
                        req.updated_at = _utc_now_naive()
                        db.commit()
                        return
            
                    print(
                        f"{provider_name}_FALLBACK_TO_PROVIDER3 =",
                        {"req_id": req.id, "elapsed": p4_elapsed, "err": p4_err},
                        flush=True,
                    )
                    
                    if _current_mode_is_personal(db, req.instance_name):
                        print("PERSONAL_MODE_NO_GLOBAL_FALLBACK =", req.id, flush=True)
                        raise
                        
                    _fallback_to_provider3_web(req, db, process_started_ts)
                    return
            
                raise
        
            safe_media_b64 = base64.b64encode(pdf_bytes).decode()
        
            total_seconds = _request_total_seconds(req, process_started_ts)

            caption_text = ""
            if req.source_group_id not in NO_TIME_CAPTION_GROUPS:
                caption_text = f"⏱️ Tiempo total: {_fmt_seconds(total_seconds)}"
        
            print(f"{provider_name}_CAPTION =", caption_text, flush=True)
        
            delivery_key = f"{provider_name.lower()}_delivery:{req.id}:{req.curp}:{req.source_group_id or req.requester_wa_id}"
        
            if redis_conn.exists(delivery_key):
                print(f"{provider_name}_DUPLICATE_DELIVERY_IGNORED =", delivery_key, flush=True)
                return
        
            send_ok = False
        
            filename = (
                f"{req.curp}_FOLIO.pdf"
                if "FOLIO" in (req.act_type or "").upper()
                else f"{req.curp}.pdf"
            )

            instance = req.instance_name or "docifybot8"

            if _store_api_pdf_result(req, db, safe_media_b64, filename, f"BASE64_{provider_name}_API"):
                return

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print(f"{provider_name}_SEND_INSTANCE =", instance, flush=True)
        
            for attempt in range(3):
                try:
                    if req.source_group_id:
                        send_group_document_base64(
                            req.source_group_id,
                            safe_media_b64,
                            filename=filename,
                            caption=caption_text,
                            instance_name=instance
                        )
                    else:
                        send_document_base64(
                            req.requester_wa_id,
                            safe_media_b64,
                            filename=filename,
                            caption=caption_text,
                            instance_name=instance
                        )
        
                    send_ok = True
                    print(f"{provider_name}_SEND_OK_ATTEMPT_{attempt+1} =", req.id, flush=True)
                    print(f"{provider_name}_SEND_INSTANCE =", instance, flush=True)
                    break
        
                except Exception as e:
                    print(f"{provider_name}_SEND_ATTEMPT_{attempt+1}_ERROR =", str(e), flush=True)
                    if attempt == 2:
                        raise
                    time.sleep(2)
        
            if not send_ok:
                raise RuntimeError(f"{provider_name}_PDF_SEND_FAILED")
        
            redis_conn.set(delivery_key, "1", ex=3600)
        
            req.provider_media_url = f"BASE64_{provider_name}"
            req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = _utc_now_naive()
            db.commit()

            _after_done_accounting(req, db)
        
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
                        instance = req.instance_name or "docifybot8"
        
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
                    msg = (
                        f"⚠️ Solicitud sin éxito en Registro Civil\n"
                        f"Dato: {req.curp}\n"
                        f"Tipo: {req.act_type}\n\n"
                        f"Reenviar nuevamente en unos minutos"
                    )
        
                    instance = req.instance_name or "docifybot8"

                    if req.source_group_id:
                        if should_send_extra_text(req.source_group_id):
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
        
            total_seconds = _request_total_seconds(req, process_started_ts)
            caption_text = f"⏱️ Tiempo total: {_fmt_seconds(total_seconds)}"
        
            filename = (
                f"{req.curp}_FOLIO.pdf"
                if "FOLIO" in (req.act_type or "").upper()
                else f"{req.curp}.pdf"
            )

            instance = req.instance_name or "docifybot8"

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print("PROVIDER7_SEND_INSTANCE =", instance, flush=True)
        
            if req.source_group_id:
                send_group_document_base64(
                    req.source_group_id,
                    safe_media_b64,
                    filename=filename,
                    caption=caption_text,
                    instance_name=instance
                )
            else:
                send_document_base64(
                    req.requester_wa_id,
                    safe_media_b64,
                    filename=filename,
                    caption=caption_text,
                    instance_name=instance
                )
        
            req.provider_media_url = "BASE64_PROVIDER7"
            req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = _utc_now_naive()
            db.commit()

            _after_done_accounting(req, db)
        
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

                instance = req.instance_name or "docifybot8" 
                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
                        send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

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

                instance = req.instance_name or "docifybot8"
                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
                        send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

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

                instance = req.instance_name or "docifybot8"

                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
                        send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

                _notify_support_error(req, err, msg)
                return
    
            if (
                err.startswith("PROVIDER3_NO_RECORD")
                or err.startswith("PROVIDER4_NO_RECORD")
                or err.startswith("PROVIDER10_NO_RECORD")
                or err.startswith("PROVIDER11_NO_RECORD")
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

                instance = req.instance_name or "docifybot8"
                if req.source_group_id:
                    send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

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
                
                instance = req.instance_name or "docifybot8"
                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
                        send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

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

                instance = req.instance_name or "docifybot8"

                if req.source_group_id:
                    if should_send_extra_text(req.source_group_id):
                        send_group_text(req.source_group_id, msg, instance)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg, instance)

                _notify_support_error(req, err, msg)
                return

            req.status = "ERROR"
            req.error_message = err
            db.commit()
            _notify_support_error(req, err, "ERROR NO CONTROLADO EN WORKER")
        raise
        
    finally:
        db.close()
