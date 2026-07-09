import base64
import time
import threading
import re
import json
import random
import requests
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from app.db import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.models import RequestLog, ProviderSetting, AppSetting, GroupPromotion, ApiClient, ApiCreditLog
from app.services.evolution import send_group_text, send_text, send_document_base64, send_group_document_base64
from app.config import settings
from app.utils.curp import provider_label_for_type, is_chain
from app.services.provider3 import Provider3Client, decode_pdf_base64
from app.services.provider4 import Provider4Client
from app.services.provider7 import Provider7Client
from rq import get_current_job
from app.queue import redis_conn, request_queue, slow_request_queue
from app.provider_status_cache import refresh_providers_status
from app.utils.bot_limits import increment_bot_used_and_maybe_block
from app.pdf_storage import save_request_pdf_to_r2, generate_r2_presigned_download_url

from zoneinfo import ZoneInfo

from io import BytesIO
from pypdf import PdfReader

PROVIDER4_TEST_GROUPS = set()
PROVIDER7_TEST_GROUPS = set()

SLOW_PROVIDER_QUEUE_NAME = "actas_slow"
SLOW_PROVIDERS = {"PROVIDER4", "PROVIDER10", "PROVIDER11"}

PROVIDER4_NEW_FLOW_TTL_SEC = 60 * 20
PROVIDER4_NEW_CHECK_DELAY_SEC = 30
PROVIDER4_NEW_MAX_CHECK_ATTEMPTS = 90

API_STALE_TIMEOUT_MINUTES = 45

BLOCKED_INSTANCES_KEY = "blocked_instances_no_response"
ADMIN_BLOCKED_INSTANCES_KEY = "admin_blocked_instances_no_minipanel_unlock"


def _worker_redis_sismember_str(key: str, value: str) -> bool:
    value = (value or "").strip()

    if not value:
        return False

    try:
        if redis_conn.sismember(key, value):
            return True

        if redis_conn.sismember(key, value.encode("utf-8")):
            return True

        return False

    except Exception as e:
        print(
            "WORKER_BLOCK_CHECK_REDIS_ERROR =",
            {
                "key": key,
                "value": value,
                "error": repr(e),
            },
            flush=True,
        )
        return False


def _worker_is_instance_blocked(instance_name: str | None) -> bool:
    instance_name = (instance_name or "").strip()

    if not instance_name:
        return False

    return bool(
        _worker_redis_sismember_str(BLOCKED_INSTANCES_KEY, instance_name)
        or _worker_redis_sismember_str(ADMIN_BLOCKED_INSTANCES_KEY, instance_name)
    )


def _worker_stop_if_instance_blocked(req, db, label: str = "WORKER_BLOCKED_INSTANCE") -> bool:
    instance_name = (getattr(req, "instance_name", "") or "").strip()

    if not _worker_is_instance_blocked(instance_name):
        return False

    current_status = (getattr(req, "status", "") or "").strip().upper()

    # MUY IMPORTANTE:
    # No tocar solicitudes ya terminadas. Un job viejo en Redis/RQ no debe
    # convertir un DONE o ERROR final en ERROR por bloqueo posterior.
    if current_status in {"DONE", "ERROR"}:
        print(
            f"{label}_SKIP_TERMINAL_STATUS =",
            {
                "request_id": getattr(req, "id", None),
                "curp": getattr(req, "curp", None),
                "act_type": getattr(req, "act_type", None),
                "instance_name": instance_name,
                "status": current_status,
                "error_message": getattr(req, "error_message", None),
            },
            flush=True,
        )
    
        # Importante:
        # Si el bot está bloqueado y el request ya está en estado terminal,
        # no lo modificamos, pero SÍ detenemos el procesamiento.
        return True

    now = _utc_now_naive()

    req.status = "ERROR"
    req.updated_at = now
    req.error_message = "BOT_BLOCKED_BEFORE_PROVIDER_SUBMIT"
    db.commit()

    try:
        _provider4_new_clear_flow(req.id)
    except Exception as clear_exc:
        print(
            f"{label}_CLEAR_PROVIDER4_FLOW_ERROR =",
            {
                "request_id": getattr(req, "id", None),
                "error": str(clear_exc),
            },
            flush=True,
        )

    print(
        f"{label}_SKIPPED_PROVIDER =",
        {
            "request_id": getattr(req, "id", None),
            "curp": getattr(req, "curp", None),
            "act_type": getattr(req, "act_type", None),
            "instance_name": instance_name,
            "source_group_id": getattr(req, "source_group_id", None),
            "provider_name": getattr(req, "provider_name", None),
            "status": getattr(req, "status", None),
            "error_message": getattr(req, "error_message", None),
        },
        flush=True,
    )

    return True


def _current_queue_name() -> str:
    try:
        job = get_current_job(connection=redis_conn)
        return (getattr(job, "origin", "") or "").strip()
    except Exception as e:
        print("CURRENT_QUEUE_NAME_ERROR =", str(e), flush=True)
        return ""


def _should_reroute_to_slow(provider_name: str | None) -> bool:
    return (provider_name or "").strip().upper() in SLOW_PROVIDERS


def _provider4_new_flow_key(request_id: int) -> str:
    return f"provider4_new_flow:{request_id}"


def _provider4_new_get_flow(request_id: int) -> dict:
    try:
        raw = redis_conn.get(_provider4_new_flow_key(request_id))
        if not raw:
            return {}

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")

        return json.loads(raw or "{}")

    except Exception as e:
        print("PROVIDER4_NEW_FLOW_GET_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)
        return {}


def _provider4_new_set_flow(request_id: int, data: dict):
    try:
        redis_conn.setex(
            _provider4_new_flow_key(request_id),
            PROVIDER4_NEW_FLOW_TTL_SEC,
            json.dumps(data or {}, ensure_ascii=False),
        )

    except Exception as e:
        print("PROVIDER4_NEW_FLOW_SET_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)


def _provider4_new_clear_flow(request_id: int):
    try:
        redis_conn.delete(_provider4_new_flow_key(request_id))
    except Exception as e:
        print("PROVIDER4_NEW_FLOW_CLEAR_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)


def _provider_is_enabled(db, provider_name: str | None) -> bool:
    p = (provider_name or "").strip().upper()
    if not p:
        return False

    row = (
        db.query(ProviderSetting)
        .filter(ProviderSetting.provider_name == p)
        .first()
    )

    return bool(row and row.is_enabled)


def _provider4_adaptive_delay(request_id: int, requested_delay: int) -> tuple[int, str, int]:
    """
    Ajusta reintentos de Lazaro/Provider4/10/11:
    - PDF_EXISTENTE: inmediato.
    - EN_PROCESO: primeros checks más rápidos.
    - false: un poco más rápido que 30, pero sin saturar.
    """
    reason = ""
    attempts = 0

    try:
        _db_fast = SessionLocal()
        try:
            _req_fast = (
                _db_fast.query(RequestLog)
                .filter(RequestLog.id == request_id)
                .first()
            )
            if _req_fast is not None:
                reason = (getattr(_req_fast, "error_message", "") or "").upper()
        finally:
            _db_fast.close()
    except Exception as e:
        print("PROVIDER4_ADAPTIVE_DELAY_DB_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)

    try:
        counter_key = f"provider4:adaptive_attempts:{request_id}"
        attempts = int(redis_conn.incr(counter_key) or 1)
        redis_conn.expire(counter_key, 60 * 60)
    except Exception as e:
        print("PROVIDER4_ADAPTIVE_DELAY_REDIS_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)
        attempts = 1

    new_delay = requested_delay

    if "PDF_EXISTENTE" in reason:
        new_delay = 0

    elif "EN_PROCESO" in reason:
        if attempts <= 1:
            new_delay = 10
        elif attempts == 2:
            new_delay = 15
        elif attempts == 3:
            new_delay = 20
        else:
            new_delay = 30

    elif "FALSE" in reason or reason.endswith(":FALSE"):
        new_delay = 25

    else:
        new_delay = requested_delay

    print("PROVIDER4_ADAPTIVE_DELAY_DECISION =", {
        "request_id": request_id,
        "reason": reason[:180],
        "attempts": attempts,
        "requested_delay": requested_delay,
        "new_delay": new_delay,
    }, flush=True)

    return new_delay, reason, attempts


def _enqueue_provider4_new_check(request_id: int, delay_sec: int = PROVIDER4_NEW_CHECK_DELAY_SEC):
    """
    Reprograma revisión de Lázaro.
    EN_PROCESO y false usan delay adaptativo.
    PDF_EXISTENTE entra inmediato.
    """
    try:
        old_delay_sec = delay_sec

        try:
            delay_sec, _fast_reason, _adaptive_attempts = _provider4_adaptive_delay(
                request_id,
                delay_sec,
            )
        except Exception as e:
            print("PROVIDER4_ADAPTIVE_DELAY_ERROR =", {
                "request_id": request_id,
                "old_delay_sec": old_delay_sec,
                "error": str(e),
            }, flush=True)

        if delay_sec <= 0:
            job = slow_request_queue.enqueue(
                process_request,
                request_id,
                at_front=True,
            )

            print("PROVIDER4_NEW_CHECK_ENQUEUED_IMMEDIATE =", {
                "request_id": request_id,
                "old_delay_sec": old_delay_sec,
                "delay_sec": delay_sec,
                "job_id": job.id,
            }, flush=True)

            return job

        job = slow_request_queue.enqueue_in(
            timedelta(seconds=delay_sec),
            process_request,
            request_id,
        )

        print("PROVIDER4_NEW_CHECK_ENQUEUED_IN =", {
            "request_id": request_id,
            "old_delay_sec": old_delay_sec,
            "delay_sec": delay_sec,
            "job_id": job.id,
        }, flush=True)

        return job

    except Exception as e:
        print("PROVIDER4_NEW_ENQUEUE_IN_FAILED_NORMAL_FALLBACK =", {
            "request_id": request_id,
            "delay_sec": delay_sec,
            "error": str(e),
        }, flush=True)

        job = slow_request_queue.enqueue(process_request, request_id)

        print("PROVIDER4_NEW_CHECK_ENQUEUED_NORMAL =", {
            "request_id": request_id,
            "job_id": job.id,
        }, flush=True)

        return job


BOT_PROVIDER_MODE_KEY_PREFIX = "BOT_PROVIDER_MODE:"
DEFAULT_BOT_PROVIDER_MODE = {
    "docifybot8maya": "GLOBAL_POOL",
}


BOT_HIDDEN_NO_ACCOUNTING_GROUPS = {
    "docifybot8mx": {
        "120363407565721999@g.us",
        "120363408048979577@g.us",
        "120363424360403186@g.us",
    },
}


def _bot_hidden_no_accounting_group_ids(instance_name: str | None) -> set[str]:
    inst = _norm_instance(instance_name)

    return {
        str(group_jid).strip()
        for group_jid in BOT_HIDDEN_NO_ACCOUNTING_GROUPS.get(inst, set())
        if str(group_jid).strip()
    }


def _is_bot_hidden_no_accounting_group(
    instance_name: str | None,
    group_jid: str | None,
) -> bool:
    gid = (group_jid or "").strip()

    if not gid:
        return False

    return gid in _bot_hidden_no_accounting_group_ids(instance_name)


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
        "PROVIDER12",
        "PROVIDER13",
        "PROVIDER14",
        "MAYAPROVIDER",
    }:
        return provider_name

    return None


def _maya_provider_group_ids() -> set[str]:
    return {
        g.strip()
        for g in [
            getattr(settings, "MAYAPROVIDER_GROUP_1", ""),
            getattr(settings, "MAYAPROVIDER_GROUP_2", ""),
        ]
        if (g or "").strip()
    }


def _request_is_no_accounting(req, db) -> bool:
    instance_name = _norm_instance(getattr(req, "instance_name", None))
    provider_name = (getattr(req, "provider_name", "") or "").strip().upper()
    provider_group_id = (getattr(req, "provider_group_id", "") or "").strip()

    # Estos grupos continúan procesándose, pero no consumen
    # límite, usadas ni promociones.
    if _is_bot_hidden_no_accounting_group(
        instance_name,
        getattr(req, "source_group_id", None),
    ):
        return True

    # MAYAPROVIDER jamás consume límite ni promociones.
    if provider_name == "MAYAPROVIDER":
        return True

    # Blindaje por grupo privado, aunque provider_name venga vacío o mal.
    if instance_name == "docifybot8maya" and provider_group_id in _maya_provider_group_ids():
        return True

    mode = _bot_provider_mode(db, getattr(req, "instance_name", None))
    mode_provider = _provider_from_mode(mode)

    return (
        _is_personal_provider_mode(mode)
        and mode_provider
        and provider_name == mode_provider
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


def _no_record_client_msg(req) -> str:
    return (
        "❌ No hay registros disponibles.\n"
        f"Dato: {getattr(req, 'curp', '')}\n"
        f"Tipo: {getattr(req, 'act_type', '')}\n\n"
        "Verificar que la CURP esté certificada en RENAPO"
    )


def _notify_client_no_record_once(req, label: str = "NO_RECORD") -> bool:
    """
    Blindaje global:
    una solicitud solo puede avisar 'No hay registros disponibles' una vez.
    Usa la misma llave que main.py: no_record_notified:{req.id}
    """
    if _is_api_request(req):
        print(f"{label}_API_SKIP_WHATSAPP_NOTIFY =", {
            "request_id": getattr(req, "id", None),
            "api_client_id": getattr(req, "api_client_id", None),
        }, flush=True)
        return False
        
    req_id = getattr(req, "id", None)
    dedupe_key = f"no_record_notified:{req_id}"

    first_notify = True

    if req_id:
        try:
            first_notify = redis_conn.set(dedupe_key, "1", nx=True, ex=86400)
        except Exception as dedupe_exc:
            print(f"{label}_DEDUPE_REDIS_ERROR =", str(dedupe_exc), flush=True)
            first_notify = True

    if not first_notify:
        print(f"{label}_DUPLICATE_IGNORED =", dedupe_key, flush=True)
        return False

    msg = _no_record_client_msg(req)
    instance = getattr(req, "instance_name", None) or settings.EVOLUTION_INSTANCE or "docifybot8"

    try:
        if getattr(req, "source_group_id", None):
            send_group_text(req.source_group_id, msg, instance)
        elif getattr(req, "requester_wa_id", None):
            send_text(req.requester_wa_id, msg, instance_name=instance)

        print(f"{label}_CLIENT_NOTIFIED_ONCE =", dedupe_key, flush=True)
        return True

    except Exception as send_exc:
        print(f"{label}_CLIENT_NOTIFY_ERROR =", {
            "request_id": req_id,
            "error": str(send_exc),
        }, flush=True)
        return False
    

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

# DISABLED: no iniciar refresh_providers_status en cada worker
# threading.Thread(target=providers_status_loop, daemon=True).start()


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
    "PROVIDER12": "VILLAFUERTE",
    "PROVIDER13": "RL",
    "PROVIDER14": "EMILIANO BOT",
    "MAYAPROVIDER": "PROVEEDOR DE MAYA",
}


SUPPORT_ERROR_LABELS_ES = {
    # Errores generales de selección/configuración
    "NO_PROVIDER_ENABLED": "No hay proveedores activos disponibles para procesar la solicitud.",
    "NO_PROVIDER_FOR_SPECIAL_FORMAT": "No hay proveedor disponible para este tipo/formato de solicitud.",
    "UNKNOWN_PROVIDER": "Proveedor desconocido o no configurado.",
    "NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED": "No hay grupo configurado para actas foliadas.",
    "NO_CADENA_PROVIDER_GROUP_CONFIGURED": "No hay grupo configurado para actas por cadena.",
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
    "WRONG_CURP_IN_PDF_PENDING_RETRY": (
        "El PDF recibido no se entregó al cliente porque no se pudo confirmar que corresponda al dato solicitado. "
        "Si el PDF trae una CURP interna diferente, se espera otro PDF correcto. "
        "Si no se pudo leer la CURP interna, revisar manualmente o esperar reenvío."
    ),
    "PROVIDER8_POSTPROCESS_ERROR": "Error al procesar el PDF recibido del proveedor ANGEL.",
    "SHARED_GROUP_LIMIT_REACHED": "El grupo alcanzó su límite individual de actas.",

    # Provider 3
    "PROVIDER3_NO_PDF": "AUSTRAM WEB no devolvió un PDF válido.",
    "PROVIDER3_PDF_SEND_FAILED": "No se pudo enviar el PDF generado por AUSTRAM WEB.",

    # Provider 7 / otros
    "PROVIDER7_ERROR": "Error al procesar la solicitud con MESINO SID.",
    "DELIVERY_FAILED": "No se pudo entregar el PDF al cliente por WhatsApp.",

    # Provider 4 / 10 / 11
    "EMPTY_OR_USELESS_HTML": (
        "Lázaro Web respondió vacío o con HTML inútil. "
        "La solicitud debe reintentarse automáticamente con otro proveedor; "
        "no significa que el acta quedó perdida definitivamente."
    ),
    "PROVIDER4_NEW_TIMEOUT_WAITING_PDF": (
        "LAZARO WEB 1 no entregó el PDF dentro del tiempo máximo. "
        "La solicitud debe reenviarse automáticamente a un proveedor WhatsApp disponible antes de avisar fallo al cliente."
    ),
    "PROVIDER10_NEW_TIMEOUT_WAITING_PDF": (
        "LAZARO WEB 2 no entregó el PDF dentro del tiempo máximo. "
        "La solicitud debe reenviarse automáticamente a un proveedor WhatsApp disponible antes de avisar fallo al cliente."
    ),
    "PROVIDER11_NEW_TIMEOUT_WAITING_PDF": (
        "LAZARO WEB 3 no entregó el PDF dentro del tiempo máximo. "
        "La solicitud debe reenviarse automáticamente a un proveedor WhatsApp disponible antes de avisar fallo al cliente."
    ),
    "PROVIDER4_EMPTY_OR_USELESS_HTML": (
        "LAZARO WEB 1 respondió vacío o con HTML inútil. "
        "La solicitud debe reintentarse automáticamente con otro proveedor; "
        "no significa que el acta quedó perdida definitivamente."
    ),
    "PROVIDER10_EMPTY_OR_USELESS_HTML": (
        "LAZARO WEB 2 respondió vacío o con HTML inútil. "
        "La solicitud debe reintentarse automáticamente con otro proveedor; "
        "no significa que el acta quedó perdida definitivamente."
    ),
    "PROVIDER11_EMPTY_OR_USELESS_HTML": (
        "LAZARO WEB 3 respondió vacío o con HTML inútil. "
        "La solicitud debe reintentarse automáticamente con otro proveedor; "
        "no significa que el acta quedó perdida definitivamente."
    ),
    "PROVIDER5_NACIMIENTO_GROUP_NOT_CONFIGURED": "No hay grupo de nacimiento configurado para LUIS SID.",
    "PROVIDER5_ESPECIALES_GROUP_NOT_CONFIGURED": "No hay grupo de especiales configurado para LUIS SID.",
    "PROVIDER12_GROUPS_NOT_CONFIGURED": "No hay grupos configurados para VILLAFUERTE.",
    "PROVIDER12_NACIMIENTO_GROUP_NOT_CONFIGURED": "No hay grupo de nacimiento configurado para VILLAFUERTE.",
    "PROVIDER12_ESPECIALES_GROUP_NOT_CONFIGURED": "No hay grupo de especiales configurado para VILLAFUERTE.",

    "PROVIDER13_NACIMIENTO_GROUP_NOT_CONFIGURED": (
        "No hay grupos de nacimiento configurados para RL."
    ),
    "PROVIDER13_FOLIO_GROUP_NOT_CONFIGURED": (
        "No hay grupo de foliadas configurado para RL."
    ),
    "PROVIDER13_CADENA_GROUP_NOT_CONFIGURED": (
        "No hay grupo de cadena configurado para RL."
    ),
    "PROVIDER13_ESPECIALES_GROUP_NOT_CONFIGURED": (
        "No hay grupo de especiales configurado para RL."
    ),
}


def _support_provider_label(provider_name: str | None) -> str:
    p = (provider_name or "").strip().upper()
    return PROVIDER_LABELS_SUPPORT.get(p, p or "N/D")


def _support_provider_from_error(err: str | None) -> str:
    text = (err or "").strip().upper()

    # Detecta MAYAPROVIDER aunque no venga guardado todavía en req.provider_name.
    if "MAYAPROVIDER" in text:
        return "MAYAPROVIDER"

    # Detecta:
    # PROVIDER1
    # PROVIDER1_SEND_FAILED
    # PROVIDER_1_SEND_FAILED
    # PROVIDER10_DOWNLOAD_FAILED
    m = re.search(r"\bPROVIDER_?(10|11|12|13|[1-9])(?=\b|_)", text)
    if m:
        return f"PROVIDER{m.group(1)}"

    return ""


def _should_skip_support_error(req, err: str | None) -> bool:
    provider_name = (getattr(req, "provider_name", "") or "").strip().upper()
    err_text = (err or "").strip().upper()

    # No enviar NADA de MAYAPROVIDER al grupo de soporte.
    # Cubre:
    # - req.provider_name = MAYAPROVIDER
    # - err = MAYAPROVIDER_GROUPS_NOT_CONFIGURED
    # - err = MAYAPROVIDER_SEND_FAILED
    if provider_name == "MAYAPROVIDER":
        return True

    if "MAYAPROVIDER" in err_text:
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
    code_up = re.sub(r"^PROVIDER(?:10|11|12|13|[1-9])_", "", code_up)

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

    raw_up = raw.upper()

    # Fallas de entrega por WhatsApp / Evolution.
    # Ejemplo: sendMedia/docifybot8max -> Error: Connection Closed
    # Esto NO significa que el proveedor no haya generado el PDF.
    if (
        "SENDMEDIA" in raw_up
        or "/MESSAGE/SENDMEDIA/" in raw_up
        or "MESSAGE/SENDMEDIA" in raw_up
    ) and (
        "CONNECTION CLOSED" in raw_up
        or "500 SERVER ERROR" in raw_up
        or "INTERNAL SERVER ERROR" in raw_up
    ):
        return (
            "El PDF sí se generó, pero falló la entrega por WhatsApp/Evolution. "
            "La conexión de la instancia se cerró al intentar enviar el archivo. "
            "No es un PDF perdido del proveedor; revisar/reconectar la instancia del bot "
            "y reenviar la solicitud o entregar el PDF manualmente si quedó disponible."
        )

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

    if code_clean == "SEND_FAILED" and "CONNECTION CLOSED" in raw_up:
        return (
            f"No se pudo enviar la solicitud al proveedor {provider_label or 'seleccionado'} "
            "porque la conexión de WhatsApp/Evolution se cerró al mandar el mensaje. "
            "Es un error temporal de la instancia, no necesariamente del proveedor."
        )

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
        msg = (
            f"Falló el proveedor inicial, pero la solicitud sigue EN PROCESO "
            f"y ya fue reenviada al proveedor de respaldo: {fallback_label}."
        )
        if detail:
            msg += f"\nDetalle técnico del proveedor inicial: {detail}"
        return msg

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

        # También mandar copia al grupo del proveedor que está atendiendo la solicitud.
        # Ejemplo: si el proveedor es ADMIN DIGITAL, también le cae a su grupo.
        provider_group = (getattr(req, "provider_group_id", "") or "").strip()

        if provider_group and provider_group != support_group:
            try:
                provider_msg = msg.replace(
                    "🚨 *ERROR SOPORTE ACTAS*",
                    "⚠️ *AVISO SOPORTE ACTAS / PROVEEDOR*"
                )

                provider_sender_instance = _provider_sender_instance(provider_name, req)

                print("SOPORTE_PROVIDER_COPY =", {
                    "req_id": getattr(req, "id", None),
                    "provider_name": provider_name,
                    "provider_group": provider_group,
                    "sender_instance": provider_sender_instance,
                }, flush=True)

                send_group_text(provider_group, provider_msg, provider_sender_instance)

            except Exception as provider_support_exc:
                print("PROVIDER_SUPPORT_NOTIFY_FAILED =", {
                    "req_id": getattr(req, "id", None),
                    "provider_group": provider_group,
                    "error": str(provider_support_exc),
                }, flush=True)

    except Exception as support_exc:
        print("SUPPORT_ERROR_NOTIFY_FAILED =", str(support_exc), flush=True)


def _is_curp_term(value: str | None) -> bool:
    v = (value or "").strip().upper()
    return bool(CURP_RE.match(v))


def _is_provider4_eligible(term: str | None, act_type: str | None) -> bool:
    """
    Lázaro Web 1 / 2 / 3 acepta:
    - CURP válida.
    - Cadena / identificador electrónico numérico.

    Para cadena, el flujo nuevo manda exactamente el mismo parámetro
    HTTP llamado "curp", pero su valor real es la cadena.
    """
    term_clean = (term or "").strip().upper()
    act_type_up = (act_type or "").upper().strip()

    chain_mode = (
        is_chain(term_clean)
        or bool(re.fullmatch(r"\d{15,25}", term_clean))
    )

    if chain_mode:
        print("LAZARO_CHAIN_ELIGIBLE =", {
            "term": term_clean,
            "act_type": act_type_up,
        }, flush=True)
        return True

    curp_ok = _is_curp_term(term_clean)

    print("LAZARO_CURP_ELIGIBILITY =", {
        "term": term_clean,
        "act_type": act_type_up,
        "eligible": curp_ok,
    }, flush=True)

    return curp_ok


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

    pdf_bytes = _require_pdf_bytes(provider3_result, "PROVIDER3", req)
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

    delivered = _deliver_pdf_base64_with_retries(
        req,
        db,
        safe_media_b64,
        filename,
        caption_text,
        instance,
        label="PROVIDER3_FALLBACK",
    )
    
    if not delivered:
        return

    req.provider_media_url = "BASE64_PROVIDER3"
    #req.pdf_url = None
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

    request_id = getattr(req, "id", None)

    if not request_id:
        print("API_CHARGE_SKIP_NO_REQUEST_ID =", flush=True)
        return

    try:
        # ============================================================
        # 1) BLOQUEAR Y RECARGAR LA SOLICITUD REAL DESDE POSTGRES
        # ============================================================
        # No confiar en el objeto req recibido porque puede estar viejo
        # si dos workers/procesos llegaron casi al mismo tiempo.
        locked_req = (
            db.query(RequestLog)
            .populate_existing()
            .filter(RequestLog.id == request_id)
            .with_for_update()
            .first()
        )

        if not locked_req:
            print("API_CHARGE_REQUEST_NOT_FOUND =", request_id, flush=True)
            return

        if not _is_api_request(locked_req):
            return

        if (locked_req.status or "").upper() != "DONE":
            print("API_CHARGE_SKIP_NOT_DONE =", {
                "request_id": locked_req.id,
                "status": locked_req.status,
            }, flush=True)
            return

        # Ya dentro del lock de RequestLog.
        # Si un worker anterior ya cobró, este sale sin volver a tocar saldo.
        if bool(locked_req.api_charged):
            print("API_CHARGE_ALREADY_DONE =", locked_req.id, flush=True)
            return

        # ============================================================
        # 2) BLOQUEAR EL CLIENTE ANTES DE MOVER EL SALDO
        # ============================================================
        client = (
            db.query(ApiClient)
            .populate_existing()
            .filter(ApiClient.id == locked_req.api_client_id)
            .with_for_update()
            .first()
        )

        if not client:
            print("API_CHARGE_CLIENT_NOT_FOUND =", {
                "request_id": locked_req.id,
                "api_client_id": locked_req.api_client_id,
            }, flush=True)
            return

        price = Decimal(
            str(locked_req.api_price or client.price_per_done or 5)
        )

        client.credit_balance = (
            Decimal(str(client.credit_balance or 0)) - price
        )
        client.updated_at = _utc_now_naive()

        # Marcamos cobrada la MISMA fila bloqueada.
        locked_req.api_charged = True
        locked_req.api_price = price
        locked_req.updated_at = _utc_now_naive()

        db.add(ApiCreditLog(
            api_client_id=client.id,
            request_log_id=locked_req.id,
            amount=-price,
            type="CHARGE",
            note=f"Acta DONE request_id={locked_req.id}",
            created_at=_utc_now_naive(),
        ))

        db.commit()

        print("API_CHARGED_DONE =", {
            "req_id": locked_req.id,
            "api_client_id": client.id,
            "amount": str(price),
            "balance": str(client.credit_balance),
        }, flush=True)

    except Exception as e:
        db.rollback()

        print("API_CHARGE_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)

        raise


def _store_api_pdf_result(req, db, safe_media_b64: str, filename: str, provider_media_label: str):
    if not _is_api_request(req):
        return False

    raw = (safe_media_b64 or "").strip()
    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]
    raw = raw.replace("\n", "").replace("\r", "").strip()

    try:
        pdf_bytes = base64.b64decode(raw)
        save_request_pdf_to_r2(
            req,
            db,
            pdf_bytes,
            filename=filename or f"{req.curp}.pdf",
            origin=f"api:{provider_media_label}",
        )
    except Exception as r2_exc:
        print("R2_SAVE_API_PDF_ERROR =", {
            "req_id": getattr(req, "id", None),
            "filename": filename,
            "provider_media_label": provider_media_label,
            "error": str(r2_exc),
        }, flush=True)

    req.api_result_base64 = raw
    req.api_result_filename = filename or f"{req.curp}.pdf"
    req.provider_media_url = provider_media_label
    #req.pdf_url = None
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


def _default_pdf_filename(req) -> str:
    curp = (getattr(req, "curp", "") or getattr(req, "id", "") or "acta")
    act_type = (getattr(req, "act_type", "") or "").upper()

    if "FOLIO" in act_type:
        return f"{curp}_FOLIO.pdf"

    return f"{curp}.pdf"


def _require_pdf_bytes(result, provider_name: str, req) -> bytes:
    provider = (provider_name or getattr(req, "provider_name", "") or "PROVIDER").strip().upper()

    if not isinstance(result, dict):
        raise RuntimeError(f"{provider}_INVALID_RESULT_NO_DICT")

    pdf_bytes = result.get("pdf_bytes")

    if not pdf_bytes:
        err = result.get("error") or result.get("message") or result.get("status") or ""
        err_up = str(err).upper()

        if (
            "NO_LOCALIZADO" in err_up
            or "NO REGISTRO" in err_up
            or "NO_RECORD" in err_up
            or "SIN REGISTRO" in err_up
        ):
            raise RuntimeError(f"{provider}_NO_RECORD:{getattr(req, 'curp', '')}")

        raise RuntimeError(f"{provider}_NO_PDF_BYTES:{str(result)[:300]}")

    if isinstance(pdf_bytes, str):
        raw = pdf_bytes.strip()
        if raw.startswith("data:"):
            raw = raw.split(",", 1)[1]
        raw = raw.replace("\n", "").replace("\r", "").strip()
        try:
            pdf_bytes = base64.b64decode(raw)
        except Exception as e:
            raise RuntimeError(f"{provider}_PDF_BYTES_BASE64_INVALID:{str(e)[:200]}")

    if not isinstance(pdf_bytes, (bytes, bytearray)):
        raise RuntimeError(f"{provider}_PDF_BYTES_INVALID_TYPE:{type(pdf_bytes).__name__}")

    pdf_bytes = bytes(pdf_bytes)

    if b"%PDF" not in pdf_bytes[:30]:
        raise RuntimeError(f"{provider}_PDF_BYTES_NOT_PDF")

    return pdf_bytes


def _send_pdf_base64_to_client_once(req, safe_media_b64: str, filename: str, caption_text: str, instance: str):
    if req.source_group_id:
        return send_group_document_base64(
            req.source_group_id,
            safe_media_b64,
            filename=filename,
            caption=caption_text,
            instance_name=instance,
        )

    return send_document_base64(
        req.requester_wa_id,
        safe_media_b64,
        filename=filename,
        caption=caption_text,
        instance_name=instance,
    )


def _schedule_delivery_retry(request_id: int, attempt: int = 1, delay_sec: int = 30):
    try:
        request_queue.enqueue_in(
            timedelta(seconds=delay_sec),
            retry_pdf_delivery,
            request_id,
            attempt,
        )

        print("PDF_DELIVERY_RETRY_SCHEDULED =", {
            "request_id": request_id,
            "attempt": attempt,
            "delay_sec": delay_sec,
        }, flush=True)

    except Exception as e:
        print("PDF_DELIVERY_RETRY_SCHEDULE_ERROR =", {
            "request_id": request_id,
            "attempt": attempt,
            "error": str(e),
        }, flush=True)


def _deliver_pdf_base64_with_retries(
    req,
    db,
    safe_media_b64: str,
    filename: str,
    caption_text: str,
    instance: str,
    *,
    label: str,
) -> bool:
    last_error = None

    for attempt in range(1, 3):
        try:
            _send_pdf_base64_to_client_once(
                req,
                safe_media_b64,
                filename,
                caption_text,
                instance,
            )

            print(f"{label}_DELIVERY_OK_ATTEMPT_{attempt} =", req.id, flush=True)
            return True

        except Exception as e:
            last_error = e
            print(f"{label}_DELIVERY_ERROR_ATTEMPT_{attempt} =", str(e), flush=True)

            if attempt < 3:
                time.sleep(3 * attempt)

    # Si llegó aquí, falló la entrega. No perder PDF.
    req.status = "ERROR"
    req.error_message = f"DELIVERY_FAILED_PENDING_RETRY: {str(last_error)[:300]}"
    req.updated_at = _utc_now_naive()
    db.commit()

    if getattr(req, "pdf_url", None):
        _schedule_delivery_retry(req.id, attempt=1, delay_sec=30)
    else:
        print("PDF_DELIVERY_FAILED_NO_R2_URL =", {
            "request_id": req.id,
            "curp": req.curp,
            "provider": req.provider_name,
        }, flush=True)

    try:
        _notify_support_error(
            req,
            "DELIVERY_FAILED_PENDING_RETRY",
            f"PDF generado, pero falló entrega WhatsApp. Se programó reintento automático. error={str(last_error)[:500]}"
        )
    except Exception as support_exc:
        print("DELIVERY_FAILED_PENDING_RETRY_SUPPORT_ERROR =", str(support_exc), flush=True)

    return False


def retry_pdf_delivery(request_id: int, attempt: int = 1):
    db = SessionLocal()

    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()

        if not req:
            print("RETRY_PDF_DELIVERY_REQUEST_NOT_FOUND =", request_id, flush=True)
            return

        if req.status == "DONE":
            print("RETRY_PDF_DELIVERY_ALREADY_DONE =", request_id, flush=True)
            return

        if not req.pdf_url:
            print("RETRY_PDF_DELIVERY_NO_PDF_URL =", {
                "request_id": request_id,
                "status": req.status,
                "error_message": req.error_message,
            }, flush=True)
            return

        instance = req.instance_name or settings.EVOLUTION_INSTANCE or "docifybot8"
        filename = _default_pdf_filename(req)

        url = generate_r2_presigned_download_url(req.pdf_url)
        r = requests.get(url, timeout=(5, 45))
        r.raise_for_status()

        pdf_bytes = r.content

        if b"%PDF" not in pdf_bytes[:30]:
            raise RuntimeError("RETRY_PDF_DELIVERY_NOT_PDF")

        safe_media_b64 = base64.b64encode(pdf_bytes).decode()

        caption_text = "📄 Reenvío automático de acta generada previamente."

        _send_pdf_base64_to_client_once(
            req,
            safe_media_b64,
            filename,
            caption_text,
            instance,
        )

        req.status = "DONE"
        req.error_message = None
        req.updated_at = _utc_now_naive()
        db.commit()

        try:
            _after_done_accounting(req, db)
        except Exception as accounting_exc:
            print("RETRY_PDF_DELIVERY_ACCOUNTING_ERROR =", str(accounting_exc), flush=True)

        print("RETRY_PDF_DELIVERY_OK =", {
            "request_id": request_id,
            "attempt": attempt,
            "instance": instance,
            "source_group_id": req.source_group_id,
        }, flush=True)

    except Exception as e:
        print("RETRY_PDF_DELIVERY_ERROR =", {
            "request_id": request_id,
            "attempt": attempt,
            "error": str(e),
        }, flush=True)

        try:
            req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
            if req:
                req.status = "ERROR"
                req.error_message = f"DELIVERY_FAILED_RETRY_{attempt}: {str(e)[:300]}"
                req.updated_at = _utc_now_naive()
                db.commit()
        except Exception as db_exc:
            print("RETRY_PDF_DELIVERY_DB_ERROR =", str(db_exc), flush=True)

        if attempt < 5:
            next_delay = [60, 180, 300, 600][min(attempt - 1, 3)]
            _schedule_delivery_retry(request_id, attempt=attempt + 1, delay_sec=next_delay)

    finally:
        db.close()


def sweep_stuck_requests(max_age_minutes: int = 20, limit: int = 80):
    """
    Limpieza automática de solicitudes estancadas.

    - WhatsApp normal:
      conserva el comportamiento existente:
      QUEUED se reencola y PROCESSING viejo se cierra.

    - API externa:
      nunca se reencola automáticamente después del timeout.
      Se marca ERROR y libera la reserva de saldo.
    """
    db = SessionLocal()

    try:
        now = _utc_now_naive()

        normal_cutoff = now - timedelta(minutes=max_age_minutes)
        api_cutoff = now - timedelta(minutes=API_STALE_TIMEOUT_MINUTES)

        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                or_(
                    and_(
                        RequestLog.api_client_id.is_(None),
                        RequestLog.updated_at < normal_cutoff,
                    ),
                    and_(
                        RequestLog.api_client_id.isnot(None),
                        RequestLog.updated_at < api_cutoff,
                    ),
                ),
            )
            .order_by(RequestLog.updated_at.asc())
            .limit(limit)
            .all()
        )

        print("SWEEP_STUCK_REQUESTS_FOUND =", {
            "count": len(rows),
            "normal_max_age_minutes": max_age_minutes,
            "api_max_age_minutes": API_STALE_TIMEOUT_MINUTES,
        }, flush=True)

        for req in rows:
            try:
                # =====================================================
                # API EXTERNA
                # =====================================================
                if _is_api_request(req):
                    # Si existe lock vivo, hay un worker trabajando justo
                    # ahora. No cerramos una solicitud activa.
                    active_lock = redis_conn.get(
                        f"request_processing_lock:{req.id}"
                    )

                    if active_lock:
                        print("API_STALE_SWEEP_SKIP_ACTIVE_LOCK =", {
                            "request_id": req.id,
                            "api_client_id": req.api_client_id,
                            "status": req.status,
                        }, flush=True)
                        continue

                    # Una API no se cobra hasta DONE. Si está pendiente,
                    # api_charged debe seguir false. No se modifica saldo:
                    # al cambiar a ERROR automáticamente deja de reservar.
                    if bool(req.api_charged):
                        print("API_STALE_SWEEP_SKIP_ALREADY_CHARGED =", {
                            "request_id": req.id,
                            "api_client_id": req.api_client_id,
                            "status": req.status,
                        }, flush=True)
                        continue

                    old_status = (req.status or "").upper()

                    req.status = "ERROR"
                    req.error_message = (
                        "API_STALE_TIMEOUT:"
                        f"sin resultado después de "
                        f"{API_STALE_TIMEOUT_MINUTES} minutos"
                    )
                    req.updated_at = now
                    db.commit()

                    print("API_STALE_TIMEOUT_CLOSED =", {
                        "request_id": req.id,
                        "api_client_id": req.api_client_id,
                        "old_status": old_status,
                        "curp": req.curp,
                        "act_type": req.act_type,
                        "api_external_id": req.api_external_id,
                    }, flush=True)

                    continue

                # =====================================================
                # WHATSAPP / FLUJO NORMAL EXISTENTE
                # =====================================================
                if req.pdf_url:
                    req.status = "ERROR"
                    req.error_message = (
                        "DELIVERY_FAILED_PENDING_RETRY: "
                        "sweep detected stored PDF"
                    )
                    req.updated_at = now
                    db.commit()

                    _schedule_delivery_retry(
                        req.id,
                        attempt=1,
                        delay_sec=10,
                    )
                    continue

                if req.status == "QUEUED":
                    if _worker_stop_if_instance_blocked(
                        req,
                        db,
                        label="SWEEP_BLOCKED_INSTANCE_QUEUED_NOT_REENQUEUED",
                    ):
                        continue
                
                    req.updated_at = now
                    db.commit()
                
                    queue = (
                        slow_request_queue
                        if (req.provider_name or "").upper() in SLOW_PROVIDERS
                        else request_queue
                    )
                
                    queue.enqueue(process_request, req.id)

                    print("SWEEP_REQUEUED_QUEUED_REQUEST =", {
                        "request_id": req.id,
                        "provider": req.provider_name,
                    }, flush=True)
                    continue

                req.status = "ERROR"
                req.error_message = (
                    f"TIMEOUT_8MIN_CANCELLED: auto-cierre por sweep > "
                    f"{max_age_minutes} min sin PDF útil"
                )
                req.updated_at = now
                db.commit()

                print("SWEEP_CLOSED_PROCESSING_TIMEOUT =", {
                    "request_id": req.id,
                    "provider": req.provider_name,
                    "curp": req.curp,
                    "act_type": req.act_type,
                }, flush=True)

            except Exception as one_exc:
                db.rollback()

                print("SWEEP_STUCK_ONE_ERROR =", {
                    "request_id": getattr(req, "id", None),
                    "error": str(one_exc),
                }, flush=True)

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
    p8 = _get_or_create_provider(db, "PROVIDER8", False)
    p9 = _get_or_create_provider(db, "PROVIDER9", False)
    p10 = _get_or_create_provider(db, "PROVIDER10", False)
    p11 = _get_or_create_provider(db, "PROVIDER11", False)
    p12 = _get_or_create_provider(db, "PROVIDER12", False)
    p13 = _get_or_create_provider(db, "PROVIDER13", False)
    p14 = _get_or_create_provider(db, "PROVIDER14", False)
    p_maya = _get_or_create_provider(db, "MAYAPROVIDER", False)

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
        enabled.append("PROVIDER12")
    if p13.is_enabled:
        enabled.append("PROVIDER13")
    if p14.is_enabled:
        enabled.append("PROVIDER14")

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

    # Escalante NO debe recibir estos tipos por CURP,
    # ni aunque vengan como FOLIO.
    return bool(re.search(
        r"\b(MATRIMONIO|MAT|DEFUNCION|DEFUNCIÓN|DEF|DIVORCIO|DIV)\b",
        t
    ))


def _is_provider6_allowed_request(term: str | None, act_type: str | None) -> bool:
    t = (act_type or "").upper().strip()

    # CADENA sí puede entrar a Escalante SIEMPRE.
    # La cadena ya identifica el acta; no dependemos del act_type.
    if is_chain(term):
        return True

    # Para solicitudes por CURP, Escalante NO debe recibir
    # matrimonio, defunción ni divorcio.
    if _is_provider6_blocked_act_type(t):
        return False

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
    
        if forced_provider in ("PROVIDER4", "PROVIDER10", "PROVIDER11") and not _is_provider4_eligible(term, act_type):
            print("BOT_PROVIDER_FORCED_LAZARO_NOT_ALLOWED_FALLBACK_TO_GLOBAL =", {
                "forced_provider": forced_provider,
                "term": term,
                "act_type": act_type,
                "instance_name": instance_name,
            }, flush=True)
        
            enabled = sorted(_enabled_providers(db))
            enabled = [p for p in enabled if p not in ("PROVIDER4", "PROVIDER10", "PROVIDER11")]
        
            if "PROVIDER6" in enabled and not _is_provider6_allowed_request(term, act_type):
                enabled = [p for p in enabled if p != "PROVIDER6"]
        
            if not enabled:
                raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")
        
            print("PICK_PROVIDER_ENABLED_FINAL_AFTER_FORCED_LAZARO_BLOCK =", enabled, flush=True)
        
            weighted_chosen = _pick_provider_by_weight(db, enabled)
        
            if weighted_chosen:
                print("PICK_PROVIDER_WEIGHTED_CHOSEN_AFTER_FORCED_LAZARO_BLOCK =", weighted_chosen, flush=True)
                return weighted_chosen
        
            idx = (request_id - 1) % len(enabled)
            chosen = enabled[idx]
        
            print("PICK_PROVIDER_NORMAL_CHOSEN_AFTER_FORCED_LAZARO_BLOCK =", chosen, flush=True)
            return chosen
    
        if forced_provider == "PROVIDER6" and not _is_provider6_allowed_request(term, act_type):
            print("BOT_PROVIDER_FORCED_PROVIDER6_NOT_ALLOWED_FALLBACK_TO_GLOBAL =", {
                "term": term,
                "act_type": act_type,
                "instance_name": instance_name,
            }, flush=True)
    
            enabled = sorted(_enabled_providers(db))
            enabled = [p for p in enabled if p != "PROVIDER6"]

            if not _is_provider4_eligible(term, act_type):
                enabled = [p for p in enabled if p not in ("PROVIDER4", "PROVIDER10", "PROVIDER11")]
    
            if not enabled:
                raise RuntimeError("NO_PROVIDER_FOR_SPECIAL_FORMAT")
    
            print("PICK_PROVIDER_ENABLED_FINAL_AFTER_FORCED_PROVIDER6_BLOCK =", enabled, flush=True)
    
            weighted_chosen = _pick_provider_by_weight(db, enabled)
            if weighted_chosen:
                print("PICK_PROVIDER_WEIGHTED_CHOSEN_AFTER_FORCED_PROVIDER6_BLOCK =", weighted_chosen, flush=True)
                return weighted_chosen
    
            idx = (request_id - 1) % len(enabled)
            chosen = enabled[idx]
            print("PICK_PROVIDER_NORMAL_CHOSEN_AFTER_FORCED_PROVIDER6_BLOCK =", chosen, flush=True)
            return chosen
    
        return forced_provider

    # GLOBAL_POOL => usa el pool normal del panel principal y SÍ cuenta
    enabled = sorted(_enabled_providers(db))

    print("PICK_PROVIDER_ENABLED_RAW =", enabled, flush=True)
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

    print("PICK_PROVIDER_ENABLED_FINAL =", enabled, flush=True)

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
    nacimiento_group_3 = (settings.PROVIDER_GROUP_NACIMIENTO_3 or "").strip()
    nacimiento_group_4 = (settings.PROVIDER_GROUP_NACIMIENTO_4 or "").strip()

    especiales_group = (settings.PROVIDER_GROUP_ESPECIALES or "").strip()
    foliadas_group = (settings.PROVIDER_GROUP_FOLIADAS or "").strip()
    cadena_group = (settings.PROVIDER_GROUP_CADENA or "").strip()

    is_nacimiento = act_type_up.startswith("NACIMIENTO") or act_type_up.startswith("NAC")
    is_cadena_req = is_chain(term)
    is_folio_req = _is_folio_act(act_type_up)
    is_curp_req = _is_curp_term(term)

    # 1. CADENAS -> grupo exclusivo de cadena.
    # Debe ir antes de FOLIADAS para que una cadena jamás termine
    # en el grupo de foliadas aunque el tipo tenga la palabra FOLIO.
    if is_cadena_req:
        if not cadena_group:
            raise RuntimeError("NO_CADENA_PROVIDER_GROUP_CONFIGURED")
        return cadena_group

    # 2. FOLIADAS -> grupo exclusivo de foliadas.
    if is_folio_req:
        if not foliadas_group:
            raise RuntimeError("NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED")
        return foliadas_group
    # 3. NACIMIENTO POR CURP -> repartir entre grupos nacimiento 1, 2, 3 y 4
    if is_nacimiento and is_curp_req:
        nacimiento_groups = [
            group
            for group in (
                nacimiento_group_1,
                nacimiento_group_2,
                nacimiento_group_3,
                nacimiento_group_4,
            )
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


def _is_birth_request(term: str | None, act_type: str | None) -> bool:
    t = (act_type or "").upper().strip()

    if is_chain(term):
        return False

    if _is_folio_type(t):
        return False

    return t.startswith("NACIMIENTO") or t.startswith("NAC")


def _pick_provider5_group(term: str | None, act_type: str | None, request_id: int) -> str:
    if _is_birth_request(term, act_type):
        group = (settings.PROVIDER5_GROUP_NACIMIENTO or "").strip()
        if not group:
            raise RuntimeError("PROVIDER5_NACIMIENTO_GROUP_NOT_CONFIGURED")
        return group

    group = (settings.PROVIDER5_GROUP_ESPECIALES or "").strip()
    if not group:
        raise RuntimeError("PROVIDER5_ESPECIALES_GROUP_NOT_CONFIGURED")
    return group


def _pick_provider12_group(term: str | None, act_type: str | None, request_id: int) -> str:
    if _is_birth_request(term, act_type):
        group = (settings.PROVIDER12_GROUP_NACIMIENTO or "").strip()
        if not group:
            raise RuntimeError("PROVIDER12_NACIMIENTO_GROUP_NOT_CONFIGURED")
        return group

    group = (settings.PROVIDER12_GROUP_ESPECIALES or "").strip()
    if not group:
        raise RuntimeError("PROVIDER12_ESPECIALES_GROUP_NOT_CONFIGURED")
    return group


def _pick_provider13_group(
    term: str | None,
    act_type: str | None,
    request_id: int,
) -> str:
    act_type_up = (act_type or "").upper().strip()

    nacimiento_group_1 = (
        settings.PROVIDER13_GROUP_NACIMIENTO_1 or ""
    ).strip()

    nacimiento_group_2 = (
        settings.PROVIDER13_GROUP_NACIMIENTO_2 or ""
    ).strip()

    folio_group = (
        settings.PROVIDER13_GROUP_FOLIO or ""
    ).strip()

    cadena_group = (
        settings.PROVIDER13_GROUP_CADENA or ""
    ).strip()

    especiales_group = (
        settings.PROVIDER13_GROUP_ESPECIALES or ""
    ).strip()

    is_cadena_req = is_chain(term)
    is_folio_req = _is_folio_act(act_type_up)
    is_birth_req = _is_birth_request(term, act_type_up)

    # 1. CADENA primero.
    # Una cadena con la palabra "folio" debe seguir yendo a RL cadena.
    if is_cadena_req:
        if not cadena_group:
            raise RuntimeError("PROVIDER13_CADENA_GROUP_NOT_CONFIGURED")
        return cadena_group

    # 2. Solicitudes foliadas.
    if is_folio_req:
        if not folio_group:
            raise RuntimeError("PROVIDER13_FOLIO_GROUP_NOT_CONFIGURED")
        return folio_group

    # 3. Nacimientos por CURP: balance entre los dos grupos.
    if is_birth_req:
        nacimiento_groups = [
            group
            for group in (
                nacimiento_group_1,
                nacimiento_group_2,
            )
            if group
        ]

        if not nacimiento_groups:
            raise RuntimeError(
                "PROVIDER13_NACIMIENTO_GROUP_NOT_CONFIGURED"
            )

        return nacimiento_groups[
            (request_id - 1) % len(nacimiento_groups)
        ]

    # 4. Matrimonio, defunción, divorcio y otros especiales.
    if not especiales_group:
        raise RuntimeError(
            "PROVIDER13_ESPECIALES_GROUP_NOT_CONFIGURED"
        )

    return especiales_group


def _provider14_private_jid() -> str:
    jid = (getattr(settings, "PROVIDER14_PRIVATE_JID", "") or "").strip()

    if not jid:
        raise RuntimeError("PROVIDER14_PRIVATE_JID_NOT_CONFIGURED")

    return jid


def _provider14_prefix_for_act_type(act_type: str | None) -> str:
    act_type_up = (act_type or "").upper().strip()

    if "MATRIMONIO" in act_type_up or "MATRI" in act_type_up:
        return "Mat"

    if "DEFUNCION" in act_type_up or "DEFUN" in act_type_up:
        return "Def"

    if "DIVORCIO" in act_type_up or "DIVOR" in act_type_up:
        return "Div"

    if "NACIMIENTO" in act_type_up or "NAC" in act_type_up:
        return "Nac"

    raise RuntimeError("PROVIDER14_ACT_TYPE_NOT_ALLOWED")


def _provider14_message(term: str | None, act_type: str | None) -> str:
    term_clean = (term or "").strip().upper()

    if not term_clean:
        raise RuntimeError("PROVIDER14_EMPTY_TERM")

    prefix = _provider14_prefix_for_act_type(act_type)
    mode = "foliado" if _is_folio_act(act_type or "") else "reverso"

    return f"{prefix} {mode} {term_clean}"


def _send_provider14_request(req, db):
    provider_jid = _provider14_private_jid()
    sender_instance = _provider_sender_instance("PROVIDER14", req)

    text_to_provider = _provider14_message(req.curp, req.act_type)

    print("PROVIDER14_SEND =", {
        "req_id": req.id,
        "provider_jid": provider_jid,
        "text": text_to_provider,
        "sender_instance": sender_instance,
    }, flush=True)

    resp_json = send_text(
        provider_jid,
        text_to_provider,
        instance_name=sender_instance,
    )

    provider_sent_msg_id = (
        (resp_json or {}).get("key", {}).get("id")
        or (resp_json or {}).get("data", {}).get("key", {}).get("id")
        or (resp_json or {}).get("id")
        or ""
    )

    req.provider_group_id = provider_jid
    req.provider_message = text_to_provider

    if provider_sent_msg_id:
        req.provider_message_id = provider_sent_msg_id

    req.updated_at = _utc_now_naive()
    db.commit()

    print("PROVIDER14_SEND_OK =", {
        "req_id": req.id,
        "provider_jid": provider_jid,
        "provider_sent_msg_id": provider_sent_msg_id,
        "provider_message": req.provider_message,
    }, flush=True)

    return True


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
        return _pick_provider5_group(term, act_type, request_id)

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

    if provider_name == "PROVIDER12":
        return _pick_provider12_group(term, act_type, request_id)

    if provider_name == "PROVIDER13":
        return _pick_provider13_group(term, act_type, request_id)

    if provider_name == "PROVIDER14":
        return _provider14_private_jid()

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
    if provider_name in (
        "PROVIDER1",
        "PROVIDER2",
        "PROVIDER5",
        "PROVIDER6",
        "PROVIDER8",
        "PROVIDER9",
        "PROVIDER12",
        "PROVIDER13",
        "MAYAPROVIDER",
    ):
        provider_type = provider_label_for_type(act_type)

        if is_chain(term):
            act_up = (act_type or "").upper().strip()
            provider_type_up = (provider_type or "").upper().strip()

            if "FOLIO" in act_up or "FOLI" in act_up or "FOLIO" in provider_type_up or "FOLI" in provider_type_up:
                return f"{term} folio"

            return f"{term}"

        return f"{term} {provider_type}"

    if provider_name == "PROVIDER14":
        return _provider14_message(term, act_type)

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
    raw = act_type or ""

    norm = (
        raw.upper()
        .strip()
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ü", "U")
    )

    norm = re.sub(r"[^A-ZÑ0-9\s]", " ", norm)
    norm = re.sub(r"\s+", " ", norm).strip()

    if "MATRIMONIO" in norm or "MATRI" in norm:
        return "matrimonio"

    if "DEFUNCION" in norm or "DEFUN" in norm:
        return "defuncion"

    if "DIVORCIO" in norm or "DIVOR" in norm:
        return "divorcio"

    if "NACIMIENTO" in norm or "NACIM" in norm:
        return "nacimiento"

    raise RuntimeError(f"PROVIDER3_UNKNOWN_ACT_TYPE:{raw}")


def _provider4_tipo_acta(act_type: str) -> str:
    raw = act_type or ""

    norm = (
        raw.upper()
        .strip()
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ü", "U")
    )

    # Limpiar símbolos raros, pero conservar letras y espacios.
    norm = re.sub(r"[^A-ZÑ0-9\s]", " ", norm)
    norm = re.sub(r"\s+", " ", norm).strip()

    if "MATRIMONIO" in norm or "MATRI" in norm:
        return "matrimonio"

    if "DEFUNCION" in norm or "DEFUN" in norm:
        return "defuncion"

    if "DIVORCIO" in norm or "DIVOR" in norm:
        return "divorcio"

    if "NACIMIENTO" in norm or "NACIM" in norm:
        return "nacimiento"

    # MUY IMPORTANTE:
    # Ya no caer a nacimiento silenciosamente.
    raise RuntimeError(f"PROVIDER4_UNKNOWN_ACT_TYPE:{raw}")


def _provider4_tipo_acta_for_request(
    db,
    provider_name: str,
    term: str,
    act_type: str,
) -> str:
    """
    Para CURP usa el tipo real de acta.

    Para cadena, Lázaro acepta la cadena usando el mismo endpoint y campo
    `curp`. El endpoint sigue exigiendo un parámetro `tipo`, aunque la cadena
    identifica el documento.

    El valor se deja configurable por proveedor para no amarrarlo a código.
    Usa nacimiento como valor por defecto porque equivale a tipo=1.
    """
    term_clean = (term or "").strip().upper()

    chain_mode = (
        is_chain(term_clean)
        or bool(re.fullmatch(r"\d{15,25}", term_clean))
    )

    if not chain_mode:
        return _provider4_tipo_acta(act_type)

    provider_name = (provider_name or "PROVIDER4").strip().upper()

    configured = (
        _get_app_setting(
            db,
            f"{provider_name}_CHAIN_TIPOA",
            "nacimiento",
        )
        or "nacimiento"
    ).strip().lower()

    allowed = {
        "nacimiento",
        "matrimonio",
        "defuncion",
        "divorcio",
    }

    if configured not in allowed:
        raise RuntimeError(
            f"{provider_name}_INVALID_CHAIN_TIPOA:{configured}"
        )

    print(f"{provider_name}_CHAIN_TIPOA_USING =", {
        "term": term_clean,
        "configured_tipoa": configured,
        "original_act_type": act_type,
    }, flush=True)

    return configured


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

    if _worker_stop_if_instance_blocked(
        req,
        db,
        label="WORKER_BLOCKED_INSTANCE_PROVIDER4_ENTRY",
    ):
        return {
            "pending": True,
            "reason": "BOT_BLOCKED_BEFORE_PROVIDER_SUBMIT",
        }

    provider_name = (provider_name or "PROVIDER4").strip().upper()

    term = (req.curp or "").strip().upper()

    chain_mode = (
        is_chain(term)
        or bool(re.fullmatch(r"\d{15,25}", term))
    )
    
    print(f"{provider_name}_NEW_PROCESS_TERM =", term, flush=True)
    print(f"{provider_name}_NEW_PROCESS_CHAIN_MODE =", chain_mode, flush=True)
    
    if not term:
        raise RuntimeError(f"{provider_name}_EMPTY_TERM")
    
    # CURP solamente se exige cuando NO es cadena.
    if not chain_mode and not _is_curp_term(term):
        raise RuntimeError(f"{provider_name}_NOT_CURP_OR_CHAIN")

    if provider_name == "PROVIDER4" and PROVIDER4_TEST_GROUPS and req.source_group_id not in PROVIDER4_TEST_GROUPS:
        raise RuntimeError("PROVIDER4_NOT_ALLOWED_GROUP")

    # Si apagaste Provider4/10/11 en panel, no dejes que un job viejo lo use.
    if not _provider_is_enabled(db, provider_name):
        _provider4_new_clear_flow(req.id)
        raise RuntimeError(f"{provider_name}_DISABLED_BEFORE_PROCESSING")

    setting = (
        db.query(ProviderSetting)
        .filter(ProviderSetting.provider_name == f"{provider_name}_HID")
        .first()
    )

    default_hid_map = {
        "PROVIDER4": "D0cuExServ1",
        "PROVIDER10": "D0cuExServ2",
        "PROVIDER11": "D0cuExServ3",
    }

    hid = setting.value if setting and setting.value else default_hid_map.get(provider_name)

    print(f"{provider_name}_NEW_HID_USING =", hid, flush=True)

    client = Provider4Client(hid=hid)

    tipoa = _provider4_tipo_acta_for_request(
        db=db,
        provider_name=provider_name,
        term=term,
        act_type=req.act_type,
    )
    
    # El foliado depende del tipo/comando solicitado.
    # Aplica igual para CURP o para cadena.
    inc_folio = _is_folio_act(req.act_type)
    
    print(
        f"{provider_name}_NEW_FOLIO_REQUEST =",
        {
            "request_id": req.id,
            "term": term,
            "chain_mode": chain_mode,
            "act_type": req.act_type,
            "inc_folio": inc_folio,
        },
        flush=True,
    )

    # User opcional; si luego te da User, lo guardas en app_settings.
    user_key = f"{provider_name}_USER"
    user_value = _get_app_setting(db, user_key, "")

    print(f"{provider_name}_NEW_ACT_TYPE_RAW =", repr(req.act_type), flush=True)
    print(f"{provider_name}_NEW_TIPOA_MAPPED =", tipoa, flush=True)
    print(f"{provider_name}_NEW_INC_FOLIO =", inc_folio, flush=True)
    print(f"{provider_name}_NEW_USER_CONFIGURED =", bool(user_value), flush=True)

    flow = _provider4_new_get_flow(req.id)
    phase = (flow.get("phase") or "").strip().upper()
    attempts = int(flow.get("attempts") or 0)

    print(f"{provider_name}_NEW_FLOW =", {
        "request_id": req.id,
        "phase": phase,
        "attempts": attempts,
    }, flush=True)

    try:
        # ==========================
        # PASO 1: PETICIÓN
        # ==========================
        if phase != "SUBMITTED":
            if _worker_stop_if_instance_blocked(
                req,
                db,
                label="WORKER_BLOCKED_INSTANCE_BEFORE_LAZARO_SUBMIT",
            ):
                return {
                    "pending": True,
                    "reason": "BOT_BLOCKED_BEFORE_PROVIDER_SUBMIT",
                }
        
            submit_result = client.submit_peticion_new_api(
                curp=term,
                tipoa=tipoa,
                inc_folio=inc_folio,
                user=user_value,
                is_chain=chain_mode,
            )

            flow = {
                "phase": "SUBMITTED",
                "provider_name": provider_name,
                "term": term,
                "tipoa": tipoa,
                "inc_folio": bool(inc_folio),
                "is_chain": bool(chain_mode),
                "attempts": 0,
                "submitted_code": submit_result.get("code"),
                "submitted_at": _utc_now_naive().isoformat(),
            }

            _provider4_new_set_flow(req.id, flow)

            req.status = "PROCESSING"
            req.error_message = f"{provider_name}_NEW_SUBMITTED:{submit_result.get('code')}"
            req.updated_at = _utc_now_naive()
            db.commit()

            _enqueue_provider4_new_check(req.id, PROVIDER4_NEW_CHECK_DELAY_SEC)

            return {
                "pending": True,
                "reason": req.error_message,
            }

        # ==========================
        # PASO 2: CONSULTA PDF
        # ==========================
        if _worker_stop_if_instance_blocked(
            req,
            db,
            label="WORKER_BLOCKED_INSTANCE_BEFORE_LAZARO_VERIFY",
        ):
            return {
                "pending": True,
                "reason": "BOT_BLOCKED_BEFORE_PROVIDER_VERIFY",
            }
        
        attempts += 1

        if attempts > PROVIDER4_NEW_MAX_CHECK_ATTEMPTS:
            _provider4_new_clear_flow(req.id)
            raise RuntimeError(f"{provider_name}_NEW_TIMEOUT_WAITING_PDF:{term}")

        try:
            if chain_mode:
                check_result = client.verificar_cadena_por_historial_new_api(
                    cadena=term,
                    tipoa=tipoa,
                    inc_folio=inc_folio,
                )
            else:
                check_result = client.verificar_pdf_new_api(
                    curp=term,
                    tipoa=tipoa,
                )
        except Exception as e:
            # NO_LOCALIZADO_VERIFICAR_CALL_SAFETY_OK:
            # Seguridad directa en la consulta PDF: si la API responde NO_LOCALIZADO
            # pero alguna capa lo lanza como UNKNOWN_RESPONSE, convertirlo en SIN REGISTRO.
            _err_txt = str(e or "").strip()
            _err_up = _err_txt.upper().replace(" ", "_")

            if (
                "NEW_VERIFICAR_UNKNOWN_RESPONSE" in _err_up
                and (
                    "NO_LOCALIZADO" in _err_up
                    or "NO_REGISTRO" in _err_up
                    or "SIN_REGISTRO" in _err_up
                )
            ):
                _provider4_new_clear_flow(req.id)

                print(f"{provider_name}_NO_LOCALIZADO_VERIFICAR_CALL_SAFETY_DETECTED =", {
                    "request_id": req.id,
                    "term": term,
                    "error": _err_txt,
                }, flush=True)

                raise RuntimeError(f"{provider_name}_NO_RECORD:{term}")

            raise

        # NO_LOCALIZADO_READY_WITHOUT_PDF_SAFETY_OK:
        # Si por alguna razón el parser marca ready=True pero no trae bytes de PDF,
        # no debe caer como KeyError ni como error de sistema. Primero revisamos si realmente era NO_LOCALIZADO.
        if check_result.get("ready") and not (
            check_result.get("pdf_bytes")
            or check_result.get("pdf bytes")
            or check_result.get("pdf")
        ):
            web_code_raw = str(
                check_result.get("code")
                or check_result.get("reason")
                or check_result.get("text")
                or check_result.get("raw")
                or check_result
                or ""
            ).strip()
            web_code_upper = web_code_raw.upper().replace(" ", "_")

            if "NO_LOCALIZADO" in web_code_upper or "NO_REGISTRO" in web_code_upper or "SIN_REGISTRO" in web_code_upper:
                _provider4_new_clear_flow(req.id)
            
                print(f"{provider_name}_WEB_NO_LOCALIZADO_DETECTED =", {
                    "request_id": req.id,
                    "term": term,
                    "code": web_code_raw,
                }, flush=True)
            
                raise RuntimeError(f"{provider_name}_NO_RECORD:{term}")

            raise RuntimeError(f"{provider_name}_READY_WITHOUT_PDF_BYTES:{web_code_raw[:300]}")

        if not check_result.get("ready"):
            # Si la API ya distingue NO_LOCALIZADO, aquí sí es SIN REGISTRO real.
            web_code_raw = str(check_result.get("code") or check_result.get("reason") or "").strip()
            web_code_upper = web_code_raw.upper().replace(" ", "_")

            if "NO_LOCALIZADO" in web_code_upper or "NO_REGISTRO" in web_code_upper or "SIN_REGISTRO" in web_code_upper:
                _provider4_new_clear_flow(req.id)

                print(f"{provider_name}_WEB_NO_LOCALIZADO_DETECTED =", {
                    "request_id": req.id,
                    "term": term,
                    "code": web_code_raw,
                }, flush=True)

                raise RuntimeError(f"{provider_name}_NO_RECORD:{term}")

            # Timeout contado desde que Lázaro aceptó peticion.php,
            # no desde que el usuario mandó el mensaje ni desde created_at.
            # Así una espera previa en Redis/RQ no consume el tiempo del proveedor.
            try:
                submitted_at_raw = (flow.get("submitted_at") or "").strip()
                web_elapsed_sec = 0.0
            
                if submitted_at_raw:
                    submitted_at = datetime.fromisoformat(submitted_at_raw)
            
                    if getattr(submitted_at, "tzinfo", None) is not None:
                        submitted_at = submitted_at.astimezone(
                            timezone.utc
                        ).replace(tzinfo=None)
            
                    web_elapsed_sec = max(
                        0.0,
                        (_utc_now_naive() - submitted_at).total_seconds(),
                    )
            
                else:
                    # Fallback solo para solicitudes antiguas creadas antes
                    # de que existiera submitted_at en el flow Redis.
                    created_at = getattr(req, "created_at", None)
            
                    if created_at:
                        if getattr(created_at, "tzinfo", None) is not None:
                            created_at = created_at.replace(tzinfo=None)
            
                        web_elapsed_sec = max(
                            0.0,
                            (_utc_now_naive() - created_at).total_seconds(),
                        )
            
                if web_elapsed_sec >= 11 * 60:
                    _provider4_new_clear_flow(req.id)
            
                    print(f"{provider_name}_NEW_TIMEOUT_BY_SUBMITTED_AT =", {
                        "request_id": req.id,
                        "elapsed_sec": round(web_elapsed_sec, 2),
                        "attempts": attempts,
                        "submitted_at": submitted_at_raw,
                        "last_code": check_result.get("code"),
                    }, flush=True)
            
                    raise RuntimeError(
                        f"{provider_name}_NEW_TIMEOUT_WAITING_PDF:{term}"
                    )
            
            except RuntimeError:
                raise
            
            except Exception as e:
                print(f"{provider_name}_NEW_TIMEOUT_BY_AGE_CHECK_ERROR =", {
                    "request_id": req.id,
                    "error": str(e),
                }, flush=True)
    
            flow["attempts"] = attempts
            flow["last_check_at"] = _utc_now_naive().isoformat()
            flow["last_code"] = check_result.get("code") or ""
            flow["last_reason"] = check_result.get("reason") or "NOT_READY"

            _provider4_new_set_flow(req.id, flow)

            req.status = "PROCESSING"
            req.error_message = f"{provider_name}_NEW_PDF_NOT_READY_ATTEMPT_{attempts}:{flow['last_code']}"
            req.updated_at = _utc_now_naive()
            db.commit()

            _enqueue_provider4_new_check(req.id, PROVIDER4_NEW_CHECK_DELAY_SEC)

            return {
                "pending": True,
                "reason": req.error_message,
            }

        pdf_bytes = check_result.get("pdf_bytes")

        if not pdf_bytes:
            raise RuntimeError(f"{provider_name}_NEW_READY_BUT_EMPTY_PDF:{term}")
        
        # PROTECCIÓN:
        # verificarpdf.php puede devolver PDF prematuro/sin marco.
        # Lo pasamos por el reparador de Provider4 antes de entregarlo.
        try:
            source = (check_result.get("source") or "").strip().lower()
        
            if chain_mode and source.startswith("history"):
                # Las rutas history y history_folio ya descargan,
                # validan y reparan mediante _download_and_validate_with_retries.
                # No volver a enmarcar ni reconstruir reverso.
                print(
                    f"{provider_name}_CHAIN_HISTORY_PDF_ALREADY_REPAIRED_SKIP =",
                    {
                        "request_id": req.id,
                        "term": term,
                        "source": source,
                        "inc_folio": inc_folio,
                    },
                    flush=True,
                )
            
            elif chain_mode:
                pdf_bytes = client._repair_chain_pdf_if_needed(
                    pdf_bytes,
                    term,
                )
            
            else:
                pdf_bytes = client._repair_pdf_if_needed(
                    pdf_bytes,
                    term,
                    inc_folio=inc_folio,
                )
        
        except Exception as repair_exc:
            print(f"{provider_name}_NEW_VERIFICAR_REPAIR_FAILED =", {
                "request_id": req.id,
                "term": term,
                "error": str(repair_exc),
            }, flush=True)
        
            raise RuntimeError(
                f"{provider_name}_NEW_VERIFICAR_PDF_INCOMPLETE_OR_FRAMELESS:"
                f"{term}:{str(repair_exc)[:250]}"
            )
        
        if not client._pdf_has_two_pages(pdf_bytes):
            raise RuntimeError(f"{provider_name}_NEW_VERIFICAR_REPAIRED_STILL_INCOMPLETE:{term}")
        
        _provider4_new_clear_flow(req.id)
        
        print(f"{provider_name}_NEW_DOWNLOAD_OK =", {
            "request_id": req.id,
            "attempts": attempts,
            "pdf_bytes": len(pdf_bytes),
            "repaired": True,
        }, flush=True)
        
        return {
            "pdf_bytes": pdf_bytes,
        }

    except Exception as e:
        err = str(e)

        # Normalizar errores para PROVIDER10/11.
        if provider_name != "PROVIDER4" and err.startswith("PROVIDER4_"):
            err = f"{provider_name}_{err[len('PROVIDER4_'):]}"

        raise RuntimeError(err) from e
    

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


def _validate_pdf_term_detailed(pdf_bytes: bytes, term: str, act_type: str | None = None) -> dict:
    """
    Valida CURP interna del PDF con 3 estados:
    - MATCH: la CURP esperada aparece internamente.
    - MISMATCH: el PDF trae CURP(s), pero ninguna es la esperada.
    - UNCERTAIN: no se pudo extraer CURP interna confiable.
    """
    expected = _normalize_alnum(term)

    if not expected:
        return {
            "status": "MATCH",
            "reason": "empty_expected",
            "expected": expected,
            "found_curps": [],
        }

    text = _extract_pdf_visible_text(pdf_bytes)

    if not text or len(text.strip()) < 30:
        return {
            "status": "UNCERTAIN",
            "reason": "text_too_short",
            "expected": expected,
            "found_curps": [],
        }

    found_curps = _find_curps_in_text(text)
    normalized_text = _normalize_alnum(text)
    act_type_up = (act_type or "").upper().strip()

    print("PROVIDER_VALIDATE_DETAILED_EXPECTED_CURP =", expected, flush=True)
    print("PROVIDER_VALIDATE_DETAILED_FOUND_CURPS =", found_curps, flush=True)
    print("PROVIDER_VALIDATE_DETAILED_ACT_TYPE =", act_type_up, flush=True)

    # 1) Si encontró CURPs completas internas, esa es la evidencia fuerte.
    # Si hay una diferente y no aparece la esperada, NO aceptar por filename.
    if found_curps:
        if expected in found_curps:
            return {
                "status": "MATCH",
                "reason": "expected_curp_found_in_pdf",
                "expected": expected,
                "found_curps": found_curps,
            }

        return {
            "status": "MISMATCH",
            "reason": "different_internal_curp_found",
            "expected": expected,
            "found_curps": found_curps,
        }

    # 2) A veces pypdf separa letras/números y el regex no arma CURP completa.
    # Aquí solo si el texto normalizado contiene exactamente la esperada.
    if expected in normalized_text:
        return {
            "status": "MATCH",
            "reason": "expected_found_in_normalized_text",
            "expected": expected,
            "found_curps": [],
        }

    # 3) No encontré CURP diferente, pero tampoco pude confirmar.
    # Aquí sí puede entrar respaldo por filename en main.py.
    return {
        "status": "UNCERTAIN",
        "reason": "no_internal_curp_detected",
        "expected": expected,
        "found_curps": [],
    }


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


def _extract_pdf_first_page_text(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))

        if len(reader.pages) < 1:
            return ""

        text = reader.pages[0].extract_text() or ""
        return text.upper().strip()

    except Exception as e:
        print("PROVIDER_VALIDATE_FIRST_PAGE_TEXT_ERROR =", str(e), flush=True)
        return ""


def _detect_pdf_act_type(pdf_bytes: bytes) -> str:
    # El tipo debe detectarse únicamente desde el frente del acta.
    # El reverso puede contener referencias a otros tipos de actas.
    text = _extract_pdf_first_page_text(pdf_bytes)

    if not text or len(text.strip()) < 30:
        print("PROVIDER_VALIDATE_ACT_TEXT_TOO_SHORT_SOFT_PASS", flush=True)
        return ""

    text_up = text.upper()
    text_norm = (
        text_up
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ü", "U")
    )

    text_compact = re.sub(r"[^A-Z]", "", text_norm)

    print("PROVIDER_VALIDATE_ACT_TEXT_PREVIEW =", text_norm[:800], flush=True)

    # 1) Primero detectar por título real del acta.
    # Esto es la evidencia más fuerte.
    title_patterns = [
        ("DEFUNCION", [
            r"ACTA\s+DE\s+DEFUNCION",
            r"ACTA\s+DE\s+DEFUNCI[OÓ]N",
            r"ACTADEDEFUNCION",
        ]),
        ("MATRIMONIO", [
            r"ACTA\s+DE\s+MATRIMONIO",
            r"ACTADEMATRIMONIO",
        ]),
        ("DIVORCIO", [
            r"ACTA\s+DE\s+DIVORCIO",
            r"ACTADEDIVORCIO",
        ]),
        ("NACIMIENTO", [
            r"ACTA\s+DE\s+NACIMIENTO",
            r"ACTADENACIMIENTO",
        ]),
    ]

    title_matches = []

    for act_group, patterns in title_patterns:
        matched = False
    
        for pat in patterns:
            if re.search(pat, text_norm, flags=re.IGNORECASE):
                matched = True
                break
    
            if re.search(pat, text_compact, flags=re.IGNORECASE):
                matched = True
                break
    
        if matched:
            title_matches.append(act_group)
    
    print("PROVIDER_VALIDATE_ACT_TYPE_TITLE_MATCHES =", title_matches, flush=True)
    
    # Solo aceptar el título si encontró exactamente un tipo.
    # Si aparecen varios títulos, continuar con la estructura fuerte.
    if len(title_matches) == 1:
        print("PROVIDER_VALIDATE_ACT_TYPE_TITLE_MATCH =", title_matches[0], flush=True)
        return title_matches[0]
    
    if len(title_matches) > 1:
        print("PROVIDER_VALIDATE_ACT_TYPE_MULTIPLE_TITLES_CONTINUE_TO_STRUCTURE =", title_matches, flush=True)

    # 2) Si no encontró título, usar SOLO estructura fuerte.
    # No usar palabras sueltas como NACIMIENTO, MATRIMONIO, CONTRAYENTE.
    scores = {
        "NACIMIENTO": 0,
        "MATRIMONIO": 0,
        "DEFUNCION": 0,
        "DIVORCIO": 0,
    }

    # NACIMIENTO: estructura fuerte
    if "DATOSDELAPERSONAREGISTRADA" in text_compact:
        scores["NACIMIENTO"] += 8

    if "DATOSDELREGISTRADO" in text_compact:
        scores["NACIMIENTO"] += 8

    if "PERSONAREGISTRADA" in text_compact:
        scores["NACIMIENTO"] += 5

    if "DATOSDEREGISTRO" in text_compact and "PADRES" in text_compact:
        scores["NACIMIENTO"] += 5

    # MATRIMONIO: estructura fuerte
    # OJO: ya NO usamos "CONTRAYENTE" solo.
    if "DATOSDELOSCONTRAYENTES" in text_compact:
        scores["MATRIMONIO"] += 8

    if "PRIMERCONTRAYENTE" in text_compact and "SEGUNDOCONTRAYENTE" in text_compact:
        scores["MATRIMONIO"] += 8

    if "CONTRAYENTES" in text_compact and "REGIMENPATRIMONIAL" in text_compact:
        scores["MATRIMONIO"] += 6

    if "SOCIEDADCONYUGAL" in text_compact or "SEPARACIONDEBIENES" in text_compact:
        scores["MATRIMONIO"] += 4

    # DEFUNCIÓN: estructura fuerte
    if "DATOSDELAPERSONAFALLECIDA" in text_compact:
        scores["DEFUNCION"] += 8

    if "DATOSDELADEFUNCION" in text_compact:
        scores["DEFUNCION"] += 8

    if "CERTIFICADODEDEFUNCION" in text_compact:
        scores["DEFUNCION"] += 5

    if "DESTINODELCADAVER" in text_compact or "CAUSASDELADEFUNCION" in text_compact:
        scores["DEFUNCION"] += 5

    # DIVORCIO: estructura fuerte
    if "DATOSDELDIVORCIO" in text_compact:
        scores["DIVORCIO"] += 8

    if "SENTENCIADEDIVORCIO" in text_compact:
        scores["DIVORCIO"] += 6

    if "DIVORCIADO" in text_compact and "DIVORCIADA" in text_compact:
        scores["DIVORCIO"] += 5

    print("PROVIDER_VALIDATE_ACT_TYPE_STRUCTURAL_SCORES =", scores, flush=True)

    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_type, best_score = ordered[0]
    second_type, second_score = ordered[1]

    print("PROVIDER_VALIDATE_ACT_TYPE_BEST =", {
        "best_type": best_type,
        "best_score": best_score,
        "second_type": second_type,
        "second_score": second_score,
    }, flush=True)

    # Solo aceptar estructura si:
    # - tiene score fuerte
    # - gana claramente al segundo lugar
    if best_score >= 8 and (best_score - second_score) >= 3:
        print("PROVIDER_VALIDATE_ACT_TYPE_STRUCTURAL_MATCH =", best_type, flush=True)
        return best_type

    # 3) Si no hay título ni estructura fuerte, NO adivinar.
    # Así evita falso MATRIMONIO/NACIMIENTO.
    print("PROVIDER_VALIDATE_ACT_TYPE_NOT_CONFIRMED_SAFE =", flush=True)
    return ""


def _expected_act_type_group(act_type: str | None) -> str:
    t = (act_type or "").upper().strip()

    if "NAC" in t:
        return "NACIMIENTO"

    if "MAT" in t:
        return "MATRIMONIO"

    if "DIV" in t:
        return "DIVORCIO"

    if "DEF" in t:
        return "DEFUNCION"

    return ""


def _validate_act_type_pdf(pdf_bytes: bytes, act_type: str | None) -> bool:
    expected = _expected_act_type_group(act_type)
    detected = _detect_pdf_act_type(pdf_bytes)

    print("PROVIDER_VALIDATE_ACT_TYPE_EXPECTED =", expected, flush=True)
    print("PROVIDER_VALIDATE_ACT_TYPE_DETECTED =", detected or "NO_CONFIRMADO", flush=True)

    # Si no sabemos qué esperaba el sistema, no bloquear.
    if not expected:
        print("PROVIDER_VALIDATE_ACT_TYPE_NO_EXPECTED_SOFT_PASS", flush=True)
        return True

    # Si no se pudo confirmar el tipo por texto interno del PDF,
    # NO bloquear. Esto evita falsos errores como el PDF visualmente correcto
    # de matrimonio que pypdf no leyó bien.
    if not detected:
        print("PROVIDER_VALIDATE_ACT_TYPE_NOT_CONFIRMED_SOFT_PASS", flush=True)
        return True

    # Solo bloquear cuando el PDF confirma claramente OTRO tipo.
    if detected != expected:
        print("PROVIDER_VALIDATE_ACT_TYPE_MISMATCH_HARD_FAIL =", {
            "expected": expected,
            "detected": detected,
        }, flush=True)
        return False

    return True


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


def _acquire_request_processing_lock(request_id: int) -> str | None:
    """
    Impide que dos workers procesen simultáneamente el mismo request_id.
    El token permite liberar únicamente nuestro propio lock.
    """
    token = f"{random.getrandbits(128):032x}"
    key = f"request_processing_lock:{request_id}"

    try:
        acquired = redis_conn.set(
            key,
            token,
            nx=True,
            ex=60 * 15,
        )

        if acquired:
            return token

        return None

    except Exception as e:
        print("REQUEST_PROCESSING_LOCK_REDIS_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)

        # Mejor no procesar si Redis no puede garantizar exclusión.
        return None


def _release_request_processing_lock(request_id: int, token: str | None):
    if not token:
        return

    key = f"request_processing_lock:{request_id}"

    try:
        current = redis_conn.get(key)

        if isinstance(current, bytes):
            current = current.decode("utf-8", errors="ignore")

        if current == token:
            redis_conn.delete(key)

    except Exception as e:
        print("REQUEST_PROCESSING_UNLOCK_REDIS_ERROR =", {
            "request_id": request_id,
            "error": str(e),
        }, flush=True)


def process_request(request_id: int):
    processing_lock_token = _acquire_request_processing_lock(request_id)

    if not processing_lock_token:
        print("PROCESS_REQUEST_DUPLICATE_OR_LOCK_UNAVAILABLE_SKIP =", {
            "request_id": request_id,
        }, flush=True)
        return

    db = SessionLocal()

    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return
        
        if _worker_stop_if_instance_blocked(
            req,
            db,
            label="WORKER_BLOCKED_INSTANCE_AT_START",
        ):
            return
        
        filename = _default_pdf_filename(req)
        
        current_status = (req.status or "").strip().upper()
        
        if current_status == "DONE":
            print("PROCESS_REQUEST_ALREADY_DONE_SKIP =", {
                "request_id": req.id,
                "curp": req.curp,
                "act_type": req.act_type,
                "provider_name": req.provider_name,
                "status": req.status,
            }, flush=True)
            return

        terminal_error = (getattr(req, "error_message", "") or "").upper()

        # Una API vencida ya no debe ser procesada otra vez aunque exista
        # un job viejo pendiente en Redis/RQ.
        if (
            _is_api_request(req)
            and current_status == "ERROR"
            and terminal_error.startswith("API_STALE_TIMEOUT:")
        ):
            print("PROCESS_REQUEST_API_STALE_SKIP =", {
                "request_id": req.id,
                "api_client_id": req.api_client_id,
                "curp": req.curp,
                "act_type": req.act_type,
                "error_message": req.error_message,
            }, flush=True)
            return

        if current_status == "ERROR" and (
            "CLIENT_NOTIFIED_FAIL" in terminal_error
            or "SIN REGISTRO" in terminal_error
            or "SIN_REGISTRO" in terminal_error
            or "_NO_RECORD" in terminal_error
            or "NO_RECORD" in terminal_error
            or "NO_LOCALIZADO" in terminal_error
            or "NO HAY REGISTROS" in terminal_error
        ):
            print("PROCESS_REQUEST_ALREADY_TERMINAL_NO_RECORD_SKIP =", {
                "request_id": req.id,
                "curp": req.curp,
                "act_type": req.act_type,
                "provider_name": req.provider_name,
                "status": req.status,
                "error_message": req.error_message,
            }, flush=True)
            return
        
        print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
        print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
        
        process_started_ts = time.perf_counter()
        
        req.status = "PROCESSING"
        req.updated_at = _utc_now_naive()
        db.commit()

        if req.source_group_id and not _request_is_no_accounting(req, db):
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

        current_queue = _current_queue_name()
        existing_provider = (req.provider_name or "").strip().upper()
        
        reuse_existing_slow_provider = (
            current_queue == SLOW_PROVIDER_QUEUE_NAME
            and existing_provider in SLOW_PROVIDERS
        )
        
        if reuse_existing_slow_provider:
            if existing_provider in SLOW_PROVIDERS and not _is_provider4_eligible(req.curp, req.act_type):
                print("SLOW_QUEUE_LAZARO_NOT_ELIGIBLE_REQUEUE_NORMAL =", {
                    "request_id": req.id,
                    "curp": req.curp,
                    "act_type": req.act_type,
                    "old_provider": existing_provider,
                    "queue": current_queue,
                }, flush=True)
        
                try:
                    _provider4_new_clear_flow(req.id)
                except Exception as clear_exc:
                    print("SLOW_QUEUE_LAZARO_NOT_ELIGIBLE_CLEAR_FLOW_ERROR =", {
                        "request_id": req.id,
                        "old_provider": existing_provider,
                        "error": str(clear_exc),
                    }, flush=True)
        
                req.provider_name = ""
                req.provider_group_id = None
                req.provider_message = None
                req.status = "QUEUED"
                req.error_message = f"REQUEUED_LAZARO_NOT_ELIGIBLE:{existing_provider}"
                req.updated_at = _utc_now_naive()
                db.commit()
        
                request_queue.enqueue(process_request, req.id)
        
                print("SLOW_QUEUE_LAZARO_NOT_ELIGIBLE_REQUEUED_TO_NORMAL =", {
                    "request_id": req.id,
                    "old_provider": existing_provider,
                    "queue": "actas",
                }, flush=True)
        
                return
            
            if not _provider_is_enabled(db, existing_provider):
                print("SLOW_QUEUE_EXISTING_PROVIDER_DISABLED_REQUEUE =", {
                    "request_id": req.id,
                    "old_provider": existing_provider,
                    "queue": current_queue,
                }, flush=True)
        
                try:
                    _provider4_new_clear_flow(req.id)
                except Exception as clear_exc:
                    print("SLOW_QUEUE_DISABLED_CLEAR_FLOW_ERROR =", {
                        "request_id": req.id,
                        "old_provider": existing_provider,
                        "error": str(clear_exc),
                    }, flush=True)
        
                req.provider_name = ""
                req.provider_group_id = None
                req.provider_message = None
                req.status = "QUEUED"
                req.error_message = f"REQUEUED_AFTER_{existing_provider}_DISABLED"
                req.updated_at = _utc_now_naive()
                db.commit()
        
                request_queue.enqueue(process_request, req.id)
        
                print("SLOW_QUEUE_DISABLED_REQUEUED_TO_NORMAL =", {
                    "request_id": req.id,
                    "old_provider": existing_provider,
                    "queue": "actas",
                }, flush=True)
        
                return
        
            # Este job viene de un reroute actas -> actas_slow.
            # NO volver a sortear proveedor, porque puede cambiar PROVIDER10/11/4
            # y generar errores falsos o reprocesos raros.
            provider_name = existing_provider
            provider_group_id = req.provider_group_id
            text_to_provider = req.provider_message
        
            print("SLOW_QUEUE_REUSING_PROVIDER_FROM_REROUTE =", {
                "request_id": req.id,
                "provider_name": provider_name,
                "queue": current_queue,
            }, flush=True)
        
        else:
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

        if provider_name in (
            "PROVIDER1",
            "PROVIDER2",
            "PROVIDER5",
            "PROVIDER6",
            "PROVIDER8",
            "PROVIDER9",
            "PROVIDER12",
            "PROVIDER13",
            "PROVIDER14",
            "MAYAPROVIDER",
        ):
            print("PROVIDER_SEND_TO_PROVIDER =", req.id, time.time(), flush=True)
        
            send_ok = False
            last_err = None
        
            for attempt in range(3):
                try:
                    sender_instance = _provider_sender_instance(provider_name, req)
                    print("PROVIDER_SENDER_INSTANCE =", sender_instance, flush=True)

                    if provider_name == "PROVIDER14":
                        _send_provider14_request(req, db)
                        send_ok = True
                        break
                    
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
                        time.sleep(5 * (attempt + 1))
        
            if send_ok:
                return
            
            last_err_text = (last_err or "")
            is_connection_closed = "CONNECTION CLOSED" in last_err_text.upper()
            
            retry_key = f"provider_send_retry:{req.id}"
            
            if is_connection_closed:
                try:
                    already_retried = redis_conn.get(retry_key)
            
                    if not already_retried:
                        redis_conn.set(retry_key, "1", ex=600)
            
                        req.status = "QUEUED"
                        req.error_message = f"{provider_name}_SEND_RETRY_CONNECTION_CLOSED"
                        req.updated_at = _utc_now_naive()
                        db.commit()
            
                        request_queue.enqueue_in(
                            timedelta(seconds=20),
                            process_request,
                            req.id,
                        )
            
                        print("PROVIDER_SEND_CONNECTION_CLOSED_REQUEUED =", {
                            "req_id": req.id,
                            "provider_name": provider_name,
                            "retry_in_seconds": 20,
                            "last_err": last_err_text[:300],
                        }, flush=True)
            
                        return
            
                except Exception as retry_exc:
                    print("PROVIDER_SEND_REQUEUE_ERROR =", str(retry_exc), flush=True)
            
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
        
            _notify_support_error(
                req,
                f"{provider_name}_SEND_FAILED:{last_err or ''}",
                last_err or ""
            )
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
        
            pdf_bytes = _require_pdf_bytes(provider3_result, "PROVIDER3", req)
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

            filename = (
                f"{req.curp}_FOLIO.pdf"
                if "FOLIO" in (req.act_type or "").upper()
                else f"{req.curp}.pdf"
            )

            try:
                save_request_pdf_to_r2(
                    req,
                    db,
                    pdf_bytes,
                    filename=filename,
                    origin="worker:PROVIDER3",
                )
            except Exception as r2_exc:
                print("R2_SAVE_PROVIDER3_PDF_ERROR =", {
                    "req_id": getattr(req, "id", None),
                    "filename": filename,
                    "error": str(r2_exc),
                }, flush=True)
            
            instance = req.instance_name or "docifybot8"
            
            if _store_api_pdf_result(req, db, safe_media_b64, filename, f"BASE64_{provider_name}_API"):
                return

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print("PROVIDER3_SEND_INSTANCE =", instance, flush=True)
        
            delivered = _deliver_pdf_base64_with_retries(
                req,
                db,
                safe_media_b64,
                filename,
                caption_text,
                instance,
                label="PROVIDER3",
            )
            
            if not delivered:
                return
        
            redis_conn.set(delivery_key, "1", ex=3600)
        
            req.provider_media_url = "BASE64_PROVIDER3"
            #req.pdf_url = None
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
        
                if isinstance(provider4_result, dict) and provider4_result.get("pending"):
                    print("PROCESS_REQUEST_PROVIDER4_PENDING_RETURN =", {
                        "request_id": req.id,
                        "provider_name": provider_name,
                        "reason": provider4_result.get("reason"),
                    }, flush=True)
                    return
        
                pdf_bytes = _require_pdf_bytes(provider4_result, provider_name, req)

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
                    pdf_text = _extract_pdf_visible_text(pdf_bytes)
                    normalized_pdf_text = _normalize_alnum(pdf_text)
                    normalized_chain = _normalize_alnum(term)
                
                    # Solo rechazar si sí pudimos leer contenido suficiente
                    # y la cadena definitivamente no aparece.
                    #
                    # Si el PDF es escaneado o el texto no se puede extraer,
                    # no se debe rechazar una acta válida por falso negativo.
                    if pdf_text and len(pdf_text.strip()) >= 30:
                        if normalized_chain not in normalized_pdf_text:
                            print(
                                f"{provider_name}_VALIDATE_FAIL_REQ_ELECTRONIC_ID_OR_CODE =",
                                {
                                    "term": term,
                                    "text_len": len(pdf_text),
                                },
                                flush=True,
                            )
                
                            raise RuntimeError(
                                f"{provider_name}_WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF:{term}"
                            )
                
                        print(
                            f"{provider_name}_VALIDATE_CHAIN_FOUND_IN_FINAL_PDF =",
                            term,
                            flush=True,
                        )
                
                    else:
                        print(
                            f"{provider_name}_VALIDATE_CHAIN_TEXT_UNREADABLE_SOFT_PASS =",
                            {
                                "term": term,
                                "text_len": len(pdf_text or ""),
                            },
                            flush=True,
                        )
                else:
                    term_check = _validate_pdf_term_detailed(
                        pdf_bytes,
                        term,
                        req.act_type,
                    )
                
                    term_status = term_check.get("status")
                    term_reason = term_check.get("reason")
                    found_curps = term_check.get("found_curps") or []
                
                    print(f"{provider_name}_VALIDATE_TERM_DETAILED =", {
                        "req_id": req.id,
                        "term": term,
                        "status": term_status,
                        "reason": term_reason,
                        "found_curps": found_curps,
                    }, flush=True)
                
                    if term_status == "MISMATCH":
                        print(f"{provider_name}_VALIDATE_FAIL_INTERNAL_CURP =", {
                            "req_id": req.id,
                            "expected": term,
                            "found_curps": found_curps,
                        }, flush=True)
                        raise RuntimeError(f"{provider_name}_WRONG_CURP_IN_PDF:{term}:found_curps={found_curps}")
                
                    if term_status == "UNCERTAIN":
                        # Provider4/10/11 ya pasó por su propio proceso de descarga/validación.
                        # Si aquí pypdf no pudo confirmar CURP interna, NO lo mates como CURP incorrecta.
                        print(f"{provider_name}_VALIDATE_CURP_UNCERTAIN_SOFT_PASS =", {
                            "req_id": req.id,
                            "term": term,
                            "reason": term_reason,
                            "found_curps": found_curps,
                        }, flush=True)
        
            except Exception as p4_exc:
                p4_err = str(p4_exc)
                p4_elapsed = time.perf_counter() - provider4_started_ts
                enabled = _enabled_providers(db)

                wrong_pdf_errors = (
                    p4_err.startswith(f"{provider_name}_WRONG_CURP_IN_PDF")
                    or p4_err.startswith(f"{provider_name}_WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF")
                    or p4_err.startswith(f"{provider_name}_WRONG_ACT_TYPE")
                )
            
                fallback_errors = (
                    p4_err.startswith(f"{provider_name}_BACKEND_FAILED:")
                    or p4_err.startswith(f"{provider_name}_VGET_FAILED:")
                    or p4_err.startswith(f"{provider_name}_EMPTY_OR_USELESS_HTML")
                    or p4_err.startswith(f"{provider_name}_HISTORY_FAILED:")
                    or p4_err.startswith(f"{provider_name}_HISTORY_NOT_CONFIRMED_PDF:")
                    or p4_err.startswith(f"{provider_name}_HISTORY_NOT_CONFIRMED_FOLIO:")
                    or p4_err.startswith(f"{provider_name}_NO_PDF_LINK_FOR:")
                    or p4_err.startswith(f"{provider_name}_NO_FOLIO_LINK_FOR:")
                    or p4_err.startswith(f"{provider_name}_DOWNLOAD_FAILED:")
                    or p4_err.startswith(f"{provider_name}_FOLIO_DOWNLOAD_FAILED:")
                    or p4_err.startswith(f"{provider_name}_NEW_TIMEOUT_WAITING_PDF:")
                    or p4_err.startswith(f"{provider_name}_READY_WITHOUT_PDF_BYTES:")
                    or p4_err.startswith(f"{provider_name}_NEW_PETICION_UNKNOWN_RESPONSE:")
                    or p4_err.startswith(f"{provider_name}_NEW_VERIFICAR_UNKNOWN_RESPONSE:")
                    or wrong_pdf_errors
                    or "Read timed out" in p4_err
                    or "READ TIMED OUT" in p4_err.upper()
                    or "TIMEOUT" in p4_err.upper()
                )
            
                immediate_fallback_errors = (
                    p4_err.startswith(f"{provider_name}_NO_FORM_ACTION")
                    or p4_err.startswith(f"{provider_name}_EMPTY_OR_USELESS_HTML")
                    or p4_err.startswith(f"{provider_name}_BACKEND_FAILED:")
                    or p4_err.startswith(f"{provider_name}_VGET_FAILED:")
                    or p4_err.startswith(f"{provider_name}_NEW_TIMEOUT_WAITING_PDF:")
                    or p4_err.startswith(f"{provider_name}_READY_WITHOUT_PDF_BYTES:")
                    or p4_err.startswith(f"{provider_name}_NEW_PETICION_UNKNOWN_RESPONSE:")
                    or p4_err.startswith(f"{provider_name}_NEW_VERIFICAR_UNKNOWN_RESPONSE:")
                    or wrong_pdf_errors
                )
                
                should_fallback = (
                    immediate_fallback_errors
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
                            "PROVIDER12",
                            "PROVIDER13",
                            "PROVIDER14",
                        }
                        and p != provider_name
                        and not (p == "PROVIDER6" and not _is_provider6_allowed_request(req.curp, req.act_type))
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
                
                        if fallback_provider == "PROVIDER14":
                            _send_provider14_request(req, db)
                        else:
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
                        retry_count_key = f"fallback:no_provider_available:{req.id}"
                        retry_count = int(redis_conn.incr(retry_count_key) or 1)
                        redis_conn.expire(retry_count_key, 3600)
                    
                        if retry_count <= 3:
                            req.status = "QUEUED"
                            req.error_message = f"{provider_name}_WAITING_FALLBACK_PROVIDER_AVAILABLE_ATTEMPT_{retry_count}:{p4_err[:300]}"
                            req.updated_at = _utc_now_naive()
                            db.commit()
                    
                            slow_request_queue.enqueue_in(
                                timedelta(seconds=120 * retry_count),
                                process_request,
                                req.id,
                            )
                    
                            print("FALLBACK_NO_PROVIDER_AVAILABLE_REQUEUED =", {
                                "req_id": req.id,
                                "attempt": retry_count,
                                "delay_sec": 120 * retry_count,
                                "provider": provider_name,
                                "err": p4_err[:300],
                            }, flush=True)
                    
                            return
                    
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

            req_id_for_pdf = int(req.id)

            try:
                db.rollback()
            
                req = (
                    db.query(RequestLog)
                    .filter(RequestLog.id == req_id_for_pdf)
                    .first()
                )
            
                if not req:
                    raise RuntimeError(
                        f"REQUEST_NOT_FOUND_BEFORE_R2_SAVE:{req_id_for_pdf}"
                    )
            
                save_request_pdf_to_r2(
                    req,
                    db,
                    pdf_bytes,
                    filename=filename,
                    origin=f"worker:{provider_name}",
                )
            
            except Exception as r2_exc:
                try:
                    db.rollback()
                except Exception:
                    pass
            
                print(f"R2_SAVE_{provider_name}_PDF_ERROR =", {
                    "req_id": req_id_for_pdf,
                    "filename": filename,
                    "error": str(r2_exc),
                }, flush=True)

            instance = req.instance_name or "docifybot8"

            if _store_api_pdf_result(req, db, safe_media_b64, filename, f"BASE64_{provider_name}_API"):
                return

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print(f"{provider_name}_SEND_INSTANCE =", instance, flush=True)
        
            delivered = _deliver_pdf_base64_with_retries(
                req,
                db,
                safe_media_b64,
                filename,
                caption_text,
                instance,
                label=provider_name,
            )
            
            if not delivered:
                return
        
            redis_conn.set(delivery_key, "1", ex=3600)
        
            req.provider_media_url = f"BASE64_{provider_name}"
            #req.pdf_url = None
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
        
            pdf_bytes = _require_pdf_bytes(provider7_result, "PROVIDER7", req)
            safe_media_b64 = base64.b64encode(pdf_bytes).decode()
        
            total_seconds = _request_total_seconds(req, process_started_ts)
            caption_text = f"⏱️ Tiempo total: {_fmt_seconds(total_seconds)}"
        
            filename = (
                f"{req.curp}_FOLIO.pdf"
                if "FOLIO" in (req.act_type or "").upper()
                else f"{req.curp}.pdf"
            )

            try:
                save_request_pdf_to_r2(
                    req,
                    db,
                    pdf_bytes,
                    filename=filename,
                    origin="worker:PROVIDER7",
                )
            except Exception as r2_exc:
                print("R2_SAVE_PROVIDER7_PDF_ERROR =", {
                    "req_id": getattr(req, "id", None),
                    "filename": filename,
                    "error": str(r2_exc),
                }, flush=True)

            instance = req.instance_name or "docifybot8"

            if _store_api_pdf_result(
                req,
                db,
                safe_media_b64,
                filename,
                "BASE64_PROVIDER7_API",
            ):
                return

            print("REQ_INSTANCE_NAME =", req.instance_name, flush=True)
            print("REQ_SOURCE_GROUP_ID =", req.source_group_id, flush=True)
            print("PROVIDER7_SEND_INSTANCE =", instance, flush=True)
        
            delivered = _deliver_pdf_base64_with_retries(
                req,
                db,
                safe_media_b64,
                filename,
                caption_text,
                instance,
                label="PROVIDER7",
            )
            
            if not delivered:
                return
        
            req.provider_media_url = "BASE64_PROVIDER7"
            #req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = _utc_now_naive()
            db.commit()

            _after_done_accounting(req, db)
        
            return

        raise RuntimeError("UNKNOWN_PROVIDER")

    except Exception as e:
        err = str(e)
    
        try:
            db.rollback()
        except Exception as rollback_exc:
            print("PROCESS_REQUEST_ERROR_ROLLBACK_FAILED =", {
                "request_id": request_id,
                "error": str(rollback_exc),
            }, flush=True)
    
        req = (
            db.query(RequestLog)
            .filter(RequestLog.id == request_id)
            .first()
        )
    
        if req:
            req.updated_at = _utc_now_naive()

            if err == "NO_PROVIDER_FOR_SPECIAL_FORMAT":
                req.status = "ERROR"
                req.error_message = err
                db.commit()

                msg = (
                    "⚠️ *Formato no disponible actualmente*\n\n"
                    "Las consultas por *curp, cadena o código de verificación* "
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
                    "Las consultas por *curp, cadena o código de verificación* "
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
                retry_count_key = f"no_provider_enabled_retry:{req.id}"
                retry_count = int(redis_conn.incr(retry_count_key) or 1)
                redis_conn.expire(retry_count_key, 1800)
            
                if retry_count <= 3:
                    req.status = "QUEUED"
                    req.error_message = f"NO_PROVIDER_ENABLED_RETRY_{retry_count}"
                    req.updated_at = _utc_now_naive()
                    db.commit()
            
                    request_queue.enqueue_in(
                        timedelta(seconds=60 * retry_count),
                        process_request,
                        req.id,
                    )
            
                    print("NO_PROVIDER_ENABLED_REQUEUED =", {
                        "request_id": req.id,
                        "attempt": retry_count,
                        "delay_sec": 60 * retry_count,
                    }, flush=True)
            
                    return
            
                req.status = "ERROR"
                req.error_message = err
                req.updated_at = _utc_now_naive()
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

            if "DISABLED_BEFORE_PROCESSING" in err:
                old_provider = (req.provider_name or "").strip().upper()

                print("PROVIDER_DISABLED_BEFORE_PROCESSING_REQUEUE =", {
                    "request_id": req.id,
                    "curp": req.curp,
                    "act_type": req.act_type,
                    "old_provider": old_provider,
                    "err": err,
                }, flush=True)

                try:
                    if old_provider in {"PROVIDER4", "PROVIDER10", "PROVIDER11"}:
                        _provider4_new_clear_flow(req.id)
                except Exception as clear_exc:
                    print("PROVIDER_DISABLED_CLEAR_FLOW_ERROR =", {
                        "request_id": req.id,
                        "old_provider": old_provider,
                        "error": str(clear_exc),
                    }, flush=True)

                req.provider_name = ""
                req.provider_group_id = None
                req.status = "QUEUED"
                req.error_message = f"REQUEUED_AFTER_{old_provider}_DISABLED"
                req.updated_at = _utc_now_naive()
                db.commit()

                request_queue.enqueue(process_request, req.id)

                print("PROVIDER_DISABLED_REQUEUED =", {
                    "request_id": req.id,
                    "old_provider": old_provider,
                    "queue": "actas",
                }, flush=True)

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

                dedupe_key = f"no_record_notified:{req.id}"
                
                try:
                    first_notify = redis_conn.set(dedupe_key, "1", nx=True, ex=86400)
                except Exception as dedupe_exc:
                    print("NO_RECORD_DEDUPE_REDIS_ERROR =", str(dedupe_exc), flush=True)
                    first_notify = True
                
                if first_notify:
                    if req.source_group_id:
                        send_group_text(req.source_group_id, msg, instance)
                    else:
                        from app.services.evolution import send_text
                        send_text(req.requester_wa_id, msg, instance)
                
                    print("NO_RECORD_NOTIFIED_ONCE =", dedupe_key, flush=True)
                else:
                    print("NO_RECORD_DUPLICATE_IGNORED =", dedupe_key, flush=True)

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

            # PDF_BYTES_GENERAL_NO_LOCALIZADO_SAFETY_OK:
            # Si cayó al except general con KeyError('pdf bytes'), pero el web provider realmente
            # responde NO_LOCALIZADO, no debe mandar soporte. Debe avisar al cliente como SIN REGISTRO.
            try:
                _err_pdf_txt = str(err or "").strip()
                _err_pdf_up = _err_pdf_txt.upper().replace(" ", "_")
                _prov_pdf_up = (getattr(req, "provider_name", "") or "").strip().upper()

                if (
                    _prov_pdf_up in ("PROVIDER4", "PROVIDER10", "PROVIDER11")
                    and "PDF" in _err_pdf_up
                    and "BYTES" in _err_pdf_up
                ):
                    # PDF_BYTES_DIRECT_SIN_REGISTRO_OK:
                    # En Lázaro Web, este error aparece cuando el panel/verificarpdf ya trae NO_LOCALIZADO
                    # pero alguna capa intentó leer pdf_bytes inexistente.
                    # Para evitar falso soporte, se cierra como SIN REGISTRO y se avisa al cliente.
                    _no_loc_detected = any(
                        token in _err_pdf_up
                        for token in ("NO_LOCALIZADO", "NO_REGISTRO", "SIN_REGISTRO", "NO_RECORD")
                    )
                    _check_debug = "PDF_BYTES_CHECK_STARTED"

                    try:
                        _tipo_retry = _map_act_type_to_provider4_tipo(req.act_type)
                    except Exception:
                        _tipo_retry = tipoa if "tipoa" in locals() else ""

                    try:
                        _client_retry = client if "client" in locals() else None
                        if _client_retry is not None:
                            if _worker_stop_if_instance_blocked(
                                req,
                                db,
                                label="WORKER_BLOCKED_INSTANCE_BEFORE_LAZARO_ERROR_RECHECK",
                            ):
                                return
                                
                            _retry_term = (
                                getattr(req, "curp", "") or ""
                            ).strip().upper()
                            
                            _retry_chain_mode = (
                                is_chain(_retry_term)
                                or bool(re.fullmatch(r"\d{15,25}", _retry_term))
                            )
                            
                            _retry_inc_folio = _is_folio_act(
                                getattr(req, "act_type", "")
                            )
                            
                            if _retry_chain_mode:
                                _chk_retry = _client_retry.verificar_cadena_por_historial_new_api(
                                    cadena=_retry_term,
                                    tipoa=_tipo_retry,
                                    inc_folio=_retry_inc_folio,
                                )
                            else:
                                _chk_retry = _client_retry.verificar_pdf_new_api(
                                    curp=_retry_term,
                                    tipoa=_tipo_retry,
                                )
                            _check_debug = str(_chk_retry)
                            _chk_up = _check_debug.upper().replace(" ", "_")
                            if "NO_LOCALIZADO" in _chk_up or "NO_REGISTRO" in _chk_up or "SIN_REGISTRO" in _chk_up:
                                _no_loc_detected = True
                    except Exception as _chk_e:
                        _check_debug = str(_chk_e)
                        _chk_up = _check_debug.upper().replace(" ", "_")
                        if "NO_LOCALIZADO" in _chk_up or "NO_REGISTRO" in _chk_up or "SIN_REGISTRO" in _chk_up:
                            _no_loc_detected = True

                    if _no_loc_detected:
                        try:
                            _provider4_new_clear_flow(req.id)
                        except Exception:
                            pass

                        req.status = "ERROR"
                        req.error_message = "SIN REGISTRO | CLIENT_NOTIFIED_FAIL"
                        req.updated_at = _utc_now_naive()
                        db.commit()

                        print(f"{_prov_pdf_up}_PDF_BYTES_GENERAL_NO_LOCALIZADO_DETECTED = {{'request_id': {req.id}, 'curp': {getattr(req, 'curp', '')!r}, 'error': {_err_pdf_txt!r}, 'check': {_check_debug[:300]!r}}}", flush=True)

                        _notify_client_no_record_once(
                            req,
                            label=f"{_prov_pdf_up}_PDF_BYTES_GENERAL_NO_LOCALIZADO"
                        )
                        return

            except Exception as _pdfbytes_general_safety_e:
                print(f"PDF_BYTES_GENERAL_NO_LOCALIZADO_SAFETY_ERROR = {str(_pdfbytes_general_safety_e)!r}", flush=True)

            req.status = "ERROR"
            req.error_message = err
            req.updated_at = _utc_now_naive()
            db.commit()

            extra_soporte = (
                "Falla capturada por el worker. "
                "Revisar si fue error de proveedor, validación o entrega por WhatsApp. "
                "Si el detalle menciona sendMedia / Connection Closed, el PDF pudo haberse generado "
                "pero falló la entrega por la instancia del bot."
            )
            
            if err.startswith("PROVIDER6_ACT_TYPE_NOT_ALLOWED"):
                extra_soporte = (
                    "El sistema detectó que ACTAS ESCALANTE no acepta este tipo de acta. "
                    "No debe enviarse MATRIMONIO, DEFUNCIÓN ni DIVORCIO a Escalante. "
                    "Revisar el selector de proveedor, modo forzado del bot o pesos del proveedor. "
                    "La solución correcta es excluir PROVIDER6 antes del sorteo para estos tipos."
                )
            
            _notify_support_error(req, err, extra_soporte)
        raise
        
    finally:
        try:
            db.close()
        finally:
            _release_request_processing_lock(
                request_id,
                processing_lock_token,
            )
