import os
import base64
import secrets
import uuid
from decimal import Decimal
import time
import random
import asyncio
import re
import uuid
import json
import requests
import secrets
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import Counter, defaultdict
from urllib.parse import urlencode

from fastapi import FastAPI, Depends, Body, Request, BackgroundTasks, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from starlette.middleware.gzip import GZipMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import Base, engine, get_db, SessionLocal
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog, ProviderSetting, AppSetting, GroupPromotion, GroupAlias, GroupCategory, BotControl, BotRechargeLog, ApiClient, ApiCreditLog
from app.queue import request_queue, slow_request_queue, redis_conn, broadcast_queue, ack_queue
from rq.registry import StartedJobRegistry, DeferredJobRegistry, ScheduledJobRegistry, FailedJobRegistry
from app.worker import (
    process_request,
    provider3_keepalive_job,
    _validate_act_type_pdf,
    _validate_pdf_matches_term,
    _validate_pdf_term_detailed,
    _notify_support_error,
    _handle_api_charge_after_done,
    _detect_pdf_act_type,
    _expected_act_type_group,
    retry_pdf_delivery,
)
from app.services.provider3 import Provider3Client
from app.services.provider4 import Provider4Client
from app.services.provider7 import Provider7Client
from types import SimpleNamespace

from app.utils.curp import (
    extract_request_terms,
    detect_act_type,
    normalize_text,
    extract_identifier_loose,
    extract_identifier_from_filename,
    detect_identifier_problem,
    is_chain,
)

from app.services.evolution import (
    send_text,
    send_document,
    send_group_document,
    send_group_text,
    send_document_base64,
    send_group_document_base64,
    get_media_base64,
    send_reaction,
)

from app.utils.bot_limits import (
    get_bot_limit,
    get_bot_used,
    set_bot_limit,
    set_bot_used,
    increment_bot_used_and_maybe_block,
    block_instance,
    unblock_instance,
)

from sqlalchemy import func, case, or_, and_
from app.broadcast_jobs import botpanel_broadcast_job, panel_private_bots_broadcast_job
from app.pdf_storage import save_request_pdf_to_r2, generate_r2_presigned_download_url

app = FastAPI(title=settings.APP_NAME)

# Comprime HTML/JS/CSS grandes del panel y mini panel.
# Esto ayuda mucho cuando 20+ personas abren paneles desde internet.
app.add_middleware(
    GZipMiddleware,
    minimum_size=1000,
    compresslevel=5,
)


@app.middleware("http")
async def panel_request_timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    path = request.url.path

    try:
        response = await call_next(request)
        return response
    finally:
        if path.startswith("/panel") or path.startswith("/botpanel"):
            elapsed = round(time.perf_counter() - start, 3)
            print(
                "PANEL_HTTP_TIMING =",
                {
                    "path": path,
                    "method": request.method,
                    "elapsed_s": elapsed,
                },
                flush=True,
            )

PANEL_TZ = "America/Monterrey"
BLOCKED_GROUPS_KEY = "blocked_groups_no_response"
BLOCKED_INSTANCES_KEY = "blocked_instances_no_response"
ADMIN_BLOCKED_INSTANCES_KEY = "admin_blocked_instances_no_minipanel_unlock"

PANEL_HTML_TTL = 180
PANEL_RECENT_TTL = 60
PANEL_GROUP_DETAIL_TTL = 180
GROUP_NAME_CACHE_TTL = 300
EVOLUTION_STATE_CACHE_TTL = 60
PANEL_HTML_CACHE_VERSION = "2026-07-01-qr-token-v1"

PANEL_STREAM_SLEEP = 5
PANEL_STREAM_ENABLED = False

CLEANUP_ENABLED_KEY = "cleanup:enabled"
CLEANUP_MAX_AGE_MINUTES_KEY = "cleanup:max_age_minutes"
CLEANUP_DEFAULT_MAX_AGE_MINUTES = 45

EVOLUTION_BASE_URL = settings.EVOLUTION_BASE_URL.rstrip("/")
EVOLUTION_APIKEY = settings.EVOLUTION_API_KEY
PANEL_TOKEN = settings.ADMIN_PANEL_TOKEN
MAIN_PANEL_INSTANCE = "docifybot8"

BOT_PROVIDER_MODE_KEY_PREFIX = "BOT_PROVIDER_MODE:"
DEFAULT_BOT_PROVIDER_MODE = {
    "docifybot8maya": "GLOBAL_POOL",
}

BOT_PROVIDER_OPTIONS = {
    "GLOBAL_POOL": "Global · proveedores de Mesino",
    "PERSONAL:MAYAPROVIDER": "Privado · proveedor personal",
}


def _norm_instance(instance_name: str | None) -> str:
    return (instance_name or "").strip().lower()


def _bot_provider_mode(db: Session, instance_name: str | None) -> str:
    inst = _norm_instance(instance_name)
    if not inst:
        return "GLOBAL_POOL"

    default = DEFAULT_BOT_PROVIDER_MODE.get(inst, "GLOBAL_POOL")
    mode = _get_app_setting(db, f"{BOT_PROVIDER_MODE_KEY_PREFIX}{inst}", default)
    return (mode or default or "GLOBAL_POOL").strip().upper()


def _set_bot_provider_mode(db: Session, instance_name: str, mode: str):
    inst = _norm_instance(instance_name)
    mode = (mode or "GLOBAL_POOL").strip().upper()

    if mode not in BOT_PROVIDER_OPTIONS:
        raise ValueError("Modo de proveedor inválido")

    return _set_app_setting(db, f"{BOT_PROVIDER_MODE_KEY_PREFIX}{inst}", mode)


def _is_personal_provider_mode(mode: str | None) -> bool:
    return (mode or "").strip().upper().startswith("PERSONAL:")


def _provider_from_mode(mode: str | None) -> str | None:
    mode = (mode or "").strip().upper()

    if ":" not in mode:
        return None

    _, provider_name = mode.split(":", 1)
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


def _request_is_no_accounting_main(db: Session, req) -> bool:
    instance_name = _norm_instance(getattr(req, "instance_name", None))
    provider_name = (getattr(req, "provider_name", "") or "").strip().upper()
    provider_group_id = (getattr(req, "provider_group_id", "") or "").strip()

    # Grupos especiales: siguen funcionando, pero no consumen
    # límite, contador de usadas ni promociones del bot.
    if _is_bot_hidden_no_accounting_group(
        instance_name,
        getattr(req, "source_group_id", None),
    ):
        return True

    # MAYAPROVIDER jamás debe consumir límite, promo ni contador,
    # aunque el bot cambie después de modo privado a global.
    if provider_name == "MAYAPROVIDER":
        return True

    # Seguridad extra: si el PDF vino desde los grupos privados de Maya,
    # también se considera privado aunque provider_name venga vacío/mal.
    if instance_name == "docifybot8maya" and provider_group_id in _maya_provider_group_ids():
        return True

    personal_provider = _personal_provider_filter_for_instance(db, instance_name)

    return bool(
        personal_provider
        and provider_name == personal_provider
    )


def _personal_provider_filter_for_instance(db: Session, instance_name: str):
    inst = _norm_instance(instance_name)

    # Las solicitudes hechas con MAYAPROVIDER son privadas SIEMPRE,
    # aunque después el bot vuelva a modo GLOBAL_POOL.
    if inst == "docifybot8maya":
        return "MAYAPROVIDER"

    return None

NO_DONE_NOTIFY_GROUPS = {
    "120363427267191472@g.us"
}

NO_EXTRA_TEXT_GROUPS = {
    "120363427267191472@g.us"
}

def should_notify_done(group_id: str | None) -> bool:
    if not group_id:
        return True
    return group_id not in NO_DONE_NOTIFY_GROUPS
    

def should_send_extra_text(group_id: str | None) -> bool:
    if not group_id:
        return True
    return group_id not in NO_EXTRA_TEXT_GROUPS


SLOW_REQUEST_PROVIDERS = {"PROVIDER4", "PROVIDER10", "PROVIDER11"}

WHATSAPP_TEXT_PROVIDERS = [
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
]

def _enqueue_process_request(req, reason: str = ""):
    provider = (getattr(req, "provider_name", None) or "").strip().upper()

    if provider in SLOW_REQUEST_PROVIDERS:
        queue_name = "actas_slow"
        queue = slow_request_queue
    else:
        queue_name = "actas"
        queue = request_queue

    job = queue.enqueue(process_request, req.id)

    print(
        "REQUEST_ENQUEUED_QUEUE =",
        {
            "request_id": req.id,
            "provider_name": provider,
            "queue": queue_name,
            "reason": reason,
            "job_id": job.id,
        },
        flush=True,
    )

    return job


def _evolution_headers():
    return {"apikey": EVOLUTION_APIKEY}


def _evolution_get(path: str, timeout: int = 8):
    url = f"{EVOLUTION_BASE_URL}{path}"
    r = requests.get(url, headers=_evolution_headers(), timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    return r.status_code, data


def _evolution_instance_state(instance_name: str) -> dict:
    """
    Estado de Evolution con cache corto para acelerar panel principal y mini panel.
    No depende de update_evolution_state_cache.py.
    Si no hay cache, consulta directo al Evolution nuevo y guarda por 25 segundos.
    """
    inst = (instance_name or "").strip()

    if not inst:
        return {
            "ok": False,
            "state": "unknown",
            "error": "empty_instance",
        }

    cache_key = f"panel:evolution_state:{inst}"

    cached = _cache_get_json(cache_key)
    if isinstance(cached, dict) and cached.get("state"):
        cached["cached"] = True
        return cached

    try:
        url = f"{EVOLUTION_BASE_URL}/instance/connectionState/{inst}"

        r = requests.get(
            url,
            headers={"apikey": EVOLUTION_APIKEY},
            timeout=2.5,
        )

        try:
            data = r.json()
        except Exception:
            data = {"raw": (r.text or "")[:300]}

        state = "unknown"

        if isinstance(data, dict):
            state = (
                data.get("instance", {}).get("state")
                or data.get("state")
                or data.get("connectionState")
                or data.get("status")
                or "unknown"
            )

        result = {
            "ok": r.status_code < 400,
            "state": str(state or "unknown").strip().lower(),
            "direct": True,
        }

        _cache_set_json(cache_key, result, ttl=25)
        return result

    except Exception as e:
        print("EVOLUTION_STATE_ERROR =", inst, repr(e), flush=True)

        result = {
            "ok": False,
            "state": "unknown",
            "error": str(e),
            "direct": True,
        }

        _cache_set_json(cache_key, result, ttl=8)
        return result

    
def _extract_qr_from_evolution_payload(payload) -> str:
    """
    Extrae QR/base64 aunque Evolution lo mande con estructura distinta.
    Soporta:
    - {"base64": "..."}
    - {"qrcode": {"base64": "..."}}
    - {"qrcode": "..."}
    - {"qr": "..."}
    - {"qrCode": "..."}
    - {"code": "..."}
    - {"instance": {"qrcode": "..."}}
    - listas con un dict adentro
    """
    if isinstance(payload, list):
        for item in payload:
            qr = _extract_qr_from_evolution_payload(item)
            if qr:
                return qr
        return ""

    if not isinstance(payload, dict):
        return ""

    candidates = []

    candidates.extend([
        payload.get("base64"),
        payload.get("qr"),
        payload.get("qrCode"),
        payload.get("code"),
        payload.get("pairingCode"),
    ])

    qrcode = payload.get("qrcode")
    if isinstance(qrcode, dict):
        candidates.extend([
            qrcode.get("base64"),
            qrcode.get("code"),
            qrcode.get("qr"),
            qrcode.get("qrCode"),
        ])
    elif isinstance(qrcode, str):
        candidates.append(qrcode)

    instance = payload.get("instance")
    if isinstance(instance, dict):
        candidates.extend([
            instance.get("qrcode"),
            instance.get("qr"),
            instance.get("qrCode"),
            instance.get("base64"),
        ])

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def _evolution_create_instance_if_needed(instance_name: str) -> dict:
    """
    Crea/recrea la instancia en Evolution si connect responde count:0
    o si la instancia no existe realmente en Evolution.
    """
    url = f"{EVOLUTION_BASE_URL}/instance/create"

    r = requests.post(
        url,
        headers={
            "apikey": EVOLUTION_APIKEY,
            "Content-Type": "application/json",
        },
        json={
            "instanceName": instance_name,
            "qrcode": True,
            "integration": "WHATSAPP-BAILEYS",
        },
        timeout=30,
    )

    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    print(
        "EVOLUTION_CREATE_FOR_QR =",
        instance_name,
        r.status_code,
        data,
        flush=True,
    )

    return {
        "ok": r.status_code in (200, 201, 403, 409),
        "status_code": r.status_code,
        "data": data,
    }


def _evolution_connect_qr(instance_name: str) -> dict:
    inst = (instance_name or "").strip()

    if not inst:
        return {
            "ok": False,
            "error": "EMPTY_INSTANCE",
        }

    def _connect_once() -> dict:
        url = f"{EVOLUTION_BASE_URL}/instance/connect/{inst}"

        r = requests.get(
            url,
            headers={"apikey": EVOLUTION_APIKEY},
            timeout=20,
        )

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        qr = _extract_qr_from_evolution_payload(data)

        print(
            "EVOLUTION_QR_DEBUG =",
            inst,
            r.status_code,
            data,
            "qr_found=",
            bool(qr),
            flush=True,
        )

        return {
            "http_ok": r.status_code < 400,
            "status_code": r.status_code,
            "data": data,
            "qr": qr,
        }

    try:
        first = _connect_once()

        # Caso exacto de tu pantalla: Evolution responde {"count": 0}
        # Tu código antes lo trataba como ok, pero realmente NO trae QR.
        first_data = first.get("data") or {}
        first_qr = first.get("qr") or ""

        no_qr = not first_qr
        count_zero = isinstance(first_data, dict) and int(first_data.get("count") or 0) == 0

        if no_qr and count_zero:
            created = _evolution_create_instance_if_needed(inst)

            if not created.get("ok"):
                return {
                    "ok": False,
                    "error": "EVOLUTION_CREATE_FAILED_BEFORE_QR",
                    "create_status_code": created.get("status_code"),
                    "create_response": created.get("data"),
                    "connect_response": first_data,
                }

            time.sleep(1.5)
            second = _connect_once()

            if second.get("qr"):
                return {
                    "ok": True,
                    "status_code": second.get("status_code"),
                    "qr": second.get("qr"),
                    "qr_image": second.get("qr"),
                    "data": second.get("data"),
                    "created_before_qr": True,
                }

            return {
                "ok": False,
                "error": "EVOLUTION_NO_QR_AFTER_CREATE",
                "connect_response": second.get("data"),
                "create_response": created.get("data"),
            }

        if first_qr:
            return {
                "ok": True,
                "status_code": first.get("status_code"),
                "qr": first_qr,
                "qr_image": first_qr,
                "data": first.get("data"),
                "created_before_qr": False,
            }

        return {
            "ok": False,
            "error": "EVOLUTION_NO_QR_RETURNED",
            "status_code": first.get("status_code"),
            "data": first_data,
        }

    except Exception as e:
        print("EVOLUTION_QR_ERROR =", inst, repr(e), flush=True)
        return {
            "ok": False,
            "error": str(e),
        }
        

def _bot_status_rows_uncached(db: Session) -> list[dict]:
    static_bots = set(BOT_LABELS.keys()) | set(BOT_PANEL_TOKENS.values())

    dynamic_rows = (
        db.query(BotControl)
        .filter(BotControl.is_active == True)
        .all()
    )

    dynamic_bots = {
        r.instance_name: r.label
        for r in dynamic_rows
        if r.instance_name not in static_bots
    }

    bots = sorted(static_bots | set(dynamic_bots.keys()))

    out = []

    for inst in bots:
        bc = (
            db.query(BotControl)
            .filter(BotControl.instance_name == inst)
            .first()
        )

        if bc and bc.is_active is False:
            continue

        q_total = (
            db.query(RequestLog)
            .filter(RequestLog.instance_name == inst)
        )

        hidden_no_accounting_groups = _bot_hidden_no_accounting_group_ids(
            inst
        )

        if hidden_no_accounting_groups:
            q_total = q_total.filter(
                or_(
                    RequestLog.source_group_id.is_(None),
                    RequestLog.source_group_id.notin_(
                        hidden_no_accounting_groups
                    ),
                )
            )
        
        personal_provider = _personal_provider_filter_for_instance(db, inst)
        if personal_provider:
            q_total = _exclude_private_provider_query(q_total, db, inst)
        
        total = q_total.count()

        used = get_bot_used(db, inst)
        
        limit_value = get_bot_limit(db, inst)
        blocked = is_instance_blocked(inst)
        # DESACTIVADO: no consultar Evolution por cada bot al cargar panel; saturaba Evolution y hacía lento webhook/sendText
        ev = _evolution_instance_state(inst)

        out.append({
            "instance_name": inst,
            "label": dynamic_bots.get(inst) or bot_label(inst),
            "state": ev.get("state", "unknown"),
            "blocked": blocked,
            "used": used,
            "limit": limit_value,
            "available": max(0, limit_value - used) if limit_value > 0 else None,
            "total_requests": total,
            "panel_token": (
                db.query(BotControl.panel_token)
                .filter(BotControl.instance_name == inst)
                .scalar()
                or next(
                    (tok for tok, bot_inst in BOT_PANEL_TOKENS.items() if bot_inst == inst),
                    None
                )
            )
        })

    return out


def _bot_status_rows(db: Session) -> list[dict]:
    """
    Cache corto para acelerar panel principal.
    Evita recalcular conteos por bot en cada refresh/cambio.
    TTL bajo para que bloqueos, recargas y usos se reflejen rápido.
    """
    cache_key = "panel:bot_status_rows:v3"

    cached = _cache_get_json(cache_key)
    if isinstance(cached, list):
        return cached

    rows = _bot_status_rows_uncached(db)

    try:
        _cache_set_json(cache_key, rows, ttl=5)
    except Exception:
        pass

    return rows

    
@app.get("/panel/instance/{instance_name}/qr")
def panel_instance_qr(
    instance_name: str,
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    inst = (instance_name or "").strip()

    if not inst:
        return {"ok": False, "error": "EMPTY_INSTANCE"}

    # Seguridad: solo permitir QR de bots registrados/visibles en panel.
    exists_static = inst in BOT_LABELS or inst in BOT_PANEL_TOKENS.values()

    exists_dynamic = (
        db.query(BotControl)
        .filter(
            BotControl.instance_name == inst,
            BotControl.is_active == True,
        )
        .first()
    )

    if not exists_static and not exists_dynamic:
        return {
            "ok": False,
            "error": "INSTANCE_NOT_REGISTERED_IN_PANEL",
            "instance_name": inst,
        }

    result = _evolution_connect_qr(inst)
    return result


@app.post("/panel/bots/{instance_name}/hide")
def panel_hide_bot(
    instance_name: str,
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False}

    row = (
        db.query(BotControl)
        .filter(BotControl.instance_name == instance_name)
        .first()
    )

    if not row:
        row = BotControl(
            instance_name=instance_name,
            is_active=False,
        )
        db.add(row)
    else:
        row.is_active = False

    db.commit()
    _clear_panel_cache()

    return {"ok": True}


@app.post("/panel/bots/{instance_name}/disconnect")
def panel_disconnect_bot(
    instance_name: str,
    token: str = "",
):
    if token != PANEL_TOKEN:
        return {"ok": False}

    try:
        url = f"{EVOLUTION_BASE_URL}/instance/logout/{instance_name}"

        r = requests.delete(
            url,
            headers={"apikey": EVOLUTION_APIKEY},
            timeout=20,
        )

        _clear_panel_cache()

        return {"ok": r.status_code in (200, 201)}

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/panel/bots/create")
def panel_create_bot(
    payload: dict = Body(...),
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    label = (payload.get("label") or "").strip()
    instance_name = (payload.get("instance_name") or "").strip()

    if not label or not instance_name:
        return {"ok": False, "error": "FALTAN_DATOS"}

    static_bots = set(BOT_LABELS.keys()) | set(BOT_PANEL_TOKENS.values())

    hidden_static = {
        r.instance_name
        for r in (
            db.query(BotControl.instance_name)
            .filter(
                BotControl.instance_name.in_(static_bots),
                BotControl.is_active == False,
            )
            .all()
        )
    }
    
    visible_static_count = len(static_bots - hidden_static)
    
    active_dynamic = (
        db.query(BotControl)
        .filter(
            BotControl.is_active == True,
            ~BotControl.instance_name.in_(static_bots),
        )
        .count()
    )
    
    total = visible_static_count + active_dynamic

    if total >= 30:
        return {"ok": False, "error": "MAX_30_BOTS"}

    exists_static = instance_name in BOT_LABELS or instance_name in BOT_PANEL_TOKENS.values()

    exists_dynamic = (
        db.query(BotControl)
        .filter(BotControl.instance_name == instance_name)
        .first()
    )

    if exists_static or exists_dynamic:
        return {"ok": False, "error": "YA_EXISTE"}

    new_token = secrets.token_hex(5)

    evolution_create_ok = False
    webhook_ok = False

    try:
        create_url = f"{EVOLUTION_BASE_URL}/instance/create"

        r1 = requests.post(
            create_url,
            headers={
                "apikey": EVOLUTION_APIKEY,
                "Content-Type": "application/json",
            },
            json={
                "instanceName": instance_name,
                "qrcode": True,
                "integration": "WHATSAPP-BAILEYS",
            },
            timeout=30,
        )

        print("CREATE_EVOLUTION_INSTANCE:", r1.status_code, r1.text[:500], flush=True)

        if r1.status_code not in (200, 201, 403, 409):
            return {
                "ok": False,
                "error": "EVOLUTION_CREATE_FAILED",
                "status_code": r1.status_code,
                "response": r1.text[:500],
            }

        evolution_create_ok = True

        webhook_url = f"{EVOLUTION_BASE_URL}/webhook/set/{instance_name}"

        r2 = requests.post(
            webhook_url,
            headers={
                "apikey": EVOLUTION_APIKEY,
                "Content-Type": "application/json",
            },
            json={
                "webhook": {
                    "url": "http://187.127.248.94:8000/webhook/evolution",
                    "enabled": True,
                    "webhook_by_events": False,
                    "events": [
                        "MESSAGES_UPSERT",
                    ],
                }
            },
            timeout=30,
        )

        print("SET_EVOLUTION_WEBHOOK:", r2.status_code, r2.text[:500], flush=True)

        if r2.status_code not in (200, 201):
            return {
                "ok": False,
                "error": "WEBHOOK_SET_FAILED",
                "status_code": r2.status_code,
                "response": r2.text[:500],
            }

        webhook_ok = True

    except Exception as e:
        print("CREATE_BOT_EVOLUTION_ERROR:", str(e), flush=True)
        return {"ok": False, "error": "EVOLUTION_EXCEPTION", "detail": str(e)}

    row = BotControl(
        instance_name=instance_name,
        label=label,
        panel_token=new_token,
        limit=0,
        used=0,
        recharges=0,
        is_blocked=False,
        is_active=True,
    )

    db.add(row)
    db.commit()

    _clear_panel_cache()

    return {
        "ok": True,
        "token": new_token,
        "evolution_create_ok": evolution_create_ok,
        "webhook_ok": webhook_ok,
    }


def _is_valid_admin_panel_token(request: Request) -> bool:
    token = (request.query_params.get("token") or "").strip()
    expected = (settings.ADMIN_PANEL_TOKEN or "").strip()
    return bool(expected) and token == expected


def _bot_instance_from_token(db: Session, token: str) -> str | None:
    token = (token or "").strip()

    row = (
        db.query(BotControl)
        .filter(
            BotControl.panel_token == token,
            BotControl.is_active == True,
        )
        .first()
    )

    if row:
        return row.instance_name

    return BOT_PANEL_TOKENS.get(token)


def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mx_now():
    return datetime.now(ZoneInfo(PANEL_TZ))


def _to_panel_tz(dt):
    if not dt:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(ZoneInfo(PANEL_TZ))


def _panel_to_utc_naive(dt):
    if not dt:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(PANEL_TZ))

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


DAYS_ES = {
    0: "LUNES",
    1: "MARTES",
    2: "MIÉRCOLES",
    3: "JUEVES",
    4: "VIERNES",
    5: "SÁBADO",
    6: "DOMINGO",
}


PROVIDER_LABELS = {
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
    "PROVIDER14": "E-BOT",
    "MAYAPROVIDER": "PROVEEDOR DE MAYA",
}


BOT_LABELS = {
    "docifybot8": "🚀 DOCU EXPRES",
    "docifybot8max": "☄️ MAX BOT",
    "docifybot8docify": "👽 DOCIFY MX",
    "docifybot8maya": "🔱 GESTORIA MAYA",
    "docifybot8leli": "🌼 TRAMITES LELI",
    "docifybot8rywya": "🌹 GESTORIA EXPRESS RYWYA",
    "docifybot8xpress": "⚡ DIGITAL XPRESS",
    "docifybot8moon": "🌙 MOON MX",
    "docifybot8trami": "🌪️ TRAMI-EXPRESS",
}


BOT_PANEL_TOKENS = {
    "t777fgh6j5": "docifybot8",
    "4a8c92a1e7": "docifybot8max",
    "asd5a6d7g9": "docifybot8docify",
    "as5613f4se": "docifybot8maya",
    "65as6d8fg9": "docifybot8leli",
    "dg5f5f6g3s": "docifybot8rywya",
    "df48r8dg62": "docifybot8xpress",
    "7f56d65g9d": "docifybot8moon",
    "fg9er5b6g6": "docifybot8trami",
}


BOT_MANAGER_NAME_KEY_PREFIX = "BOT_MANAGER_NAME:"
BOT_MANAGER_PRICE_KEY_PREFIX = "BOT_MANAGER_PRICE:"

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
    

MIN_BOT_PROMO_ACTAS = 10


def hide_group_from_main_panel(db: Session, group_jid: str):
    row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()

    if row:
        row.hidden_in_main = True
    else:
        row = AuthorizedGroup(
            group_jid=group_jid,
            hidden_in_main=True,
        )
        db.add(row)

    db.commit()


def hide_group_from_bot_panel(db: Session, group_jid: str, instance_name: str):
    row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()
    if row and (row.owner_instance or "").strip() == (instance_name or "").strip():
        row.owner_instance = None
    db.commit()
    

def _is_child_bot(instance_name: str) -> bool:
    inst = (instance_name or "").strip().lower()
    return inst.startswith("docifybot") and inst != "docifybot"


def _bot_title(db: Session, instance_name: str) -> str:
    name = BOT_LABELS.get(instance_name)

    if name:
        return name

    row = (
        db.query(BotControl)
        .filter(BotControl.instance_name == instance_name)
        .first()
    )

    if row and row.label:
        return row.label

    return instance_name


def _ensure_group_owner(db: Session, group_jid: str | None, instance_name: str | None):
    if not group_jid or not instance_name or not _is_child_bot(instance_name):
        return

    row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()
    if row and not (row.owner_instance or "").strip():
        row.owner_instance = instance_name
        db.commit()


def _assert_group_owned_by_bot(db: Session, group_jid: str, instance_name: str):
    row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()
    if not row:
        raise ValueError("Grupo no encontrado")
    if (row.owner_instance or "").strip() != (instance_name or "").strip():
        raise ValueError("Este grupo no pertenece a este bot")
    return row


def _should_ignore_group_for_instance(db: Session, group_jid: str | None, instance_name: str | None) -> tuple[bool, str]:
    if not group_jid:
        return False, ""

    row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()

    if not row:
        return True, "unauthorized_group"

    owner = (row.owner_instance or "").strip()
    current_instance = (instance_name or "").strip()

    if owner and current_instance and owner != current_instance:
        return True, "group_owner_mismatch"

    return False, ""


def _get_bot_group_name(db: Session, group_jid: str) -> str:
    alias = db.query(GroupAlias).filter_by(group_jid=group_jid).first()
    if alias and (alias.custom_name or "").strip():
        return alias.custom_name.strip()
    return _group_name(group_jid)


def _exclude_private_provider_query(q, db: Session, instance_name: str):
    inst = _norm_instance(instance_name)
    personal_provider = _personal_provider_filter_for_instance(db, instance_name)

    private_conditions = []

    if personal_provider:
        private_conditions.append(
            func.coalesce(RequestLog.provider_name, "") == personal_provider
        )

    if inst == "docifybot8maya":
        maya_groups = _maya_provider_group_ids()
        if maya_groups:
            private_conditions.append(
                and_(
                    RequestLog.provider_group_id.isnot(None),
                    RequestLog.provider_group_id.in_(maya_groups),
                )
            )

    if private_conditions:
        q = q.filter(~or_(*private_conditions))

    return q


def _owned_group_ids_for_instance(db: Session, instance_name: str) -> list[str]:
    inst = (instance_name or "").strip()

    rows = (
        db.query(AuthorizedGroup.group_jid)
        .filter(
            AuthorizedGroup.owner_instance == inst,
            AuthorizedGroup.is_hidden == False,
        )
        .all()
    )

    hidden_no_accounting_groups = _bot_hidden_no_accounting_group_ids(
        instance_name
    )

    return [
        gid
        for (gid,) in rows
        if gid and gid not in hidden_no_accounting_groups
    ]


def _bot_groups_for_instance(db: Session, instance_name: str):
    owned_group_ids = _owned_group_ids_for_instance(db, instance_name)

    groups = []
    for group_jid in owned_group_ids:
        groups.append(SimpleNamespace(group_jid=group_jid))

    groups.sort(key=lambda x: (_get_bot_group_name(db, x.group_jid) or "").lower())
    return groups


def _bot_day_bounds():
    now_local = _mx_now()
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return _panel_to_utc_naive(start_local), _panel_to_utc_naive(end_local)


def _bot_30d_bounds():
    now_local = _mx_now()
    start_local = now_local - timedelta(days=29)
    start_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)
    return _panel_to_utc_naive(start_local), _panel_to_utc_naive(end_local)


def _bot_current_month_bounds():
    now_local = _mx_now()
    start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if start_local.month == 12:
        end_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        end_local = start_local.replace(month=start_local.month + 1)

    return _panel_to_utc_naive(start_local), _panel_to_utc_naive(end_local)


def _bot_prev_month_bounds():
    now_local = _mx_now()
    first_this_month = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    return _panel_to_utc_naive(first_prev_month), _panel_to_utc_naive(first_this_month)


def _bot_sales_today(db: Session, instance_name: str) -> int:
    start_utc, end_utc = _bot_day_bounds()
    owned_group_ids = _owned_group_ids_for_instance(db, instance_name)

    if not owned_group_ids:
        return 0

    q = (
        db.query(RequestLog)
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.source_group_id.in_(owned_group_ids),
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
            RequestLog.created_at < end_utc,
        )
    )

    q = _exclude_private_provider_query(q, db, instance_name)

    return q.count()


def _bot_sales_30d(db: Session, instance_name: str) -> int:
    start_utc, end_utc = _bot_30d_bounds()
    owned_group_ids = _owned_group_ids_for_instance(db, instance_name)

    if not owned_group_ids:
        return 0

    q = (
        db.query(RequestLog)
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.source_group_id.in_(owned_group_ids),
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
            RequestLog.created_at < end_utc,
        )
    )

    q = _exclude_private_provider_query(q, db, instance_name)

    return q.count()


def _bot_sales_history_30d(db: Session, instance_name: str):
    start_utc, end_utc = _bot_30d_bounds()
    owned_group_ids = _owned_group_ids_for_instance(db, instance_name)

    if not owned_group_ids:
        return []

    mx_date = func.date(
        func.timezone(
            'America/Monterrey',
            func.timezone('UTC', RequestLog.created_at)
        )
    )

    q = (
        db.query(
            mx_date.label("day"),
            func.count(RequestLog.id).label("total"),
        )
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.source_group_id.in_(owned_group_ids),
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
            RequestLog.created_at < end_utc,
        )
    )

    q = _exclude_private_provider_query(q, db, instance_name)

    rows = (
        q.group_by(mx_date)
        .order_by(mx_date.desc())
        .all()
    )

    return rows

def _bot_group_stats(db: Session, instance_name: str):
    start_day, end_day = _bot_day_bounds()
    start_30d, end_30d = _bot_30d_bounds()
    start_month, end_month = _bot_current_month_bounds()
    start_prev_month, end_prev_month = _bot_prev_month_bounds()

    groups = _bot_groups_for_instance(db, instance_name)
    if not groups:
        return []

    group_ids = [g.group_jid for g in groups if getattr(g, "group_jid", None)]
    if not group_ids:
        return []

    def _count_map(start_utc, end_utc):
        q = (
            db.query(
                RequestLog.source_group_id,
                func.count(RequestLog.id).label("total"),
            )
            .filter(
                RequestLog.instance_name == instance_name,
                RequestLog.source_group_id.in_(group_ids),
                RequestLog.status == "DONE",
                RequestLog.created_at >= start_utc,
                RequestLog.created_at < end_utc,
            )
        )

        q = _exclude_private_provider_query(q, db, instance_name)

        rows = (
            q.group_by(RequestLog.source_group_id)
            .all()
        )

        return {
            row.source_group_id: int(row.total or 0)
            for row in rows
        }

    today_map = _count_map(start_day, end_day)
    d30_map = _count_map(start_30d, end_30d)
    month_map = _count_map(start_month, end_month)
    prev_month_map = _count_map(start_prev_month, end_prev_month)

    # Cargar promociones de todos los grupos en una sola consulta.
    promo_rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid.in_(group_ids))
        .order_by(GroupPromotion.updated_at.desc(), GroupPromotion.id.desc())
        .all()
    )

    promo_by_group = {}
    for promo in promo_rows:
        gid = promo.group_jid
        if gid and gid not in promo_by_group:
            promo_by_group[gid] = promo

    # Cargar alias de todos los grupos en una sola consulta.
    alias_rows = (
        db.query(GroupAlias.group_jid, GroupAlias.custom_name)
        .filter(GroupAlias.group_jid.in_(group_ids))
        .all()
    )

    alias_by_group = {
        row.group_jid: row.custom_name
        for row in alias_rows
        if row.group_jid and row.custom_name
    }

    blocked_set = set(list_blocked_groups())

    out = []
    for g in groups:
        gid = g.group_jid

        # Evita _get_bot_group_name(db, gid) aquí porque hacía consulta por grupo.
        group_name = (
            alias_by_group.get(gid)
            or getattr(g, "group_name", None)
            or getattr(g, "name", None)
            or gid
        )

        promo = promo_by_group.get(gid)

        out.append({
            "group_jid": gid,
            "group_name": group_name,
            "today_done": today_map.get(gid, 0),
            "done_30d": d30_map.get(gid, 0),
            "month_done": month_map.get(gid, 0),
            "prev_month_done": prev_month_map.get(gid, 0),
            "blocked": gid in blocked_set,
            "promo_total": int(promo.total_actas or 0) if promo else 0,
            "promo_used": int(promo.used_actas or 0) if promo else 0,
            "promo_active": bool(promo.is_active) if promo else False,
        })

    out.sort(key=lambda x: (-x["done_30d"], x["group_name"].lower()))
    return out



def bot_label(inst, db: Session = None):
    if not inst:
        return ""

    inst = inst.strip()

    static_name = BOT_LABELS.get(inst.lower())
    if static_name:
        return static_name

    if db:
        row = (
            db.query(BotControl)
            .filter(func.lower(BotControl.instance_name) == inst.lower())
            .first()
        )

        if row and row.label:
            return row.label

    return inst


BOT_PRIVATE_NOTIFY_KEY_PREFIX = "BOT_PRIVATE_NOTIFY_JID:"


def _normalize_private_wa_jid(value: str | None) -> str:
    raw = (value or "").strip()

    if not raw:
        return ""

    # Si ya viene como JID, lo dejamos.
    if raw.endswith("@s.whatsapp.net"):
        return raw

    # Por compatibilidad si alguien pega @c.us
    if raw.endswith("@c.us"):
        return raw.replace("@c.us", "@s.whatsapp.net")

    digits = re.sub(r"\D", "", raw)

    if not digits:
        return ""

    # México: si capturas solo 10 dígitos, le agrega 52.
    # Ejemplo: 8991234567 -> 528991234567@s.whatsapp.net
    if len(digits) == 10:
        digits = "52" + digits

    if len(digits) < 12 or len(digits) > 15:
        return ""

    return f"{digits}@s.whatsapp.net"


def _bot_private_notify_jid(db: Session, instance_name: str) -> str:
    inst = (instance_name or "").strip()
    if not inst:
        return ""

    return _get_app_setting(
        db,
        f"{BOT_PRIVATE_NOTIFY_KEY_PREFIX}{inst}",
        "",
    )


def _set_bot_private_notify_jid(db: Session, instance_name: str, jid: str):
    inst = (instance_name or "").strip()

    if not inst:
        raise ValueError("Instancia vacía")

    return _set_app_setting(
        db,
        f"{BOT_PRIVATE_NOTIFY_KEY_PREFIX}{inst}",
        jid,
    )


def _internal_bots_for_private_broadcast(db: Session) -> list[dict]:
    """
    Lista bots internos visibles: estáticos + dinámicos BotControl.
    Excluye MAIN_PANEL_INSTANCE para que el bot principal no se avise a sí mismo.
    """
    static_bots = set(BOT_LABELS.keys()) | set(BOT_PANEL_TOKENS.values())

    dynamic_rows = (
        db.query(BotControl)
        .filter(BotControl.is_active == True)
        .all()
    )

    dynamic_bots = {
        (r.instance_name or "").strip(): (r.label or "").strip()
        for r in dynamic_rows
        if (r.instance_name or "").strip()
    }

    all_instances = sorted(static_bots | set(dynamic_bots.keys()))

    out = []

    for inst in all_instances:
        inst = (inst or "").strip()

        if not inst:
            continue

        # "extra registrado": no mandarse al bot principal.
        if inst == MAIN_PANEL_INSTANCE:
            continue

        # Si está oculto/desactivado en BotControl, no lo mostramos.
        bc = (
            db.query(BotControl)
            .filter(BotControl.instance_name == inst)
            .first()
        )

        if bc and bc.is_active is False:
            continue

        jid = _bot_private_notify_jid(db, inst)

        out.append({
            "instance_name": inst,
            "label": dynamic_bots.get(inst) or bot_label(inst, db),
            "jid": jid,
            "configured": bool(jid),
        })

    return out


def _provider_label(name: str) -> str:
    if not name:
        return ""
    return PROVIDER_LABELS.get(name, name)


def _panel_period_label(view: str, time_min=None, time_max=None) -> str:
    view = (view or "day").strip().lower()

    if view == "custom" and time_min and time_max:
        start_local = _to_panel_tz(time_min)
        end_local = _to_panel_tz(time_max)

        if start_local and end_local:
            end_show = end_local - timedelta(days=1)
            return (
                f"Rango personalizado: "
                f"{start_local.strftime('%Y-%m-%d')} a {end_show.strftime('%Y-%m-%d')}"
            )

    return {
        "day": "Hoy",
        "30d": "Últimos 30 días",
        "month": "Mes actual",
        "prev_month": "Mes anterior",
        "custom": "Rango personalizado",
    }.get(view, view)


def _panel_error_bucket(error_message: str | None, status: str | None = None) -> str:
    raw = (error_message or "").strip()
    up = raw.upper()

    if (status or "").upper() in {"QUEUED", "PROCESSING"}:
        return "Pendiente / en proceso"

    if not raw:
        return "Error sin detalle"

    # Código corto para errores tipo PROVIDER4_NO_RECORD:CURP
    code = up

    if ":" in code:
        code = code.split(":", 1)[0].strip()

    if " | " in code:
        code = code.split(" | ", 1)[0].strip()

    code = re.sub(r"^PROVIDER(?:10|11|12|13|14|[1-9])_", "", code)
    code = re.sub(r"^MAYAPROVIDER_", "", code)

    # ==========================================================
    # 1) Sin proveedor / sesión / configuración
    # ==========================================================

    if "NO_PROVIDER_FOR_SPECIAL_FORMAT" in up:
        return "Formato especial sin proveedor compatible"

    if "NO_PROVIDER_ENABLED" in up:
        return "Sin proveedor habilitado / sesión caída"

    if (
        "SESSION_INVALID_OR_EXPIRED" in up
        or "SID CAIDO" in up
        or "SID CAÍDO" in up
        or "NO AUTORIZADO" in up
        or '"ERROR":"NO AUTORIZADO"' in up
        or "NO AUTORIZADO" in code
    ):
        return "Sesión/SID del proveedor caído"

    if "NOT_CONFIGURED" in code or "GROUPS_NOT_CONFIGURED" in code:
        return "Falta configuración del proveedor/grupo"

    if "DISABLED_BEFORE_PROCESSING" in up:
        return "Proveedor deshabilitado antes de procesar"

    # ==========================================================
    # 2) Sin registro / no localizado
    # ==========================================================

    if (
        "SIN REGISTRO" in up
        or "SIN_REGISTRO" in up
        or "NO_LOCALIZADO" in up
        or "NO LOCALIZADO" in up
        or "NO_REGISTRO" in up
        or "NO REGISTRO" in up
        or "NO HAY REGISTRO" in up
        or "NO HAY REGISTROS" in up
        or "NO_RECORD" in up
        or "NO RECORD" in up
        or "ACTA NO LOCALIZADA" in up
    ):
        return "Sin registro en sistema"

    has_identifier = bool(
        re.search(r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d", up)
    )

    if has_identifier and (
        " SIN" in up
        or "SSIN" in up
        or "SINI" in up
        or " SIN " in up
    ):
        return "Sin registro en sistema"

    # ==========================================================
    # 3) Acta/PDF incorrecto o duplicado
    # ==========================================================

    if "WRONG_ACT_TYPE" in up or "PDF DE OTRO TIPO" in up:
        return "PDF de otro tipo de acta"

    if "WRONG_CURP_IN_PDF" in up or "NO CORRESPONDE" in up:
        return "PDF no corresponde al dato solicitado"

    if "WRONG_ELECTRONIC" in up:
        return "PDF no corresponde a cadena/folio"

    if (
        "TRAMITEEXISTENTE" in up
        or "TRAMITE EXISTENTE" in up
        or "DUPLICADO_IGNORADO" in up
        or "DUPLIC" in up
    ):
        return "Duplicada / trámite existente"

    if "ACT_TYPE_NOT_ALLOWED" in code:
        return "Tipo de acta no permitido para proveedor"

    # ==========================================================
    # 4) Timeout / auto-cierre
    # ==========================================================

    if (
        "TIMEOUT" in up
        or "TIMED OUT" in up
        or "READ TIMED OUT" in up
        or "AUTO-CIERRE" in up
        or "AUTO CIERRE" in up
        or "TASK EXCEEDED MAXIMUM TIMEOUT" in up
        or "ESPERA MAYOR" in up
    ):
        return "Timeout / auto-cierre por espera"

    # ==========================================================
    # 5) Falla WhatsApp / Evolution local
    # ==========================================================

    if (
        "SENDMEDIA" in up
        or "SENDTEXT" in up
        or "MESSAGE/SENDMEDIA" in up
        or "MESSAGE/SENDTEXT" in up
        or "127.0.0.1" in up
        or "PORT=8080" in up
        or "CONNECTION CLOSED" in up
        or "SERVICE-UNAVAILABLE" in up
        or "PDF_SEND_FAILED" in up
        or "DELIVERY_FAILED" in up
        or "SEND_FAILED" in up
        or "PRISMACLIENTKNOWNREQUESTERROR" in up
    ):
        return "Falla de envío / entrega WhatsApp"

    # ==========================================================
    # 6) Respuesta web/proveedor incompleta
    # ==========================================================

    if "NO_PDF" in code or "NO PDF" in up:
        return "Proveedor no devolvió PDF"

    if "EMPTY_OR_USELESS_HTML" in up:
        return "Respuesta vacía/inútil del proveedor web"

    if "FINAL_PDF_INCOMPLETE" in up:
        return "PDF final incompleto"

    if "PDF_NOT_READABLE" in up:
        return "PDF no legible"

    if "NEW_VERIFICAR_UNKNOWN_RESPONSE" in up:
        return "Respuesta desconocida al verificar PDF"

    if "NEW_PETICION_UNKNOWN_RESPONSE" in up:
        return "Respuesta desconocida al crear petición"

    if "NEW_API_CHAIN_NOT_SUPPORTED_YET" in up:
        return "Cadena no soportada por API nueva"

    # ==========================================================
    # 7) Fallbacks
    # ==========================================================

    if "FALLBACK_NO_PROVIDER3" in up:
        return "Fallback sin Austram / Provider3 disponible"

    if "FALLBACK_NO_PROVIDER_AVAILABLE" in up:
        return "Falló y no hubo proveedor de respaldo"

    if "FAILED_FALLBACK_TO_" in up:
        return "Falló proveedor inicial y pasó a fallback"

    # ==========================================================
    # 8) Errores de conexión externa del proveedor web
    # ==========================================================

    if (
        "403 CLIENT ERROR" in up
        or "500 SERVER ERROR" in up
        or "503 SERVER ERROR" in up
        or "CONNECTION ABORTED" in up
        or "CONNECTIONRESETERROR" in up
        or "REMOTEDISCONNECTED" in up
        or "HTTPSCONNECTIONPOOL" in up
        or "MAX RETRIES EXCEEDED" in up
        or "CONNECTION RESET BY PEER" in up
        or "REMOTE END CLOSED CONNECTION" in up
    ):
        return "Error de conexión / servidor del proveedor"

    # ==========================================================
    # 9) Bugs internos
    # ==========================================================

    if "'PDF_BYTES'" in up or "PDF_BYTES" in up:
        return "Bug interno: pdf_bytes faltante"

    if "LOCAL VARIABLE 'FILENAME'" in up:
        return "Bug interno: filename no inicializado"

    if "CANNOT ACCESS LOCAL VARIABLE" in up:
        return "Bug interno: variable local no inicializada"

    # ==========================================================

    if "CUENTA_INEXISTENTE" in up:
        return "Cuenta del proveedor inexistente/inválida"
    
    if "FRAME_FAILED" in up:
        return "Falla en marco/consulta secundaria del proveedor"
    
    if "FOLIO_DOWNLOAD_FAILED" in up:
        return "Falló descarga de folio del proveedor"

    # ==========================================================
    # 10) Inválidos / falla general
    # ==========================================================

    if "INVALID" in code or "CURP INVALIDA" in up or "CURP INVÁLIDA" in up:
        return "Dato o respuesta inválida"

    if "FAILED" in code or "FAILED" in up:
        return "Falla general del proveedor/sistema"

    return code[:100] if code else "Error no clasificado"


def _provider_accounting_query(
    db: Session,
    *,
    time_min,
    time_max,
    group_jid: str = "",
    provider_name: str = "",
    act_type: str = "",
):
    return _query_requests_for_panel(
        db=db,
        time_min=time_min,
        time_max=time_max,
        group_jid=group_jid or None,
        provider_name=provider_name or None,
        status=None,
        act_type=act_type or None,
    )


def _provider_accounting_data(
    db: Session,
    *,
    time_min,
    time_max,
    group_jid: str = "",
    provider_name: str = "",
    act_type: str = "",
):
    q = _provider_accounting_query(
        db,
        time_min=time_min,
        time_max=time_max,
        group_jid=group_jid,
        provider_name=provider_name,
        act_type=act_type,
    )

    err_txt = func.upper(func.coalesce(RequestLog.error_message, ""))

    sin_registro_cond = and_(
        RequestLog.status == "ERROR",
        ~err_txt.ilike("NO_PROVIDER_ENABLED%"),
        ~err_txt.ilike("%SESSION_INVALID_OR_EXPIRED%"),
        ~err_txt.ilike("%SID CAIDO%"),
        ~err_txt.ilike("%SID CAÍDO%"),
        ~err_txt.ilike("%NO AUTORIZADO%"),
        or_(
            err_txt.ilike("%SIN REGISTRO%"),
            err_txt.ilike("%SIN_REGISTRO%"),
            err_txt.ilike("%NO_LOCALIZADO%"),
            err_txt.ilike("%NO LOCALIZADO%"),
            err_txt.ilike("%NO_RECORD%"),
            err_txt.ilike("%NO RECORD%"),
            err_txt.ilike("%NO_REGISTRO%"),
            err_txt.ilike("%NO REGISTRO%"),
            err_txt.ilike("%NO HAY REGISTRO%"),
            err_txt.ilike("%NO HAY REGISTROS%"),
            err_txt.ilike("%ACTA NO LOCALIZADA%"),
    
            # Respuestas mal escritas de proveedores WhatsApp:
            err_txt.op("~")(
                "[A-Z]{4}[0-9]{6}[HM][A-Z]{5}[A-Z0-9][0-9].*[[:space:]]SIN([[:space:][:punct:]]|$)"
            ),
            err_txt.op("~")(
                "[A-Z]{4}[0-9]{6}[HM][A-Z]{5}[A-Z0-9][0-9].*SSIN"
            ),
            err_txt.op("~")(
                "[A-Z]{4}[0-9]{6}[HM][A-Z]{5}[A-Z0-9][0-9].*SINI"
            ),
        ),
    )

    acta_erronea_cond = and_(
        RequestLog.status == "ERROR",
        or_(
            err_txt.ilike("%WRONG_ACT_TYPE%"),
            err_txt.ilike("%WRONG ACT TYPE%"),
            err_txt.ilike("%WRONG_CURP_IN_PDF%"),
            err_txt.ilike("%WRONG ELECTRONIC%"),
            err_txt.ilike("%WRONG_ELECTRONIC%"),
            err_txt.ilike("%PDF_PENDING_RETRY%"),
            err_txt.ilike("%TRAMITEEXISTENTE%"),
            err_txt.ilike("%TRAMITE EXISTENTE%"),
            err_txt.ilike("%DUPLIC%"),
            err_txt.ilike("%PDF DE OTRO TIPO%"),
            err_txt.ilike("%NO CORRESPONDE%"),
        ),
    )

    provider_control_raw = (
        q.with_entities(
            RequestLog.provider_name.label("provider_name"),
            func.count(RequestLog.id).label("total_solicitudes"),
            func.sum(case((RequestLog.status == "DONE", 1), else_=0)).label("total_exito"),
            func.sum(case((sin_registro_cond, 1), else_=0)).label("sin_registro"),
            func.sum(case((acta_erronea_cond, 1), else_=0)).label("actas_erroneas"),
            func.sum(case((RequestLog.status.in_(["QUEUED", "PROCESSING"]), 1), else_=0)).label("pendientes"),
            func.sum(
                case(
                    (
                        and_(
                            RequestLog.status == "ERROR",
                            ~sin_registro_cond,
                            ~acta_erronea_cond,
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("otros_errores"),
        )
        .group_by(RequestLog.provider_name)
        .all()
    )

    provider_control_rows = []
    provider_control_totals = {
        "total_exito": 0,
        "sin_registro": 0,
        "actas_erroneas": 0,
        "otros_errores": 0,
        "pendientes": 0,
        "total_solicitudes": 0,
    }

    for row in provider_control_raw:
        item = {
            "provider_name": row.provider_name or "NO IDENTIFICADO",
            "total_exito": int(row.total_exito or 0),
            "sin_registro": int(row.sin_registro or 0),
            "actas_erroneas": int(row.actas_erroneas or 0),
            "otros_errores": int(row.otros_errores or 0),
            "pendientes": int(row.pendientes or 0),
            "total_solicitudes": int(row.total_solicitudes or 0),
        }

        for k in provider_control_totals:
            provider_control_totals[k] += item[k]

        provider_control_rows.append(item)

    provider_control_rows.sort(
        key=lambda x: (-x["total_solicitudes"], x["provider_name"])
    )

    error_detail_raw = (
        q.with_entities(
            RequestLog.provider_name.label("provider_name"),
            RequestLog.status.label("status"),
            RequestLog.error_message.label("error_message"),
            func.count(RequestLog.id).label("total"),
        )
        .filter(RequestLog.status == "ERROR")
        .group_by(
            RequestLog.provider_name,
            RequestLog.status,
            RequestLog.error_message,
        )
        .all()
    )

    error_bucket_map = {}

    for row in error_detail_raw:
        provider = row.provider_name or "NO IDENTIFICADO"
        bucket = _panel_error_bucket(row.error_message, row.status)
        key = (provider, bucket)

        if key not in error_bucket_map:
            error_bucket_map[key] = {
                "provider_name": provider,
                "error_type": bucket,
                "total": 0,
                "examples": [],
                "items": [],
            }

        error_bucket_map[key]["total"] += int(row.total or 0)

        example = (row.error_message or "").strip()
        if example and len(error_bucket_map[key]["examples"]) < 2:
            error_bucket_map[key]["examples"].append(example[:220])

    # Detalle real de cada solicitud con error:
    # sirve para comparar contra WhatsApp: ID, CURP/dato, tipo, bot, grupo, solicitante y error técnico.
    error_items_raw = (
        q.filter(RequestLog.status == "ERROR")
        .order_by(RequestLog.created_at.desc(), RequestLog.id.desc())
        .all()
    )

    for req in error_items_raw:
        provider = req.provider_name or "NO IDENTIFICADO"
        bucket = _panel_error_bucket(req.error_message, req.status)
        key = (provider, bucket)

        if key not in error_bucket_map:
            error_bucket_map[key] = {
                "provider_name": provider,
                "error_type": bucket,
                "total": 0,
                "examples": [],
                "items": [],
            }

        created_local = _to_panel_tz(req.created_at)
        created_txt = created_local.strftime("%Y-%m-%d %H:%M:%S") if created_local else ""

        error_bucket_map[key]["items"].append({
            "id": req.id,
            "created_at": created_txt,
            "curp": getattr(req, "curp", "") or "",
            "act_type": getattr(req, "act_type", "") or "",
            "instance_name": getattr(req, "instance_name", "") or MAIN_PANEL_INSTANCE,
            "source_group_id": getattr(req, "source_group_id", "") or "",
            "requester_wa_id": getattr(req, "requester_wa_id", "") or "",
            "error_message": getattr(req, "error_message", "") or "",
        })

    error_detail_rows = list(error_bucket_map.values())
    error_detail_rows.sort(
        key=lambda x: (-x["total"], x["provider_name"], x["error_type"])
    )

    provider_bot_raw = (
        q.with_entities(
            RequestLog.provider_name.label("provider_name"),
            RequestLog.instance_name.label("instance_name"),
            func.count(RequestLog.id).label("total_solicitudes"),
            func.sum(case((RequestLog.status == "DONE", 1), else_=0)).label("total_exito"),
            func.sum(case((sin_registro_cond, 1), else_=0)).label("sin_registro"),
            func.sum(case((acta_erronea_cond, 1), else_=0)).label("actas_erroneas"),
            func.sum(case((RequestLog.status.in_(["QUEUED", "PROCESSING"]), 1), else_=0)).label("pendientes"),
            func.sum(
                case(
                    (
                        and_(
                            RequestLog.status == "ERROR",
                            ~sin_registro_cond,
                            ~acta_erronea_cond,
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("otros_errores"),
        )
        .group_by(RequestLog.provider_name, RequestLog.instance_name)
        .all()
    )

    provider_bot_rows = []

    for row in provider_bot_raw:
        provider_bot_rows.append({
            "provider_name": row.provider_name or "NO IDENTIFICADO",
            "instance_name": row.instance_name or MAIN_PANEL_INSTANCE,
            "total_exito": int(row.total_exito or 0),
            "sin_registro": int(row.sin_registro or 0),
            "actas_erroneas": int(row.actas_erroneas or 0),
            "otros_errores": int(row.otros_errores or 0),
            "pendientes": int(row.pendientes or 0),
            "total_solicitudes": int(row.total_solicitudes or 0),
        })

    provider_bot_rows.sort(
        key=lambda x: (-x["total_solicitudes"], x["provider_name"], x["instance_name"])
    )

    pending_rows = (
        q.filter(RequestLog.status.in_(["QUEUED", "PROCESSING"]))
        .order_by(RequestLog.created_at.asc())
        .limit(80)
        .all()
    )

    return {
        "provider_control_rows": provider_control_rows,
        "provider_control_totals": provider_control_totals,
        "error_detail_rows": error_detail_rows,
        "provider_bot_rows": provider_bot_rows,
        "pending_rows": pending_rows,
    }


def _day_name_es_from_date(day_str: str) -> str:
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    return DAYS_ES[dt.weekday()]


def is_group_blocked(group_jid: str) -> bool:
    if not group_jid:
        return False

    group_jid = str(group_jid).strip()

    # 1) Revisar Redis primero
    try:
        blocked = redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid)

        if not blocked:
            blocked = redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid.encode("utf-8"))

        if blocked:
            return True

    except Exception as e:
        print("IS_GROUP_BLOCKED_REDIS_ERROR =", str(e), flush=True)

    # 2) Respaldo: revisar PostgreSQL si la promo ya está agotada
    db = SessionLocal()
    try:
        promo = (
            db.query(GroupPromotion)
            .filter(GroupPromotion.group_jid == group_jid)
            .first()
        )

        if not promo:
            return False

        total = int(promo.total_actas or 0)
        used = int(promo.used_actas or 0)

        shared_limit = int(promo.shared_group_limit_actas or 0)
        shared_used = int(promo.shared_group_used_actas or 0)

        promo_agotada = (
            (total > 0 and used >= total)
            or
            (shared_limit > 0 and shared_used >= shared_limit)
        )

        if promo_agotada:
            print("IS_GROUP_BLOCKED_DB_EXHAUSTED_PROMO =", {
                "group_jid": group_jid,
                "promo_name": promo.promo_name,
                "used": used,
                "total": total,
                "shared_used": shared_used,
                "shared_limit": shared_limit,
            }, flush=True)

            # Re-sincronizar Redis automáticamente
            try:
                redis_conn.sadd(BLOCKED_GROUPS_KEY, group_jid)
            except Exception as redis_set_exc:
                print("IS_GROUP_BLOCKED_REDIS_RESYNC_ERROR =", str(redis_set_exc), flush=True)

            return True

        return False

    except Exception as e:
        print("IS_GROUP_BLOCKED_DB_ERROR =", str(e), flush=True)
        return False

    finally:
        db.close()


def _redis_sismember_str(key: str, value: str) -> bool:
    value = (value or "").strip()
    if not value:
        return False

    try:
        blocked = redis_conn.sismember(key, value)

        if not blocked:
            blocked = redis_conn.sismember(key, value.encode("utf-8"))

        return bool(blocked)

    except Exception as e:
        print("REDIS_SISMEMBER_ERROR =", key, value, str(e), flush=True)
        return False


def is_instance_admin_blocked(instance_name: str) -> bool:
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return False

    return _redis_sismember_str(ADMIN_BLOCKED_INSTANCES_KEY, instance_name)


def is_instance_blocked(instance_name: str) -> bool:
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return False

    # Bloqueo normal: límite, mini panel, etc.
    normal_blocked = _redis_sismember_str(BLOCKED_INSTANCES_KEY, instance_name)

    # Bloqueo administrativo: impuesto desde panel principal.
    admin_blocked = is_instance_admin_blocked(instance_name)

    return bool(normal_blocked or admin_blocked)


def admin_block_instance(instance_name: str):
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return

    # Lo metemos en ambos:
    # 1) en el bloqueo normal para que el webhook lo ignore
    # 2) en el bloqueo admin para que el mini panel NO pueda desbloquearlo
    block_instance(instance_name)
    redis_conn.sadd(ADMIN_BLOCKED_INSTANCES_KEY, instance_name)

    print("ADMIN_INSTANCE_BLOCKED =", instance_name, flush=True)
    print("ADMIN_BLOCKED_INSTANCES_NOW =", redis_conn.smembers(ADMIN_BLOCKED_INSTANCES_KEY), flush=True)


def admin_unblock_instance(instance_name: str):
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return

    # Solo el panel principal debe llamar esto.
    redis_conn.srem(ADMIN_BLOCKED_INSTANCES_KEY, instance_name)
    unblock_instance(instance_name)

    print("ADMIN_INSTANCE_UNBLOCKED =", instance_name, flush=True)
    print("ADMIN_BLOCKED_INSTANCES_NOW =", redis_conn.smembers(ADMIN_BLOCKED_INSTANCES_KEY), flush=True)


def list_blocked_instances():
    try:
        items = redis_conn.smembers(BLOCKED_INSTANCES_KEY) or []
        out = []
        for x in items:
            if isinstance(x, bytes):
                out.append(x.decode("utf-8", errors="ignore"))
            else:
                out.append(str(x))
        return sorted(out)
    except Exception:
        return []


def block_group(group_jid: str):
    if not group_jid:
        return
    redis_conn.sadd(BLOCKED_GROUPS_KEY, group_jid)
    print("GROUP_BLOCKED =", group_jid, flush=True)
    print("BLOCKED_GROUPS_NOW =", redis_conn.smembers(BLOCKED_GROUPS_KEY), flush=True)


def unblock_group(group_jid: str):
    if not group_jid:
        return
    redis_conn.srem(BLOCKED_GROUPS_KEY, group_jid)
    print("GROUP_UNBLOCKED =", group_jid, flush=True)
    print("BLOCKED_GROUPS_NOW =", redis_conn.smembers(BLOCKED_GROUPS_KEY), flush=True)


@app.post("/panel/group/{group_jid}/hide")
def panel_hide_group(group_jid: str, db: Session = Depends(get_db)):
    try:
        hide_group_from_main_panel(db, group_jid)
        _clear_panel_cache()
        _clear_group_name_cache()

        row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()

        return {
            "ok": bool(row and row.hidden_in_main),
            "group_jid": group_jid,
            "hidden": bool(row and row.hidden_in_main),
        }
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/{group_jid}/hide")
def panel_bot_hide_group(token: str, group_jid: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        _assert_group_owned_by_bot(db, group_jid, instance_name)
        hide_group_from_bot_panel(db, group_jid, instance_name)

        _clear_panel_cache()
        _clear_group_name_cache()
        return {"ok": True, "hidden": True}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.get("/botpanel/{token}/audit", response_class=HTMLResponse)
def botpanel_audit_all_groups(
    token: str,
    period: str = "day",
    group_jid: str = "",
    status: str = "DONE",
    db: Session = Depends(get_db),
):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        return HTMLResponse("<h3>Panel no válido.</h3>", status_code=404)

    if not _is_child_bot(instance_name):
        return HTMLResponse("<h3>Este panel es solo para bots desde docifybot8 en adelante.</h3>", status_code=400)

    period_view = period or "day"
    time_min, time_max, view = _panel_period_bounds(period_view)

    bot_title = _bot_title(db, instance_name)
    group_cache = _build_group_name_cache(db)

    bot_groups = _bot_groups_for_instance(db, instance_name)
    allowed_group_ids = [g.group_jid for g in bot_groups if g.group_jid]

    if not allowed_group_ids:
        return HTMLResponse(f"""
        <html>
        <head>
            <meta charset="utf-8">
            <title>Historial - {_esc(bot_title)}</title>
        </head>
        <body style="font-family:Arial;padding:24px;">
            <h2>Historial - {_esc(bot_title)}</h2>
            <p>Este bot aún no tiene grupos asignados.</p>
            <a href="/botpanel/{_esc(token)}">Volver al panel</a>
        </body>
        </html>
        """)

    q = db.query(RequestLog).filter(
        RequestLog.instance_name == instance_name,
        RequestLog.source_group_id.in_(allowed_group_ids),
        RequestLog.created_at >= time_min,
        RequestLog.created_at < time_max,
    )

    q = _exclude_private_provider_query(q, db, instance_name)

    if status:
        q = q.filter(RequestLog.status == status)

    if group_jid:
        # seguridad: solo permitir filtrar grupos que pertenecen al bot
        if group_jid not in allowed_group_ids:
            return HTMLResponse("<h3>Grupo no pertenece a este bot.</h3>", status_code=403)
        q = q.filter(RequestLog.source_group_id == group_jid)

    rows = (
        q.order_by(RequestLog.created_at.desc())
        .all()
    )

    totals = {
        "total": len(rows),
        "done": sum(1 for r in rows if r.status == "DONE"),
        "error": sum(1 for r in rows if r.status == "ERROR"),
        "queued": sum(1 for r in rows if r.status == "QUEUED"),
        "processing": sum(1 for r in rows if r.status == "PROCESSING"),
    }

    # ==========================================================
    # CORTE DIARIO + CORTE SEMANAL
    # Aplica al filtro actual:
    # - Si group_jid viene seleccionado, suma solo ese grupo.
    # - Si group_jid viene vacío, suma todos los grupos del mini panel.
    # - Si status=DONE, suma solo vendidas/hechas para cobranza.
    # ==========================================================
    daily_cut_map = {}

    start_local = _to_panel_tz(time_min)
    end_local = _to_panel_tz(time_max)

    # Crear todos los días del periodo aunque estén en cero.
    if start_local and end_local:
        current_day = start_local.date()
        last_day = (end_local - timedelta(days=1)).date()

        while current_day <= last_day:
            day_key = current_day.strftime("%Y-%m-%d")

            daily_cut_map[day_key] = {
                "date": day_key,
                "day_name": _day_name_es_from_date(day_key),
                "total": 0,
                "done": 0,
                "error": 0,
                "queued": 0,
                "processing": 0,
            }

            current_day = current_day + timedelta(days=1)

    # Sumar movimientos reales sobre esos días.
    for r in rows:
        local_dt = _to_panel_tz(r.created_at)
        if not local_dt:
            continue

        day_key = local_dt.strftime("%Y-%m-%d")

        if day_key not in daily_cut_map:
            daily_cut_map[day_key] = {
                "date": day_key,
                "day_name": _day_name_es_from_date(day_key),
                "total": 0,
                "done": 0,
                "error": 0,
                "queued": 0,
                "processing": 0,
            }

        item = daily_cut_map[day_key]
        item["total"] += 1

        st = (r.status or "").upper()

        if st == "DONE":
            item["done"] += 1
        elif st == "ERROR":
            item["error"] += 1
        elif st == "QUEUED":
            item["queued"] += 1
        elif st == "PROCESSING":
            item["processing"] += 1

    daily_cut_rows = sorted(
        daily_cut_map.values(),
        key=lambda x: x["date"]
    )

    by_group = {}

    for r in rows:
        gid = r.source_group_id or "PRIVADO"

        if gid not in by_group:
            by_group[gid] = {
                "group_jid": gid,
                "group_name": _group_name_cached(gid, group_cache),
                "total": 0,
                "done": 0,
                "error": 0,
                "queued": 0,
                "processing": 0,
            }

        item = by_group[gid]
        item["total"] += 1

        if r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1
        elif r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1

    group_rows = list(by_group.values())
    group_rows.sort(key=lambda x: (-x["total"], x["group_name"] or ""))

    range_buttons = f"""
        <a class="btn" href="/botpanel/{_esc(token)}/audit?period=day&status={_esc(status)}">Hoy</a>
        <a class="btn" href="/botpanel/{_esc(token)}/audit?period=30d&status={_esc(status)}">30 días</a>
        <a class="btn" href="/botpanel/{_esc(token)}/audit?period=month&status={_esc(status)}">Mes actual</a>
        <a class="btn" href="/botpanel/{_esc(token)}/audit?period=prev_month&status={_esc(status)}">Mes anterior</a>
    """

    group_options = '<option value="">Todos los grupos</option>'
    for g in bot_groups:
        selected = "selected" if group_jid == g.group_jid else ""
        group_options += (
            f'<option value="{_esc(g.group_jid)}" {selected}>'
            f'{_esc(_get_bot_group_name(db, g.group_jid))}</option>'
        )

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>Historial - {_esc(bot_title)}</title>
      <style>
        body {{
          font-family: Arial, sans-serif;
          background:#f4f6f8;
          padding:24px;
          color:#111827;
        }}
        .wrap {{
          max-width: 1250px;
          margin: 0 auto;
        }}
        .box {{
          background:white;
          border-radius:16px;
          padding:18px;
          margin-bottom:18px;
          box-shadow:0 8px 24px rgba(15,23,42,.08);
        }}
        .head {{
          display:flex;
          justify-content:space-between;
          align-items:center;
          gap:12px;
          flex-wrap:wrap;
        }}
        .btn {{
          display:inline-block;
          padding:8px 12px;
          border-radius:10px;
          text-decoration:none;
          background:#2563eb;
          color:white;
          font-size:13px;
          margin:3px;
        }}
        .btn-secondary {{
          background:#64748b;
        }}
        .stats {{
          display:grid;
          grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
          gap:12px;
          margin-top:14px;
        }}
        .stat {{
          background:#f8fafc;
          border:1px solid #e5e7eb;
          border-radius:14px;
          padding:12px;
        }}
        .stat strong {{
          display:block;
          font-size:24px;
        }}
        table {{
          width:100%;
          border-collapse:collapse;
          background:white;
        }}
        th, td {{
          padding:10px;
          border-bottom:1px solid #e5e7eb;
          font-size:13px;
          vertical-align:top;
        }}
        th {{
          text-align:left;
          background:#f8fafc;
        }}
        .small {{
          font-size:12px;
          color:#64748b;
        }}
        .status-d {{ color:#15803d;font-weight:bold; }}
        .status-e {{ color:#b91c1c;font-weight:bold; }}
        .status-q {{ color:#92400e;font-weight:bold; }}
        .status-p {{ color:#1d4ed8;font-weight:bold; }}
        .weekly-row {{
          background:#dbeafe;
          font-weight:800;
        }}
        .weekly-row td {{
          border-top:2px solid #93c5fd;
        }}
        select {{
          padding:8px;
          border-radius:10px;
          border:1px solid #cbd5e1;
          min-width:260px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          background: white;
          table-layout: fixed;
        }}
        td, th {{
          word-break: break-word;
        }}
        td:nth-child(1) {{
          width: 90px;
        }}
        td:nth-child(2) {{
          width: 60px;
        }}
        td:nth-child(3) {{
          width: 160px;
        }}
        td:nth-child(4) {{
          width: 110px;
        }}
        td:nth-child(5) {{
          width: 80px;
        }}
        td:nth-child(6) {{
          width: 220px;
        }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="box">
          <div class="head">
            <div>
              <h2>Historial de {_esc(bot_title)}</h2>
              <div class="small">Instancia: {_esc(instance_name)} | Periodo: {_esc(view)}</div>
            </div>
            <div>
              <a class="btn btn-secondary" href="/botpanel/{_esc(token)}">Volver al mini panel</a>
            </div>
          </div>

          <div style="margin-top:14px;">
            {range_buttons}
          </div>

          <form method="get" action="/botpanel/{_esc(token)}/audit" style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
            <input type="hidden" name="period" value="{_esc(view)}">
            <select name="group_jid">
              {group_options}
            </select>

            <select name="status">
              <option value="DONE" {"selected" if status == "DONE" else ""}>Solo vendidas / DONE</option>
              <option value="" {"selected" if status == "" else ""}>Todas</option>
              <option value="ERROR" {"selected" if status == "ERROR" else ""}>Errores</option>
              <option value="PROCESSING" {"selected" if status == "PROCESSING" else ""}>Procesando</option>
              <option value="QUEUED" {"selected" if status == "QUEUED" else ""}>En cola</option>
            </select>

            <button class="btn" type="submit" style="border:0;cursor:pointer;">Filtrar</button>
          </form>

          <div class="stats">
            <div class="stat"><span>Total</span><strong>{totals["total"]}</strong></div>
            <div class="stat"><span>DONE</span><strong>{totals["done"]}</strong></div>
            <div class="stat"><span>ERROR</span><strong>{totals["error"]}</strong></div>
            <div class="stat"><span>PROCESSING</span><strong>{totals["processing"]}</strong></div>
            <div class="stat"><span>QUEUED</span><strong>{totals["queued"]}</strong></div>
          </div>
        </div>
    """

    html += """
        <div class="box">
          <h3>Corte diario y semanal</h3>
          <p class="small">
            Suma diaria del periodo seleccionado y corte automático de lunes a domingo.
            Si filtras un grupo, el corte es solo de ese grupo. Si no filtras grupo, suma todos los grupos del mini panel.
          </p>
          <table>
            <thead>
              <tr>
                <th>Día</th>
                <th>Fecha</th>
                <th>Total</th>
                <th>DONE</th>
                <th>ERROR</th>
                <th>PROCESSING</th>
                <th>QUEUED</th>
              </tr>
            </thead>
            <tbody>
    """

    if daily_cut_rows:
        weekly_total = 0
        weekly_done = 0
        weekly_error = 0
        weekly_processing = 0
        weekly_queued = 0
        weekly_start = None

        for idx, d in enumerate(daily_cut_rows):
            if weekly_start is None:
                weekly_start = d["date"]

            weekly_total += int(d["total"] or 0)
            weekly_done += int(d["done"] or 0)
            weekly_error += int(d["error"] or 0)
            weekly_processing += int(d["processing"] or 0)
            weekly_queued += int(d["queued"] or 0)

            html += f"""
              <tr>
                <td>{_esc(d["day_name"])}</td>
                <td>{_esc(d["date"])}</td>
                <td>{int(d["total"] or 0)}</td>
                <td>{int(d["done"] or 0)}</td>
                <td>{int(d["error"] or 0)}</td>
                <td>{int(d["processing"] or 0)}</td>
                <td>{int(d["queued"] or 0)}</td>
              </tr>
            """

            is_sunday = (d["day_name"] or "").upper() == "DOMINGO"
            is_last_day = idx == len(daily_cut_rows) - 1

            if is_sunday or is_last_day:
                html += f"""
                  <tr class="weekly-row">
                    <td>CORTE SEMANAL</td>
                    <td>{_esc(weekly_start)} a {_esc(d["date"])}</td>
                    <td>{weekly_total}</td>
                    <td>{weekly_done}</td>
                    <td>{weekly_error}</td>
                    <td>{weekly_processing}</td>
                    <td>{weekly_queued}</td>
                  </tr>
                """

                weekly_total = 0
                weekly_done = 0
                weekly_error = 0
                weekly_processing = 0
                weekly_queued = 0
                weekly_start = None
    else:
        html += """
              <tr>
                <td colspan="7">Sin movimientos en este periodo.</td>
              </tr>
        """

    html += """
            </tbody>
          </table>
        </div>

        <div class="box">
          <h3>Resumen por grupo</h3>
          
          <table>
            <thead>
              <tr>
                <th>Grupo</th>
                <th>Total</th>
                <th>DONE</th>
                <th>ERROR</th>
                <th>PROCESSING</th>
                <th>QUEUED</th>
              </tr>
            </thead>
            <tbody>
    """

    if group_rows:
        for g in group_rows:
            html += f"""
              <tr>
                <td>
                  <strong>{_esc(g["group_name"])}</strong><br>
                  <span class="small">{_esc(g["group_jid"])}</span>
                </td>
                <td>{g["total"]}</td>
                <td>{g["done"]}</td>
                <td>{g["error"]}</td>
                <td>{g["processing"]}</td>
                <td>{g["queued"]}</td>
              </tr>
            """
    else:
        html += """
              <tr>
                <td colspan="6">Sin movimientos en este periodo.</td>
              </tr>
        """

    html += """
            </tbody>
          </table>
        </div>

        <div class="box">
          <h3>Movimientos</h3>
          <table>
            <thead>
              <tr>
                <th>Hora envío solicitud</th>
                <th>Hora recibido PDF</th>
                <th>Tiempo total</th>
                <th>ID</th>
                <th>Dato</th>
                <th>Tipo</th>
                <th>Estado</th>
                <th>Grupo</th>
                <th>PDF</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
    """

    if rows:
        for r in rows:
            local_created = _to_panel_tz(r.created_at)
            hora_envio = local_created.strftime("%Y-%m-%d %H:%M:%S") if local_created else ""
            
            hora_recibido = ""
            tiempo_total = ""
            
            if r.status == "DONE" and r.created_at and r.updated_at:
                local_updated = _to_panel_tz(r.updated_at)
                hora_recibido = local_updated.strftime("%Y-%m-%d %H:%M:%S") if local_updated else ""
            
                try:
                    tiempo_total = _fmt_duration_seconds((r.updated_at - r.created_at).total_seconds())
                except Exception:
                    tiempo_total = ""

            status_class = {
                "DONE": "status-d",
                "ERROR": "status-e",
                "QUEUED": "status-q",
                "PROCESSING": "status-p",
            }.get(r.status, "")

            gid = r.source_group_id or ""
            gname = _group_name_cached(gid, group_cache) if gid else "PRIVADO"

            pdf_btn = "<span class='small'>No disponible</span>"

            if r.status == "DONE" and getattr(r, "pdf_storage_key", None):
                pdf_btn = f"""
                  <button class="btn"
                          type="button"
                          onclick="downloadPdf('{_esc(token)}', {int(r.id)})"
                          style="background:#16a34a;border:0;cursor:pointer;">
                    📄 Descargar
                  </button>
                """

            html += f"""
              <tr>
                <td>{_esc(hora_envio)}</td>
                <td>{_esc(hora_recibido)}</td>
                <td><strong>{_esc(tiempo_total)}</strong></td>
                <td>{_esc(r.id)}</td>
                <td>{_esc(r.curp)}</td>
                <td>{_esc(r.act_type)}</td>
                <td class="{status_class}">{_esc(r.status)}</td>
                <td>
                  <strong>{_esc(gname)}</strong><br>
                  <span class="small">{_esc(gid)}</span>
                </td>
                <td>{pdf_btn}</td>
                <td class="small">{_esc((r.error_message or "")[:180])}</td>
              </tr>
            """
    else:
        html += """
              <tr>
                <td colspan="10">Sin movimientos en este periodo.</td>
              </tr>
        """

    html += """
            </tbody>
          </table>
        </div>
      </div>
      
    <script>
      async function downloadPdf(token, requestId) {
        try {
          const res = await fetch(`/botpanel/${token}/request/${requestId}/pdf`);
          const data = await res.json();

          if (!res.ok || !data.ok || !data.url) {
            alert(data.detail || "PDF no disponible");
            return;
          }

          window.open(data.url, "_blank");
        } catch (e) {
          alert("Error al generar descarga del PDF");
        }
      }
    </script>
    </body>
    </html>
    """

    return HTMLResponse(content=html)


@app.get("/botpanel/{token}/request/{request_id}/pdf")
def botpanel_download_request_pdf(
    token: str,
    request_id: int,
    db: Session = Depends(get_db),
):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        raise HTTPException(status_code=404, detail="Panel no válido")

    row = (
        db.query(RequestLog)
        .filter(
            RequestLog.id == request_id,
            RequestLog.instance_name == instance_name,
            RequestLog.status == "DONE",
        )
        .first()
    )

    if not row:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada")

    allowed_group_ids = _owned_group_ids_for_instance(db, instance_name)

    if row.source_group_id not in allowed_group_ids:
        raise HTTPException(status_code=403, detail="No autorizado")

    if not row.pdf_storage_key:
        raise HTTPException(status_code=404, detail="PDF no guardado")

    try:
        url = generate_r2_presigned_download_url(
            row.pdf_storage_key,
            row.pdf_filename or f"{row.curp}.pdf",
            expires_sec=300,
        )
    except Exception as e:
        print("R2_PRESIGNED_URL_ERROR =", {
            "request_id": request_id,
            "storage_key": row.pdf_storage_key,
            "error": str(e),
        }, flush=True)
        raise HTTPException(status_code=500, detail="No se pudo generar link de descarga")

    return {
        "ok": True,
        "url": url,
    }


@app.get("/panel/instances")
def panel_instances(db: Session = Depends(get_db)):
    rows = (
        db.query(
            RequestLog.instance_name,
            func.count(RequestLog.id)
        )
        .group_by(RequestLog.instance_name)
        .all()
    )

    items = []
    for instance_name, total in rows:
        name = instance_name or "docifybot8"
        credit = _bot_credit_stats(db, name)
        used = credit["used"]
        limit_value = credit["limit"]
        blocked = is_instance_blocked(name)
        
        items.append({
            "instance_name": name,
            "total_requests": int(total or 0),
            "used": used,
            "limit": limit_value,
            "available": credit["available"] if limit_value > 0 else None,
            "blocked": blocked,
        })

    items.sort(key=lambda x: x["instance_name"])
    return {"ok": True, "items": items}


@app.post("/panel/instance/{instance_name}/block")
def panel_block_instance(instance_name: str):
    admin_block_instance(instance_name)
    _clear_panel_cache()
    return {
        "ok": True,
        "instance_name": instance_name,
        "blocked": True,
        "admin_blocked": True,
    }


@app.post("/panel/instance/{instance_name}/unblock")
def panel_unblock_instance(instance_name: str):
    admin_unblock_instance(instance_name)
    _clear_panel_cache()
    return {
        "ok": True,
        "instance_name": instance_name,
        "blocked": False,
        "admin_blocked": False,
    }


@app.post("/panel/instance/{instance_name}/manager-price")
async def panel_set_instance_manager_price(
    instance_name: str,
    request: Request,
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {
            "ok": False,
            "error": "UNAUTHORIZED",
        }

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    inst = _norm_instance(instance_name)

    if not inst:
        return {
            "ok": False,
            "error": "INSTANCIA_INVALIDA",
        }

    manager_name = str(payload.get("manager_name") or "").strip()

    if len(manager_name) > 100:
        return {
            "ok": False,
            "error": "El nombre del gestor no puede exceder 100 caracteres.",
        }

    raw_price = str(payload.get("manager_price") or "").strip()

    # Se permite vacío para borrar el precio.
    manager_price = ""

    if raw_price:
        try:
            price_value = Decimal(
                raw_price
                .replace("$", "")
                .replace(",", "")
                .strip()
            )
        except Exception:
            return {
                "ok": False,
                "error": "El precio debe ser un número válido.",
            }

        if price_value < 0:
            return {
                "ok": False,
                "error": "El precio no puede ser negativo.",
            }

        manager_price = f"{price_value.quantize(Decimal('0.01')):.2f}"

    _set_app_setting(
        db,
        _bot_manager_name_key(inst),
        manager_name,
    )

    _set_app_setting(
        db,
        _bot_manager_price_key(inst),
        manager_price,
    )

    _clear_panel_cache()

    return {
        "ok": True,
        "instance_name": inst,
        "manager_name": manager_name,
        "manager_price": manager_price,
    }


@app.post("/panel/instance/{instance_name}/limit")
async def panel_set_instance_limit(instance_name: str, request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    limit_value = int(payload.get("limit") or 0)
    set_bot_limit(db, instance_name, limit_value)
    _clear_panel_cache()

    return {
        "ok": True,
        "instance_name": instance_name,
        "limit": get_bot_limit(db, instance_name),
        "used": get_bot_used(db, instance_name),
        "blocked": is_instance_blocked(instance_name),
    }


@app.post("/panel/instance/{instance_name}/reset-usage")
def panel_reset_instance_usage(instance_name: str, db: Session = Depends(get_db)):
    set_bot_used(db, instance_name, 0)
    _clear_panel_cache()

    used_now = get_bot_used(db, instance_name)
    limit_value = get_bot_limit(db, instance_name)

    return {
        "ok": True,
        "instance_name": instance_name,
        "used": used_now,
        "limit": limit_value,
        "available": max(0, limit_value - used_now) if limit_value > 0 else None,
        "blocked": is_instance_blocked(instance_name),
    }


@app.post("/panel/instance/{instance_name}/recharge")
async def panel_recharge_instance(instance_name: str, request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    try:
        add_value = int(payload.get("amount") or 0)
    except Exception:
        add_value = 0

    if add_value <= 0:
        return {
            "ok": False,
            "error": "La recarga debe ser mayor a 0."
        }

    current_limit = get_bot_limit(db, instance_name)
    used_now = get_bot_used(db, instance_name)

    new_limit = current_limit + add_value
    available_after = max(new_limit - used_now, 0)

    set_bot_limit(db, instance_name, new_limit)

    log = BotRechargeLog(
        instance_name=instance_name,
        amount=add_value,
        previous_limit=current_limit,
        new_limit=new_limit,
        used_at_recharge=used_now,
        available_after=available_after,
        source="panel",
        note=None,
    )

    db.add(log)

    bot = (
        db.query(BotControl)
        .filter(BotControl.instance_name == instance_name)
        .first()
    )

    if bot:
        bot.recharges = int(bot.recharges or 0) + 1
        bot.updated_at = _utc_now_naive()

    db.commit()

    unblock_instance(instance_name)
    _clear_panel_cache()

    return {
        "ok": True,
        "instance_name": instance_name,
        "amount": add_value,
        "previous_limit": current_limit,
        "limit": new_limit,
        "used": used_now,
        "available": available_after,
        "recharges": int(bot.recharges or 0) if bot else None,
        "blocked": is_instance_blocked(instance_name),
    }

    
@app.post("/panel/groups/manual-add")
async def panel_manual_add_group(request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    group_jid = (payload.get("group_jid") or "").strip()
    custom_name = (payload.get("custom_name") or "").strip()
    category = (payload.get("category") or "otro").strip().lower()

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    if not group_jid.endswith("@g.us"):
        return {"ok": False, "error": "GROUP_JID_INVALID"}

    if category not in {"papeleria_ciber", "gestor", "otro"}:
        category = "otro"

    alias_row = (
        db.query(GroupAlias)
        .filter(GroupAlias.group_jid == group_jid)
        .first()
    )

    if alias_row:
        alias_row.custom_name = custom_name or group_jid
        alias_row.updated_at = _utc_now_naive()
    else:
        alias_row = GroupAlias(
            group_jid=group_jid,
            custom_name=custom_name or group_jid,
            updated_at=_utc_now_naive(),
        )
        db.add(alias_row)

    category_row = (
        db.query(GroupCategory)
        .filter(GroupCategory.group_jid == group_jid)
        .first()
    )

    if category_row:
        category_row.category = category
        category_row.updated_at = _utc_now_naive()
    else:
        category_row = GroupCategory(
            group_jid=group_jid,
            category=category,
            created_at=_utc_now_naive(),
            updated_at=_utc_now_naive(),
        )
        db.add(category_row)

    db.commit()
    _clear_panel_cache()
    _clear_group_name_cache()

    return {
        "ok": True,
        "message": "Grupo agregado manualmente",
        "group_jid": group_jid,
        "custom_name": alias_row.custom_name,
        "category": category,
    }
    

@app.post("/cron/provider3/keepalive")
def cron_provider3_keepalive(request: Request):
    secret = request.headers.get("x-keepalive-secret", "").strip()

    if settings.PROVIDER3_KEEPALIVE_SECRET and secret != settings.PROVIDER3_KEEPALIVE_SECRET:
        return {"ok": False, "error": "unauthorized"}

    time.sleep(random.uniform(10, 35))

    return provider3_keepalive_job()


def bot_is_open():
    now = datetime.now(ZoneInfo("America/Monterrey"))
    hour = now.hour
    return True


def _clear_panel_cache():
    try:
        batch = []

        for key in redis_conn.scan_iter(match="panel:*", count=200):
            batch.append(key)

            if len(batch) >= 500:
                redis_conn.delete(*batch)
                batch.clear()

        if batch:
            redis_conn.delete(*batch)

    except Exception as e:
        print("CLEAR_PANEL_CACHE_ERROR =", repr(e), flush=True)


def _clear_group_name_cache():
    try:
        redis_conn.delete("panel:group_name_cache")
    except Exception:
        pass


def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))


def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")


def _panel_month_start(dt=None):
    dt = dt or _panel_now()
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _panel_month_end(dt=None):
    dt = dt or _panel_now()
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if start.month == 12:
        return start.replace(year=start.year + 1, month=1)
    return start.replace(month=start.month + 1)


def _daterange_days(start_dt, end_dt):
    days = []
    cur = start_dt
    while cur < end_dt:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def _esc(v):
    if v is None:
        return ""
    return str(v)


def _fmt_dt(dt):
    if not dt:
        return ""
    try:
        local_dt = _to_panel_tz(dt)
        return local_dt.strftime("%Y-%m-%d %H:%M:%S") if local_dt else ""
    except Exception:
        return str(dt)


def _fmt_duration_seconds(seconds):
    try:
        seconds = int(max(0, seconds or 0))
    except Exception:
        return ""

    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def _parse_panel_date(value: str | None):
    value = (value or "").strip()
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None


def _panel_period_bounds(view: str, date_from: str = "", date_to: str = ""):
    view = (view or "day").strip().lower()

    # RANGO PERSONALIZADO
    # date_from y date_to vienen en formato YYYY-MM-DD desde inputs type="date".
    if view == "custom":
        start_date = _parse_panel_date(date_from)
        end_date = _parse_panel_date(date_to)

        # Si falta una fecha, usa hoy para evitar romper el panel.
        now = _panel_now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if not start_date:
            start_date = today_start.replace(tzinfo=None)

        if not end_date:
            end_date = start_date

        # Convertir a inicio de día local.
        local_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = end_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

        # Si lo ponen al revés, corregir automáticamente.
        if local_end <= local_start:
            local_end = local_start + timedelta(days=1)

        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)

        return utc_start, utc_end, "custom"

    if view == "week":
        now = _panel_now()
        local_start = now - timedelta(days=now.weekday())
        local_start = local_start.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        local_end = local_start + timedelta(days=7)

        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)

        return utc_start, utc_end, "week"

    if view == "30d":
        now = _panel_now()
        local_start = now - timedelta(days=29)
        local_start = local_start.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        local_end = now + timedelta(days=1)
        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)
        return utc_start, utc_end, "30d"

    if view == "month":
        local_start = _panel_month_start()
        local_end = _panel_month_end()
        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)
        return utc_start, utc_end, "month"

    if view == "prev_month":
        now = _panel_now()
        first_day_this_month = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        last_day_prev_month = first_day_this_month - timedelta(days=1)
        local_start = last_day_prev_month.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        local_end = first_day_this_month
        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)
        return utc_start, utc_end, "prev_month"

    now = _panel_now()
    local_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    local_end = local_start + timedelta(days=1)

    utc_start = _panel_to_utc_naive(local_start)
    utc_end = _panel_to_utc_naive(local_end)

    return utc_start, utc_end, "day"


def _query_requests_for_panel(
    db: Session,
    time_min: datetime,
    time_max: datetime,
    group_jid: str | None = None,
    provider_name: str | None = None,
    status: str | None = None,
    act_type: str | None = None,
):
    q = db.query(RequestLog).filter(
        RequestLog.created_at >= time_min,
        RequestLog.created_at < time_max,
        ~RequestLog.source_group_id.in_(HIDDEN_PANEL_GROUPS),
    )

    maya_personal_provider = _personal_provider_filter_for_instance(db, "docifybot8maya")

    if maya_personal_provider:
        q = q.filter(
            ~(
                (RequestLog.instance_name == "docifybot8maya")
                & (RequestLog.provider_name == maya_personal_provider)
            )
        )

    if group_jid:
        val = group_jid.strip()
        val_like = f"%{val}%"
        val_lower = val.lower()

        alias_matches = [
            gid for (gid,) in (
                db.query(GroupAlias.group_jid)
                .filter(
                    or_(
                        GroupAlias.group_jid.ilike(val_like),
                        GroupAlias.custom_name.ilike(val_like),
                    )
                )
                .all()
            )
        ]

        map_matches = [
            gid for gid, name in GROUP_NAME_MAP.items()
            if val_lower in gid.lower() or val_lower in (name or "").lower()
        ]

        matching_group_ids = list(dict.fromkeys(alias_matches + map_matches))
        matching_group_ids = [gid for gid in matching_group_ids if gid not in HIDDEN_PANEL_GROUPS]

        if matching_group_ids:
            q = q.filter(RequestLog.source_group_id.in_(matching_group_ids))
        else:
            q = q.filter(RequestLog.source_group_id.ilike(val_like))

    if provider_name:
        q = q.filter(RequestLog.provider_name.ilike(f"%{provider_name.strip()}%"))

    if status:
        q = q.filter(RequestLog.status.ilike(f"%{status.strip()}%"))

    if act_type:
        q = q.filter(RequestLog.act_type.ilike(f"%{act_type.strip()}%"))

    return q
    

def _panel_summary_from_rows(rows: list[RequestLog]) -> dict:
    out = {
        "total": 0,
        "queued": 0,
        "processing": 0,
        "done": 0,
        "error": 0,
    }

    for r in rows:
        out["total"] += 1
        if r.status == "QUEUED":
            out["queued"] += 1
        elif r.status == "PROCESSING":
            out["processing"] += 1
        elif r.status == "DONE":
            out["done"] += 1
        elif r.status == "ERROR":
            out["error"] += 1

    return out


HIDDEN_PANEL_GROUPS = {
    "120363408639542108@g.us",  # AD 1
    "120363427054214985@g.us",  # AD 2
    "120363409374690453@g.us",  # AD 3
    "120363426725671842@g.us",  # Prov Pruebas 1
    "120363408272742958@g.us",  # Prov Pruebas 2
    "120363406806549379@g.us",  # Actas Pruebas 1
    "120363425323721713@g.us",  # Actas Pruebas 2
    "120363407066931119@g.us",  # Actas Pruebas 3
}


def _panel_group_rows(
    rows: list[RequestLog],
    db: Session,
    include_all_groups: bool = False,
    has_active_filters: bool = False,
) -> list[dict]:
    data = {}
    group_cache = _build_group_name_cache(db)

    excluded_words = (
        "PROV",
        "PRUEBA",
        "PRUEBAS",
        "TEST",
        "AD",
    )

    def _is_hidden_group(gid: str, name: str) -> bool:
        if gid in HIDDEN_PANEL_GROUPS:
            return True
    
        name_up = (name or "").strip().upper()
        excluded_words = (
            "PROV",
            "PRUEBA",
            "PRUEBAS",
            "TEST",
            "AD",
        )
        return any(word in name_up for word in excluded_words)

    if include_all_groups and not has_active_filters:
        for gid in (set(GROUP_NAME_MAP.keys()) | set(group_cache.keys())):
            gid = gid or "PRIVADO"
            group_name = _group_name_cached(gid, group_cache)
    
            if gid in HIDDEN_PANEL_GROUPS:
                continue
    
            row = db.query(AuthorizedGroup).filter_by(group_jid=gid).first()
            owner = (row.owner_instance or "").strip() if row else ""

            if gid != "PRIVADO" and owner and owner != "docifybot8":
                continue
            
            if gid != "PRIVADO" and not owner and _is_hidden_panel_group(gid, group_name):
                continue
    
            data[gid] = {
                "group_jid": gid,
                "group_name": group_name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
                "last_update": None,
            }

        data["PRIVADO"] = {
            "group_jid": "PRIVADO",
            "group_name": "PRIVADO",
            "total": 0,
            "queued": 0,
            "processing": 0,
            "done": 0,
            "error": 0,
            "last_update": None,
        }

    for r in rows:
        gid = r.source_group_id or "PRIVADO"
        group_name = _group_name_cached(gid, group_cache)

        if gid != "PRIVADO" and _is_hidden_group(gid, group_name):
            continue

        if gid not in data:
            data[gid] = {
                "group_jid": gid,
                "group_name": group_name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
                "last_update": None,
            }

        item = data[gid]
        item["total"] += 1

        if r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1
        elif r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1

        if r.updated_at and (not item["last_update"] or r.updated_at > item["last_update"]):
            item["last_update"] = r.updated_at

    out = list(data.values())

    if has_active_filters or not include_all_groups:
        out = [x for x in out if x["total"] > 0]

    out = [x for x in out if x["group_jid"] != "PRIVADO" or x["total"] > 0]
    out.sort(key=lambda x: ((x["total"] == 0), -x["total"], x["group_name"]))

    return out


def _panel_provider_rows(rows: list[RequestLog]) -> list[dict]:
    data = {}

    for r in rows:
        name = r.provider_name or "NO IDENTIFICADO"
        if name not in data:
            data[name] = {
                "provider_name": name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
            }

        item = data[name]
        item["total"] += 1

        if r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1
        elif r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1

    out = list(data.values())
    out.sort(key=lambda x: (-x["total"], x["provider_name"]))
    return out


def _panel_instance_rows(rows: list[RequestLog]) -> list[dict]:
    data = {}

    for r in rows:
        name = r.instance_name or "docifybot8"
        if name not in data:
            data[name] = {
                "instance_name": name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
            }

        item = data[name]
        item["total"] += 1

        if r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1
        elif r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1

    out = list(data.values())
    out.sort(key=lambda x: (-x["total"], x["instance_name"]))
    return out


def _panel_type_rows(rows: list[RequestLog]) -> list[dict]:
    data = {}

    for r in rows:
        name = r.act_type or "SIN_TIPO"
        if name not in data:
            data[name] = {
                "act_type": name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
            }

        item = data[name]
        item["total"] += 1

        if r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1
        elif r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1

    out = list(data.values())
    out.sort(key=lambda x: (-x["total"], x["act_type"]))
    return out


def _panel_daily_group_rows(rows: list[RequestLog], db: Session) -> list[dict]:
    data = {}

    for r in rows:
        local_dt = _to_panel_tz(r.created_at)
        day = local_dt.strftime("%Y-%m-%d") if local_dt else "SIN_FECHA"
        gid = r.source_group_id or "PRIVADO"
        key = (day, gid)

        if key not in data:
            data[key] = {
                "day": day,
                "group_jid": gid,
                "group_name": _group_name(gid, db),
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
            }

        item = data[key]
        item["total"] += 1

        if r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1
        elif r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1

    out = list(data.values())
    out.sort(key=lambda x: (x["day"], x["group_jid"]), reverse=True)
    return out


def _panel_detail_for_group(
    rows: list[RequestLog],
    group_jid: str,
    view: str,
    db: Session,
    date_from: str = "",
    date_to: str = "",
) -> dict:
    days = {}

    view = (view or "day").strip().lower()

    time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)
    
    local_start = _to_panel_tz(time_min)
    local_end = _to_panel_tz(time_max)
    
    if not local_start or not local_end:
        now_local = _panel_now()
        local_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = local_start + timedelta(days=1)

    cur = local_start
    while cur < local_end:
        day_str = cur.strftime("%Y-%m-%d")
        days[day_str] = {
            "day_name": _day_name_es_from_date(day_str),
            "date": day_str,
            "total": 0,
            "done": 0,
            "error": 0,
            "queued": 0,
            "processing": 0,
        }
        cur += timedelta(days=1)

    for r in rows:
        if (r.source_group_id or "PRIVADO") != group_jid:
            continue

        if not r.created_at:
            continue

        local_dt = _to_panel_tz(r.created_at)
        day_str = local_dt.strftime("%Y-%m-%d")
        if day_str not in days:
            continue

        item = days[day_str]
        item["total"] += 1

        if r.status == "DONE":
            item["done"] += 1
        elif r.status == "ERROR":
            item["error"] += 1
        elif r.status == "QUEUED":
            item["queued"] += 1
        elif r.status == "PROCESSING":
            item["processing"] += 1

    rows_out = list(days.values())
    rows_out.sort(key=lambda x: x["date"])

    totals = {
        "total": sum(x["total"] for x in rows_out),
        "done": sum(x["done"] for x in rows_out),
        "error": sum(x["error"] for x in rows_out),
        "queued": sum(x["queued"] for x in rows_out),
        "processing": sum(x["processing"] for x in rows_out),
    }

    return {
        "group_jid": group_jid,
        "group_name": _group_name(group_jid, db),
        "rows": rows_out,
        "totals": totals,
        "date_from": local_start.strftime("%Y-%m-%d"),
        "date_to": (local_end - timedelta(days=1)).strftime("%Y-%m-%d"),
        "view": view,
    }


@app.get("/panel/audit/group")
def panel_audit_group(
    request: Request,
    group_jid: str,
    view: str = "day",
    range: str = "",
    instance_name: str = "",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return HTMLResponse("No autorizado", status_code=403)

    period_view = range or view
    time_min, time_max, view = _panel_period_bounds(period_view, date_from, date_to)

    q = db.query(RequestLog).filter(
        RequestLog.source_group_id == group_jid,
        RequestLog.status == "DONE",
        RequestLog.created_at >= time_min,
        RequestLog.created_at < time_max,
    )

    if instance_name:
        q = q.filter(RequestLog.instance_name == instance_name)

    rows = q.order_by(RequestLog.created_at.asc()).all()

    show_provider = (instance_name or "").strip() in ("", "docifybot8")

    MX_TZ = ZoneInfo("America/Monterrey")

    def to_mx_date(dt):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MX_TZ).date()

    def week_range_for(date_obj):
        start = date_obj - timedelta(days=date_obj.weekday())  # lunes
        end = start + timedelta(days=6)  # domingo
        return start, end

    dias_es = {
        0: "LUNES",
        1: "MARTES",
        2: "MIÉRCOLES",
        3: "JUEVES",
        4: "VIERNES",
        5: "SÁBADO",
        6: "DOMINGO",
    }

    daily = defaultdict(list)
    weekly = defaultdict(list)

    for r in rows:
        mx_date = to_mx_date(r.created_at)
        daily[mx_date].append(r)

        w_start, w_end = week_range_for(mx_date)
        weekly[(w_start, w_end)].append(r)

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>Auditoría de conteo</title>
      <style>
        body {{
          font-family: Arial, sans-serif;
          padding: 20px;
          background:#f6f7fb;
          color:#111827;
        }}

        .box {{
          background:white;
          padding:18px;
          border-radius:14px;
          box-shadow:0 2px 10px #0001;
          margin-bottom:18px;
        }}

        h2 {{
          margin-top:0;
        }}

        table {{
          width:100%;
          border-collapse:collapse;
          margin-top:15px;
          background:white;
        }}

        th,td {{
          padding:10px 12px;
          border-bottom:1px solid #ddd;
          font-size:13px;
          text-align:left;
        }}

        th {{
          background:#071933;
          color:white;
          font-weight:bold;
        }}

        tbody tr:nth-child(even) {{
          background:#fafafa;
        }}

        .mono {{
          font-family: monospace;
        }}

        .ok {{
          color:green;
          font-weight:bold;
        }}

        .num {{
          text-align:right;
          white-space:nowrap;
        }}

        .weekly-row {{
          background:#dbeafe;
          font-weight:bold;
          border-top:2px solid #93c5fd;
        }}

        .muted {{
          color:#4b5563;
          font-size:13px;
        }}
      </style>
    </head>
    <body>

      <div class="box">
        <h2>Auditoría de conteo</h2>
        <p><b>Grupo:</b> <span class="mono">{_esc(group_jid)}</span></p>
        <p><b>Bot:</b> {_esc(instance_name or "Todos")}</p>
        <p><b>Periodo:</b> {_esc(view)}</p>
        <p><b>Zona horaria:</b> America/Monterrey</p>
        <p><b>Total contado:</b> <span class="ok">{len(rows)}</span></p>

        <p class="muted">
          Este conteo incluye únicamente solicitudes con status <b>DONE</b>.
          No se cuentan errores, pendientes ni solicitudes en proceso.
        </p>
      </div>

      <div class="box">
        <h3>Resumen diario y corte semanal</h3>

        <table>
          <thead>
            <tr>
              <th>Día</th>
              <th>Fecha</th>
              <th class="num">Hecho</th>
            </tr>
          </thead>
          <tbody>
    """

    sorted_days = sorted(daily.keys())

    for idx, day in enumerate(sorted_days):
        w_start, w_end = week_range_for(day)

        count_day = len(daily[day])

        html += f"""
            <tr>
              <td>{dias_es[day.weekday()]}</td>
              <td>{day}</td>
              <td class="num">{count_day}</td>
            </tr>
        """

        is_last_day = idx == len(sorted_days) - 1
        next_day_diff_week = False

        if not is_last_day:
            next_day = sorted_days[idx + 1]
            next_w_start, next_w_end = week_range_for(next_day)
            next_day_diff_week = (next_w_start, next_w_end) != (w_start, w_end)

        if is_last_day or next_day_diff_week:
            week_rows = weekly[(w_start, w_end)]
            count_week = len(week_rows)
            
            html += f"""
            <tr class="weekly-row">
              <td>CORTE SEMANAL</td>
              <td>{w_start} a {w_end}</td>
              <td class="num">{count_week}</td>
            </tr>
            """

    if not sorted_days:
        html += """
            <tr>
              <td colspan="3">No hay solicitudes DONE en este periodo.</td>
            </tr>
        """

    html += """
          </tbody>
        </table>
      </div>

      <div class="box">
        <h3>Detalle de solicitudes</h3>

        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Fecha MX</th>
              <th>Bot</th>
              <th>Grupo</th>
              <th>Dato</th>
              <th>Tipo</th>
    """

    if show_provider:
        html += """
              <th>Proveedor</th>
        """

    html += """
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
    """

    for r in rows:
        html += f"""
            <tr>
              <td>{r.id}</td>
              <td>{_esc(_fmt_dt(r.created_at))}</td>
              <td>{_esc(bot_label(r.instance_name or "docifybot8", db))}</td>
              <td class="mono">{_esc(r.source_group_id)}</td>
              <td class="mono">{_esc(r.curp)}</td>
              <td>{_esc(r.act_type)}</td>
        """

        if show_provider:
            html += f"""
              <td>{_esc(_provider_label(r.provider_name))}</td>
            """

        html += f"""
              <td>{_esc(r.status)}</td>
            </tr>
        """

    if not rows:
        html += f"""
            <tr>
              <td colspan="{8 if show_provider else 7}">No hay solicitudes DONE en este periodo.</td>
            </tr>
        """

    html += """
          </tbody>
        </table>
      </div>

    </body>
    </html>
    """

    return HTMLResponse(html)


def _cache_get_json(key: str):
    try:
        raw = redis_conn.get(key)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        return json.loads(raw)
    except Exception:
        return None


def _cache_set_json(key: str, value, ttl: int = 30):
    try:
        redis_conn.setex(key, ttl, json.dumps(value, ensure_ascii=False))
    except Exception:
        pass


def _build_group_name_cache(db: Session) -> dict[str, str]:
    cache_key = "panel:group_name_cache"

    cached = _cache_get_json(cache_key)
    if isinstance(cached, dict) and cached:
        return cached

    cache = {"PRIVADO": "PRIVADO"}

    for gid, name in GROUP_NAME_MAP.items():
        if gid:
            cache[gid] = (name or "").strip() or gid

    alias_rows = (
        db.query(GroupAlias.group_jid, GroupAlias.custom_name)
        .all()
    )

    for gid, custom_name in alias_rows:
        if gid:
            cache[gid] = (custom_name or "").strip() or cache.get(gid, gid)

    _cache_set_json(cache_key, cache, ttl=GROUP_NAME_CACHE_TTL)
    return cache


def _group_name_cached(group_jid: str | None, group_cache: dict[str, str]) -> str:
    gid = (group_jid or "").strip()
    if not gid:
        return "PRIVADO"
    return group_cache.get(gid, gid)


@app.post("/panel/ping-group")
def panel_ping_group(payload: dict):
    group_jid = (payload.get("group_jid") or "").strip()

    if not group_jid:
        return {"ok": False, "error": "NO_GROUP"}

    msg = f"""🔎 *PING PANEL*

Este mensaje es para identificar el grupo.

Group JID:
{group_jid}
"""

    try:
        send_group_text(group_jid, msg)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/panel/recent-requests/stream")
async def panel_recent_requests_stream():
    if not PANEL_STREAM_ENABLED:
        return HTMLResponse(content="", status_code=204)

    async def event_generator():
        last_seen_id = 0
        last_seen_updated = ""

        while True:
            db = SessionLocal()
            try:
                cache_key = "panel:recent_requests:latest_marker"
                marker = _cache_get_json(cache_key)

                if not marker:
                    row = (
                        db.query(RequestLog.id, RequestLog.updated_at)
                        .order_by(RequestLog.updated_at.desc(), RequestLog.id.desc())
                        .first()
                    )

                    marker = {
                        "latest_id": row.id if row else 0,
                        "latest_updated_at": row.updated_at.isoformat() if row and row.updated_at else "",
                    }
                    _cache_set_json(cache_key, marker, ttl=10)

                current_id = marker.get("latest_id", 0)
                current_updated = marker.get("latest_updated_at", "")

                changed = (
                    current_id != last_seen_id
                    or current_updated != last_seen_updated
                )

                if changed:
                    payload = {
                        "latest_id": current_id,
                        "latest_updated_at": current_updated,
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_seen_id = current_id
                    last_seen_updated = current_updated

            except Exception as e:
                payload = {"error": str(e)}
                yield f"data: {json.dumps(payload)}\n\n"
            finally:
                db.close()

            await asyncio.sleep(PANEL_STREAM_SLEEP)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    

@app.get("/panel/recent-requests")
def panel_recent_requests(
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    status: str = "",
    act_type: str = "",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    cache_key = "panel:recent_requests_html:" + "|".join([
        (view or "").strip(),
        (date_from or "").strip(),
        (date_to or "").strip(),
        (group_jid or "").strip(),
        (provider_name or "").strip(),
        (status or "").strip(),
        (act_type or "").strip(),
    ])

    cached_html = redis_conn.get(cache_key)
    if cached_html:
        if isinstance(cached_html, bytes):
            cached_html = cached_html.decode("utf-8", errors="ignore")
        return HTMLResponse(content=cached_html)

    time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)
    group_cache = _build_group_name_cache(db)

    rows = (
        _query_requests_for_panel(
            db=db,
            time_min=time_min,
            time_max=time_max,
            group_jid=group_jid or None,
            provider_name=provider_name or None,
            status=status or None,
            act_type=act_type or None,
        )
        .with_entities(
            RequestLog.id,
            RequestLog.curp,
            RequestLog.act_type,
            RequestLog.status,
            RequestLog.source_group_id,
            RequestLog.instance_name,
            RequestLog.provider_name,
            RequestLog.provider_group_id,
            RequestLog.created_at,
            RequestLog.updated_at,
            RequestLog.error_message,
        )
        .order_by(RequestLog.created_at.desc())
        .limit(15)
        .all()
    )

    html = """
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Dato</th>
            <th>Tipo</th>
            <th>Estado</th>
            <th>Grupo cliente</th>
            <th>Bot</th>
            <th>Proveedor</th>
            <th>Creado</th>
            <th>Actualizado</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
    """

    if rows:
        for r in rows:
            status_class = {
                "QUEUED": "status-q",
                "PROCESSING": "status-p",
                "DONE": "status-d",
                "ERROR": "status-e",
            }.get(r.status, "")

            html += f"""
            <tr>
              <td>{r.id}</td>
              <td class="mono">{_esc(r.curp)}</td>
              <td>{_esc(r.act_type)}</td>
              <td class="{status_class}">{_esc(r.status)}</td>
              <td>{_esc(_group_name_cached(r.source_group_id, group_cache) if (r.instance_name or "docifybot8") == "docifybot8" else "OCULTO")}</td>
              <td>{_esc(bot_label(r.instance_name, db))}</td>
              <td>{_esc(_provider_label(r.provider_name))}</td>
              <td>{_esc(_fmt_dt(r.created_at))}</td>
              <td>{_esc(_fmt_dt(r.updated_at))}</td>
              <td class="small">{_esc(r.error_message)}</td>
            </tr>
            """
    else:
        html += '<tr><td colspan="10">Sin solicitudes en este periodo.</td></tr>'

    html += """
        </tbody>
      </table>
    </div>
    """

    try:
        redis_conn.setex(cache_key, PANEL_RECENT_TTL, html)
    except Exception:
        pass

    try:
        if rows:
            latest_row = rows[0]
            marker = {
                "latest_id": latest_row.id,
                "latest_updated_at": latest_row.updated_at.isoformat() if latest_row.updated_at else "",
            }
            _cache_set_json("panel:recent_requests:latest_marker", marker, ttl=10)
    except Exception:
        pass

    return HTMLResponse(content=html)


@app.post("/panel/promotions/remove")
def panel_remove_shared_promotion(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    shared_key = (payload.get("shared_key") or "").strip().upper()

    if not shared_key:
        return {"ok": False, "error": "SHARED_KEY_REQUIRED"}

    rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.shared_key == shared_key)
        .all()
    )

    if not rows:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    for row in rows:
        row.is_active = False
        row.used_actas = 0
        row.total_actas = 0
        row.promo_name = ""
        row.price_per_piece = ""
        row.client_key = None
        row.shared_key = None
        row.credit_abono = 0
        row.credit_debe = 0
        row.shared_group_limit_actas = None
        row.shared_group_used_actas = 0
        row.warning_sent_200 = False
        row.warning_sent_100 = False
        row.warning_sent_50 = False
        row.warning_sent_10 = False
        row.warning_sent_0 = False
        row.updated_at = _utc_now_naive()

    db.commit()

    try:
        _notify_client_groups_main(
            rows,
            "⚠️ *Promoción desactivada*\n\nLa promoción compartida de este cliente fue desactivada."
        )
    except Exception as e:
        print("PROMOTION_REMOVE_NOTIFY_ERROR =", str(e), flush=True)

    _clear_panel_cache()
    return {"ok": True, "shared_key": shared_key}


@app.post("/panel/promotions/set-group-limit")
def panel_set_shared_group_limit(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    group_jid = (payload.get("group_jid") or "").strip()
    limit_actas = int(payload.get("limit_actas") or 0)

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    row = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .first()
    )

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    if not (row.shared_key or "").strip():
        return {"ok": False, "error": "GROUP_NOT_IN_SHARED_PROMOTION"}

    row.shared_group_limit_actas = limit_actas if limit_actas > 0 else None
    row.updated_at = _utc_now_naive()
    db.commit()

    try:
        if limit_actas > 0:
            msg = f"""📦 *Actualización de promoción*
Se ha establecido un *límite individual* dentro de la promoción compartida.

🔢 Límite asignado: *{limit_actas} actas*

Este grupo podrá utilizar hasta esa cantidad dentro de la bolsa compartida.
"""
        else:
            msg = """📦 *Actualización de promoción*

Se eliminó el límite individual para este grupo.
Ahora puede usar libremente la bolsa compartida disponible.
"""
        send_group_text(group_jid, msg)

    except Exception as e:
        print("PROMO_LIMIT_NOTIFY_ERROR:", e)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Límite individual actualizado correctamente",
        "group_jid": group_jid,
        "shared_group_limit_actas": row.shared_group_limit_actas,
        "shared_group_used_actas": row.shared_group_used_actas or 0,
    }


@app.post("/panel/promotions/apply")
def panel_apply_shared_promotion(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    selected_group_jids = payload.get("selected_group_jids") or []
    promo_name = (payload.get("promo_name") or "").strip()
    price_per_piece = (payload.get("price_per_piece") or "").strip()
    client_key = (payload.get("client_key") or "").strip().upper()
    shared_key = (payload.get("shared_key") or "").strip().upper()
    total_actas = int(payload.get("total_actas") or 0)

    is_credit = bool(payload.get("is_credit") or False)
    credit_abono = int(payload.get("credit_abono") or 0)
    credit_debe = int(payload.get("credit_debe") or 0)
    shared_group_limit_actas = int(payload.get("shared_group_limit_actas") or 0)

    if not selected_group_jids:
        return {"ok": False, "error": "NO_GROUPS_SELECTED"}

    if total_actas <= 0:
        return {"ok": False, "error": "TOTAL_ACTAS_INVALID"}

    if not client_key:
        client_key = _promo_client_key(None, promo_name, promo_name or "PROMOCION_COMPARTIDA")

    if not shared_key:
        shared_key = client_key

    rows = []

    for group_jid in selected_group_jids:
        row = (
            db.query(GroupPromotion)
            .filter(GroupPromotion.group_jid == group_jid)
            .first()
        )

        if not row:
            row = GroupPromotion(
                group_jid=group_jid,
                promo_name=promo_name,
                client_key=client_key,
                shared_key=shared_key,
                total_actas=total_actas,
                used_actas=0,
                price_per_piece=price_per_piece,
                is_credit=is_credit,
                credit_abono=credit_abono,
                credit_debe=credit_debe,
                shared_group_limit_actas=shared_group_limit_actas or None,
                shared_group_used_actas=0,
                warning_sent_200=False,
                warning_sent_100=False,
                warning_sent_50=False,
                warning_sent_10=False,
                warning_sent_0=False,
                is_active=True,
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
            )
            db.add(row)
            db.flush()
        else:
            row.promo_name = promo_name or row.promo_name
            row.client_key = client_key
            row.shared_key = shared_key
            row.total_actas = total_actas
            row.used_actas = 0
            row.price_per_piece = price_per_piece
            row.is_credit = is_credit
            row.credit_abono = credit_abono
            row.credit_debe = credit_debe
            row.shared_group_limit_actas = shared_group_limit_actas or None
            row.shared_group_used_actas = 0
            row.warning_sent_200 = False
            row.warning_sent_100 = False
            row.warning_sent_50 = False
            row.warning_sent_10 = False
            row.warning_sent_0 = False
            row.is_active = True
            row.updated_at = _utc_now_naive()

        rows.append(row)

    db.commit()

    rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.shared_key == shared_key)
        .all()
    )

    try:
        _unblock_client_groups_main(rows)
    except Exception as unblock_exc:
        print("PROMOTION_AUTO_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    try:
        promo_label = promo_name or "paquete promocional"
        tipo_label = "crédito" if is_credit else "pagada"
        
        used_total = max((int(r.used_actas or 0) for r in rows), default=0)
        available = max(0, int(total_actas or 0) - used_total)

        _notify_client_groups_main(
            rows,
            (
                f"✅ *Promoción activada*\n\n"
                f"Tu promoción *{promo_label}* ya fue activada correctamente.\n"
                f"Tipo: *{tipo_label}*\n"
                f"Cuentas con *{available} actas disponibles*.\n\n"
                f"Este saldo aplica para todos los grupos asociados a esta promoción compartida.\n"
                f"Bolsa compartida: *{shared_key}*.\n\n"
                f"Gracias por tu preferencia."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_ACTIVATION_NOTIFY_ERROR =", str(notify_exc), flush=True)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Promoción compartida aplicada correctamente",
        "client_key": client_key,
        "shared_key": shared_key,
        "total_actas": total_actas,
        "is_credit": is_credit,
        "credit_abono": credit_abono,
        "credit_debe": credit_debe,
        "groups": selected_group_jids,
    }


@app.post("/panel/promotions/edit")
def panel_edit_promotion(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    group_jid = (payload.get("group_jid") or "").strip()

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    row = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .first()
    )

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    promo_name = (payload.get("promo_name") or "").strip()
    price_per_piece = (payload.get("price_per_piece") or "").strip()

    # OJO:
    # Aquí NO tocamos used_actas ni shared_group_used_actas.
    # Esta ruta es solo para editar datos de la promo.
    if promo_name:
        row.promo_name = promo_name

    if "total_actas" in payload:
        try:
            total_actas = int(payload.get("total_actas") or 0)
        except Exception:
            total_actas = 0

        if total_actas <= 0:
            return {"ok": False, "error": "TOTAL_ACTAS_INVALID"}

        used_now = int(row.used_actas or 0)

        if total_actas < used_now:
            return {
                "ok": False,
                "error": "TOTAL_ACTAS_LESS_THAN_USED",
                "used_actas": used_now,
                "total_actas": total_actas,
            }

        row.total_actas = total_actas

    if price_per_piece:
        row.price_per_piece = price_per_piece

    if "is_credit" in payload:
        row.is_credit = bool(payload.get("is_credit") or False)

    if "credit_abono" in payload:
        row.credit_abono = int(payload.get("credit_abono") or 0)

    if "credit_debe" in payload:
        row.credit_debe = int(payload.get("credit_debe") or 0)

    if "shared_group_limit_actas" in payload:
        limit_value = int(payload.get("shared_group_limit_actas") or 0)
        row.shared_group_limit_actas = limit_value if limit_value > 0 else None

    row.is_active = bool(payload.get("is_active", row.is_active))
    row.updated_at = _utc_now_naive()

    db.commit()
    _clear_panel_cache()

    return {
        "ok": True,
        "message": "Promoción editada sin reiniciar consumos",
        "group_jid": row.group_jid,
        "promo_name": row.promo_name,
        "total_actas": int(row.total_actas or 0),
        "used_actas": int(row.used_actas or 0),
        "available": max(0, int(row.total_actas or 0) - int(row.used_actas or 0)),
        "shared_group_limit_actas": row.shared_group_limit_actas,
        "shared_group_used_actas": int(row.shared_group_used_actas or 0),
        "is_active": bool(row.is_active),
    }


@app.post("/panel/promotions/recharge")
def panel_recharge_promotion(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    group_jid = (payload.get("group_jid") or "").strip()

    try:
        add_actas = int(payload.get("add_actas") or 0)
    except Exception:
        add_actas = 0

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    if add_actas <= 0:
        return {"ok": False, "error": "ADD_ACTAS_INVALID"}

    row = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .first()
    )

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    # OJO:
    # Recarga = sumar al total.
    # NO tocamos used_actas.
    previous_total = int(row.total_actas or 0)
    row.total_actas = previous_total + add_actas

    # Si estaba agotada, la reactivamos y liberamos avisos.
    row.is_active = True
    row.warning_sent_0 = False
    row.warning_sent_10 = False
    row.warning_sent_50 = False
    row.warning_sent_100 = False
    row.warning_sent_200 = False
    row.updated_at = _utc_now_naive()

    db.commit()

    try:
        unblock_group(group_jid)
    except Exception as unblock_exc:
        print("PROMO_RECHARGE_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    _clear_panel_cache()

    used_now = int(row.used_actas or 0)
    available = max(0, int(row.total_actas or 0) - used_now)

    return {
        "ok": True,
        "message": "Promoción recargada sin reiniciar consumos",
        "group_jid": row.group_jid,
        "previous_total": previous_total,
        "added": add_actas,
        "total_actas": int(row.total_actas or 0),
        "used_actas": used_now,
        "available": available,
        "is_active": bool(row.is_active),
    }


@app.post("/panel/promotions/add-group")
def panel_add_group_to_shared_promotion(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    group_jid = (payload.get("group_jid") or "").strip()
    shared_key = (payload.get("shared_key") or "").strip().upper()
    shared_group_limit_actas = int(payload.get("shared_group_limit_actas") or 0)

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    if not shared_key:
        return {"ok": False, "error": "SHARED_KEY_REQUIRED"}

    # Buscar una promoción activa existente de esa bolsa compartida
    leader = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.shared_key == shared_key,
            GroupPromotion.is_active == True
        )
        .order_by(GroupPromotion.updated_at.desc(), GroupPromotion.id.desc())
        .first()
    )

    if not leader:
        return {"ok": False, "error": "SHARED_PROMOTION_NOT_FOUND"}

    # Evitar duplicar si ya pertenece a esa misma bolsa activa
    existing_same = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid == group_jid,
            GroupPromotion.shared_key == shared_key,
            GroupPromotion.is_active == True
        )
        .first()
    )

    if existing_same:
        return {
            "ok": True,
            "message": "El grupo ya pertenece a esta bolsa compartida",
            "group_jid": group_jid,
            "shared_key": shared_key,
        }

    row = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .first()
    )

    if row:
        row.promo_name = leader.promo_name
        row.client_key = leader.client_key
        row.shared_key = leader.shared_key
        row.total_actas = leader.total_actas
        #row.used_actas = 0
        row.price_per_piece = leader.price_per_piece
        row.is_credit = leader.is_credit
        row.credit_abono = leader.credit_abono or 0
        row.credit_debe = leader.credit_debe or 0
        row.shared_group_limit_actas = shared_group_limit_actas or None
        row.shared_group_used_actas = 0
        row.warning_sent_200 = bool(leader.warning_sent_200)
        row.warning_sent_100 = bool(leader.warning_sent_100)
        row.warning_sent_50 = bool(leader.warning_sent_50)
        row.warning_sent_10 = bool(leader.warning_sent_10)
        row.warning_sent_0 = bool(leader.warning_sent_0)
        row.is_active = True
        row.updated_at = _utc_now_naive()
    else:
        row = GroupPromotion(
            group_jid=group_jid,
            promo_name=leader.promo_name,
            client_key=leader.client_key,
            shared_key=leader.shared_key,
            total_actas=leader.total_actas,
            used_actas=0,
            price_per_piece=leader.price_per_piece,
            is_credit=leader.is_credit,
            credit_abono=leader.credit_abono or 0,
            credit_debe=leader.credit_debe or 0,
            shared_group_limit_actas=shared_group_limit_actas or None,
            shared_group_used_actas=0,
            warning_sent_200=bool(leader.warning_sent_200),
            warning_sent_100=bool(leader.warning_sent_100),
            warning_sent_50=bool(leader.warning_sent_50),
            warning_sent_10=bool(leader.warning_sent_10),
            warning_sent_0=bool(leader.warning_sent_0),
            is_active=True,
            created_at=_utc_now_naive(),
            updated_at=_utc_now_naive(),
        )
        db.add(row)

    db.commit()

    rows = (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.shared_key == shared_key,
            GroupPromotion.is_active == True
        )
        .all()
    )

    try:
        _unblock_client_groups_main(rows)
    except Exception as unblock_exc:
        print("PROMOTION_ADD_GROUP_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    try:
        available = max(0, int(leader.total_actas or 0) - int(leader.used_actas or 0))
        promo_label = (leader.promo_name or "").strip() or "paquete promocional"
        tipo_label = "crédito" if leader.is_credit else "pagada"

        send_group_text(
            group_jid,
            (
                f"✅ *Grupo agregado a bolsa compartida*\n\n"
                f"Tu grupo fue agregado correctamente a la promoción *{promo_label}*.\n"
                f"Tipo: *{tipo_label}*\n"
                f"Bolsa compartida: *{shared_key}*\n"
                f"Saldo disponible actual: *{available} actas*."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_ADD_GROUP_NOTIFY_ERROR =", str(notify_exc), flush=True)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Grupo agregado correctamente a la bolsa compartida",
        "group_jid": group_jid,
        "shared_key": shared_key,
        "promo_name": leader.promo_name,
        "total_actas": leader.total_actas,
        "used_actas": leader.used_actas,
        "available": max(0, int(leader.total_actas or 0) - int(leader.used_actas or 0)),
    }
    

def _is_credit_promotion(row: GroupPromotion) -> bool:
    return bool(row.is_credit)


@app.post("/panel/group/{group_jid}/promotion/abono")
def panel_register_group_promotion_abono(
    group_jid: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    abono = int(payload.get("abono") or 0)

    if abono <= 0:
        return {"ok": False, "error": "ABONO_INVALIDO"}

    row = db.query(GroupPromotion).filter(GroupPromotion.group_jid == group_jid).first()

    if not row:
        return {"ok": False, "error": "PROMOCION_NO_ENCONTRADA"}

    if not row.is_credit:
        return {"ok": False, "error": "LA_PROMOCION_NO_ES_A_CREDITO"}

    row.credit_abono = (row.credit_abono or 0) + abono
    row.credit_debe = max(0, (row.credit_debe or 0) - abono)
    row.updated_at = _utc_now_naive()

    db.commit()

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Abono registrado correctamente",
        "group_jid": group_jid,
        "credit_abono": row.credit_abono,
        "credit_debe": row.credit_debe,
    }


@app.get("/panel/promotions/report", response_class=HTMLResponse)
def panel_promotions_report(db: Session = Depends(get_db)):
    group_cache = _build_group_name_cache(db)

    rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.is_active == True)
        .order_by(GroupPromotion.updated_at.desc(), GroupPromotion.id.desc())
        .all()
    )

    pagadas = []
    credito = []

    for r in rows:
        total_actas = r.total_actas or 0
        used_actas = r.used_actas or 0
        disponibles = max(0, total_actas - used_actas)

        item = {
            "group_jid": r.group_jid or "",
            "group_name": _group_name_cached(r.group_jid, group_cache),
            "promo_name": (r.promo_name or "").strip() or "-",
            "total_actas": total_actas,
            "used_actas": used_actas,
            "disponibles": disponibles,
            "price_per_piece": (r.price_per_piece or "").strip() or "-",
            "credit_abono": r.credit_abono or 0,
            "credit_debe": r.credit_debe or 0,
        }

        if _is_credit_promotion(r):
            credito.append(item)
        else:
            pagadas.append(item)

    def render_pagadas_rows(items: list[dict]) -> str:
        if not items:
            return '<tr><td colspan="7">Sin registros.</td></tr>'

        html = ""
        for i, r in enumerate(items, start=1):
            html += f"""
            <tr>
              <td>{i}</td>
              <td>{_esc(r["group_name"])}</td>
              <td>{_esc(r["promo_name"])}</td>
              <td class="right">{r["total_actas"]}</td>
              <td class="right">{r["used_actas"]}</td>
              <td class="right">{r["disponibles"]}</td>
              <td class="right">{_esc(r["price_per_piece"])}</td>
            </tr>
            """
        return html

    def render_credito_rows(items: list[dict]) -> str:
        if not items:
            return '<tr><td colspan="8">Sin registros.</td></tr>'

        html = ""
        for i, r in enumerate(items, start=1):
            html += f"""
            <tr>
              <td>{i}</td>
              <td>{_esc(r["group_name"])}</td>
              <td class="right">{r["total_actas"]}</td>
              <td class="right">{r["credit_abono"]}</td>
              <td class="right">{r["credit_debe"]}</td>
              <td class="right">{r["used_actas"]}</td>
              <td class="right">{r["disponibles"]}</td>
              <td class="right">
                <button class="action-btn" onclick="addCreditAbono('{_esc(r["group_jid"])}')">
                  Registrar abono
                </button>
              </td>
            </tr>
            """
        return html

    script_js = """
    <script>
    async function addCreditAbono(groupJid) {
      const value = prompt("Ingresa el abono:");
      if (!value) return;

      try {
        const res = await fetch(`/panel/group/${groupJid}/promotion/abono`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            abono: Number(value)
          })
        });

        const data = await res.json();

        if (data.ok) {
          alert("Abono registrado");
          location.reload();
        } else {
          alert(data.error || "Error registrando abono");
        }
      } catch (e) {
        alert("Error de conexión");
      }
    }
    </script>
    """

    html = f"""
    <!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <title>Reporte de Promociones</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        * {{
          box-sizing: border-box;
        }}

        body {{
          margin: 0;
          padding: 24px;
          font-family: Arial, Helvetica, sans-serif;
          background: #f5f7fb;
          color: #1f2937;
        }}

        .wrap {{
          max-width: 1320px;
          margin: 0 auto;
        }}

        .topbar {{
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 12px;
          margin-bottom: 18px;
          font-size: 13px;
          color: #6b7280;
        }}

        .topbar-right {{
          display: flex;
          align-items: center;
          gap: 10px;
        }}

        .print-btn {{
          padding: 10px 14px;
          border: none;
          border-radius: 10px;
          background: #111827;
          color: white;
          font-weight: 600;
          cursor: pointer;
        }}

        .print-btn:hover {{
          opacity: .92;
        }}

        h1 {{
          margin: 0 0 18px 0;
          font-size: 38px;
          color: #111827;
        }}

        .section {{
          background: #fff;
          border: 1px solid #e5e7eb;
          border-radius: 18px;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
          margin-bottom: 24px;
          overflow: hidden;
        }}

        .section-head {{
          padding: 18px 20px;
          border-bottom: 1px solid #e5e7eb;
          background: #f8fafc;
        }}

        .section-title {{
          margin: 0;
          font-size: 24px;
          color: #111827;
        }}

        .section-sub {{
          margin-top: 6px;
          color: #6b7280;
          font-size: 13px;
        }}

        .table-wrap {{
          overflow-x: auto;
        }}

        table {{
          width: 100%;
          border-collapse: collapse;
        }}

        thead th {{
          background: #f9fafb;
          color: #111827;
          text-align: left;
          font-size: 14px;
          padding: 14px;
          border-bottom: 1px solid #e5e7eb;
          white-space: nowrap;
        }}

        tbody td {{
          padding: 14px;
          border-bottom: 1px solid #eef2f7;
          vertical-align: top;
          font-size: 14px;
        }}

        tbody tr:hover {{
          background: #fafcff;
        }}

        .right {{
          text-align: right;
        }}

        .badge {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 4px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 700;
        }}

        .badge-paid {{
          background: #dcfce7;
          color: #166534;
        }}

        .badge-credit {{
          background: #fff7ed;
          color: #c2410c;
        }}

        .action-btn {{
          padding: 8px 12px;
          border: none;
          border-radius: 10px;
          background: #166534;
          color: white;
          font-weight: 700;
          cursor: pointer;
        }}

        .action-btn:hover {{
          opacity: .92;
        }}

        @media print {{
          body {{
            background: #fff;
            padding: 0;
          }}

          .section {{
            box-shadow: none;
            border-radius: 0;
            break-inside: avoid;
          }}

          .print-btn {{
            display: none !important;
          }}
        }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="topbar">
          <div>{datetime.now().strftime("%m/%d/%y, %H:%M")}</div>
          <div class="topbar-right">
            <span>Reporte de Promociones</span>
            <button class="print-btn" onclick="window.print()">Imprimir</button>
          </div>
        </div>

        <h1>Reporte de Promociones</h1>

        <div class="section">
          <div class="section-head">
            <h2 class="section-title"><span class="badge badge-paid">Pagadas</span></h2>
            <div class="section-sub">Actas por paquetes pagados.</div>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>No.</th>
                  <th>Cliente</th>
                  <th>Promoción</th>
                  <th class="right">Actas autorizadas</th>
                  <th class="right">Actas consumidas</th>
                  <th class="right">Restan</th>
                  <th class="right">Precio</th>
                </tr>
              </thead>
              <tbody>
                {render_pagadas_rows(pagadas)}
              </tbody>
            </table>
          </div>
        </div>

        <div class="section">
          <div class="section-head">
            <h2 class="section-title"><span class="badge badge-credit">Crédito</span></h2>
            <div class="section-sub">Actas autorizadas a crédito.</div>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>No.</th>
                  <th>Cliente</th>
                  <th class="right">Crédito</th>
                  <th class="right">Abono</th>
                  <th class="right">Debe</th>
                  <th class="right">Actas consumidas</th>
                  <th class="right">Restan</th>
                  <th class="right">Acción</th>
                </tr>
              </thead>
              <tbody>
                {render_credito_rows(credito)}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {script_js}
    </body>
    </html>
    """

    return HTMLResponse(content=html)


def _get_group_category(db: Session, group_jid: str) -> str:
    row = (
        db.query(GroupCategory)
        .filter(GroupCategory.group_jid == group_jid)
        .first()
    )
    return (row.category or "otro") if row else "otro"


def _set_group_category(db: Session, group_jid: str, category: str):
    row = (
        db.query(GroupCategory)
        .filter(GroupCategory.group_jid == group_jid)
        .first()
    )

    if row:
        row.category = category
        row.updated_at = _utc_now_naive()
    else:
        row = GroupCategory(
            group_jid=group_jid,
            category=category,
            created_at=_utc_now_naive(),
            updated_at=_utc_now_naive(),
        )
        db.add(row)

    db.commit()
    _clear_panel_cache()
    _clear_group_name_cache()
    return row


def _remove_group_category(db: Session, group_jid: str):
    row = (
        db.query(GroupCategory)
        .filter(GroupCategory.group_jid == group_jid)
        .first()
    )
    if row:
        db.delete(row)
        db.commit()
        _clear_panel_cache()


GROUP_CATEGORY_OPTIONS = [
    ("papeleria_ciber", "Papelería / Ciber"),
    ("gestor", "Gestor"),
    ("otro", "Otro"),
]


def _get_broadcast_target_groups(
    db: Session,
    target_category: str,
    selected_groups: list[str] | None = None,
    instance_name: str = MAIN_PANEL_INSTANCE,
) -> list[str]:

    selected_groups = selected_groups or []
    target_category = (target_category or "all").strip().lower()
    instance_name = (instance_name or MAIN_PANEL_INSTANCE).strip()

    rows = (
        db.query(AuthorizedGroup.group_jid)
        .filter(
            AuthorizedGroup.owner_instance == instance_name,
            or_(
                AuthorizedGroup.is_hidden == False,
                AuthorizedGroup.is_hidden.is_(None),
            ),
            or_(
                AuthorizedGroup.hidden_in_main == False,
                AuthorizedGroup.hidden_in_main.is_(None),
            ),
        )
        .all()
    )

    allowed_groups = set()

    for (gid,) in rows:
        gid = (gid or "").strip()

        if not gid:
            continue

        if "@g.us" not in gid:
            continue

        # Evita mandar a grupos bloqueados / agotados.
        if is_group_blocked(gid):
            continue

        group_name = _group_name(gid)

        # Evita proveedores, pruebas, AD, etc.
        if _is_hidden_panel_group(gid, group_name):
            continue

        allowed_groups.add(gid)

    if target_category == "all":
        return sorted(allowed_groups)

    if target_category == "manual":
        return sorted([
            g for g in selected_groups
            if (g or "").strip() in allowed_groups
        ])

    category_rows = (
        db.query(GroupCategory.group_jid)
        .filter(GroupCategory.category == target_category)
        .all()
    )

    category_groups = {
        (gid or "").strip()
        for (gid,) in category_rows
        if (gid or "").strip()
    }

    return sorted(allowed_groups.intersection(category_groups))


@app.post("/panel/group/{group_jid}/category")
async def panel_group_set_category(group_jid: str, request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    category = (payload.get("category") or "").strip().lower()

    allowed = {"papeleria_ciber", "gestor", "otro"}
    if category not in allowed:
        return {"ok": False, "error": "CATEGORIA_INVALIDA"}

    _set_group_category(db, group_jid, category)
    _clear_panel_cache()
    return {"ok": True}


@app.post("/panel/group/{group_jid}/category/remove")
def panel_group_remove_category(group_jid: str, db: Session = Depends(get_db)):
    _remove_group_category(db, group_jid)
    _clear_panel_cache()
    return {"ok": True}


@app.post("/panel/group/{group_jid}/shared-promotion/remove")
def remove_group_from_shared_promotion(
    group_jid: str,
    db: Session = Depends(get_db),
):
    group_jid = (group_jid or "").strip()

    if not group_jid:
        return {"ok": False, "error": "GROUP_JID_REQUIRED"}

    row = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .first()
    )

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    if not (row.shared_key or "").strip():
        return {"ok": False, "error": "GROUP_NOT_IN_SHARED_PROMOTION"}

    # guardar datos antes de quitarlo
    promo_name = row.promo_name or "Promoción compartida"

    # quitar de bolsa
    row.shared_key = None
    row.shared_group_limit_actas = None
    row.shared_group_used_actas = 0
    row.used_actas = 0
    row.total_actas = 0
    row.updated_at = _utc_now_naive()

    db.commit()

    try:
        msg = f"""
📦 *Actualización de promoción*
Este grupo fue retirado de la *bolsa compartida*:

🏷 Promoción: *{promo_name}*

A partir de ahora este grupo ya no utilizará el saldo compartido.
"""
        send_group_text(group_jid, msg)

    except Exception as e:
        print("SHARED_PROMO_REMOVE_NOTIFY_ERROR:", e)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "El grupo fue eliminado de la bolsa compartida",
        "group_jid": group_jid,
    }


def _get_group_acta_price(db: Session, group_jid: str) -> float:
    row = (
        db.query(GroupAlias)
        .filter(GroupAlias.group_jid == group_jid)
        .first()
    )

    if not row:
        return 0.0

    try:
        return float(row.acta_price or 0)
    except Exception:
        return 0.0


def _set_group_acta_price(db: Session, group_jid: str, price: float):
    row = (
        db.query(GroupAlias)
        .filter(GroupAlias.group_jid == group_jid)
        .first()
    )

    if not row:
        row = GroupAlias(
            group_jid=group_jid,
            custom_name="",
            acta_price=price,
            updated_at=_utc_now_naive(),
        )
        db.add(row)
    else:
        row.acta_price = price
        row.updated_at = _utc_now_naive()

    db.commit()
    return row


@app.post("/panel/group/{group_jid}/acta-price")
async def panel_save_group_acta_price(
    group_jid: str,
    request: Request,
    db: Session = Depends(get_db),
):
    try:
        data = await request.json()
        price_raw = str(data.get("acta_price", "")).strip()

        if not price_raw:
            return {"ok": False, "error": "Falta precio"}

        price = float(price_raw)

        if price < 0:
            return {"ok": False, "error": "El precio no puede ser negativo"}

        _set_group_acta_price(db, group_jid, price)

        _clear_panel_cache()
        return {
            "ok": True,
            "message": "Precio guardado correctamente",
            "acta_price": price,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/panel/group-detail", response_class=HTMLResponse)
def panel_group_detail(
    group_jid: str = "",
    view: str = "month",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    if not group_jid:
        return HTMLResponse("<pre>Falta group_jid</pre>", status_code=400)

    cache_key = "panel:group_detail:" + "|".join([
        (group_jid or "").strip(),
        (view or "month").strip(),
        (date_from or "").strip(),
        (date_to or "").strip(),
    ])
    cached_html = redis_conn.get(cache_key)
    if cached_html:
        if isinstance(cached_html, bytes):
            cached_html = cached_html.decode("utf-8", errors="ignore")
        return HTMLResponse(content=cached_html)

    group_cache = _build_group_name_cache(db)

    promo = _get_group_promotion(db, group_jid)
    #promo = _sync_promo_used_from_logs(db, promo)
    promo_html = _promotion_badge_html(promo)
    group_display_name = _esc(_group_name_cached(group_jid, group_cache))
    promo_name = _esc(promo.promo_name if promo else "")
    promo_total = promo.total_actas if promo else 0
    promo_used = promo.used_actas if promo else 0
    promo_available = _promotion_available(promo) if promo else 0
    promo_price = _esc(promo.price_per_piece if promo else "")

    promo_is_credit = bool(promo.is_credit) if promo else False
    promo_credit_abono = promo.credit_abono if promo else 0
    promo_credit_debe = promo.credit_debe if promo else 0
    promo_type_label = "Crédito" if promo_is_credit else "Pagada"
    promo_shared_group_limit = promo.shared_group_limit_actas if promo else 0
    group_category = _get_group_category(db, group_jid)

    acta_price_num = _get_group_acta_price(db, group_jid)

    time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)

    rows = (
        db.query(
            RequestLog.created_at,
            RequestLog.status,
        )
        .filter(
            RequestLog.created_at >= time_min,
            RequestLog.created_at < time_max,
            RequestLog.source_group_id == group_jid,
        )
        .order_by(RequestLog.created_at.asc())
        .all()
    )

    days = {}

    local_start = _to_panel_tz(time_min)
    local_end = _to_panel_tz(time_max)
    
    if not local_start or not local_end:
        now_local = _panel_now()
        local_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = local_start + timedelta(days=1)

    cur = local_start
    while cur < local_end:
        day_str = cur.strftime("%Y-%m-%d")
        days[day_str] = {
            "day_name": _day_name_es_from_date(day_str),
            "date": day_str,
            "total": 0,
            "done": 0,
            "error": 0,
            "queued": 0,
            "processing": 0,
        }
        cur += timedelta(days=1)

    for created_at, status_value in rows:
        if not created_at:
            continue
        local_dt = _to_panel_tz(created_at)
        day_str = local_dt.strftime("%Y-%m-%d")
        if day_str not in days:
            continue

        item = days[day_str]
        item["total"] += 1

        if status_value == "DONE":
            item["done"] += 1
        elif status_value == "ERROR":
            item["error"] += 1
        elif status_value == "QUEUED":
            item["queued"] += 1
        elif status_value == "PROCESSING":
            item["processing"] += 1

    rows_out = list(days.values())
    rows_out.sort(key=lambda x: x["date"])

    detail = {
        "group_jid": group_jid,
        "group_name": _group_name_cached(group_jid, group_cache),
        "rows": rows_out,
        "totals": {
            "total": sum(x["total"] for x in rows_out),
            "done": sum(x["done"] for x in rows_out),
            "error": sum(x["error"] for x in rows_out),
            "queued": sum(x["queued"] for x in rows_out),
            "processing": sum(x["processing"] for x in rows_out),
        },
        "date_from": local_start.strftime("%Y-%m-%d"),
        "date_to": (local_end - timedelta(days=1)).strftime("%Y-%m-%d"),
        "view": view,
    }

    title = detail["group_name"]
    if view == "custom":
        subtitle = f"Historial personalizado: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
    elif view == "month":
        subtitle = f"Historial mes actual: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
    elif view == "prev_month":
        subtitle = f"Historial mes anterior: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
    elif view == "30d":
        subtitle = f"Historial últimos 30 días: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
    else:
        subtitle = f"Historial diario: {detail['date_from']} ({PANEL_TZ})"

    html = f"""
    <!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <title>{_esc(title)}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{
          font-family: Arial, sans-serif;
          background: #f4f6f8;
          margin: 0;
          padding: 16px;
          color: #1f2937;
        }}
        .wrap {{
          max-width: 1400px;
          margin: 0 auto;
        }}
        .hero {{
          background: linear-gradient(135deg, #061533 0%, #0b1f4d 100%);
          color: white;
          border-radius: 22px;
          padding: 20px 24px;
          margin-bottom: 18px;
        }}
        .hero a {{
          color: white;
          text-decoration: none;
          font-weight: 700;
          display: inline-block;
          margin-bottom: 14px;
        }}
        .hero h1 {{
          margin: 0 0 8px;
          font-size: 2rem;
        }}
        .hero-sub {{
          color: rgba(255,255,255,.9);
          font-size: 1rem;
        }}
        .box {{
          background: white;
          border-radius: 20px;
          overflow: hidden;
          box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
          margin-bottom: 18px;
        }}
        .head {{
          padding: 16px 18px;
          border-bottom: 1px solid #e5e7eb;
          background: #fafbfc;
        }}
        .filters {{
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 10px;
          padding: 16px;
        }}
        .filters input {{
          width: 100%;
          padding: 11px 12px;
          border: 1px solid #d1d5db;
          border-radius: 12px;
          font: inherit;
          background: white;
          color: #1f2937;
          outline: none;
          box-sizing: border-box;
        }}
        .filters input:focus {{
          border-color: #334155;
          box-shadow: 0 0 0 3px rgba(51, 65, 85, .10);
        }}
        .filters select {{
          width: 100%;
          padding: 11px 12px;
          border: 1px solid #d1d5db;
          border-radius: 12px;
          font: inherit;
          background: white;
          color: #1f2937;
          outline: none;
          box-sizing: border-box;
        }}
        .filters select:focus {{
          border-color: #334155;
          box-shadow: 0 0 0 3px rgba(51, 65, 85, .10);
        }}
        .btn {{
          border: none;
          border-radius: 12px;
          padding: 10px 14px;
          font-weight: 800;
          font-size: .95rem;
          cursor: pointer;
          font-family: inherit;
        }}
        .btn-primary {{
          background: #334155;
          color: white;
        }}
        .btn-success {{
          background: #166534;
          color: white;
        }}
        .btn-danger {{
          background: #b91c1c;
          color: white;
        }}
        .small {{
          color: #6b7280;
          font-size: .84rem;
          line-height: 1.45;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
        }}
        th, td {{
          padding: 16px;
          border-bottom: 1px solid #e5e7eb;
          text-align: left;
          font-size: 1rem;
        }}
        th {{
          background: #061533;
          color: white;
        }}
        .right {{
          text-align: right;
        }}
        .total-row td {{
          font-weight: 800;
          background: #f8fafc;
        }}
        .weekly-row td {{
          font-weight: 800;
          background: #dbeafe;
          color: #020617;
        }}
        @media (max-width: 900px) {{
          .filters {{
            grid-template-columns: 1fr;
          }}
        }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="hero">
          <a href="javascript:history.back()">← Volver al historial</a>
          <h1>{_esc(title)}</h1>
          <div class="hero-sub">{_esc(subtitle)}</div>
        </div>
    """

    html += f"""
        <div class="box">
          <div class="head"><strong>Nombre del grupo</strong></div>

          <div class="filters" style="grid-template-columns: minmax(0, 1fr) 360px;">
            <div>
              <div class="small">Nombre personalizado</div>
              <input id="group_custom_name" placeholder="Escribe el nombre del grupo" value="{group_display_name}">
            </div>

            <div style="display:flex;align-items:end;gap:10px;">
              <button 
                type="button" 
                class="btn btn-primary"
                style="flex:1;white-space:nowrap;"
                onclick="saveGroupName('{group_jid}')">
                Guardar nombre
              </button>
            
              <button 
                type="button" 
                class="btn btn-warning"
                style="flex:1;white-space:nowrap;"
                onclick="pingGroup('{group_jid}')">
                Ping grupo
              </button>
            </div>
          </div>
        </div>
    """

    html += f"""
        <div class="box">
          <div class="head"><strong>Categoría del grupo</strong></div>
        
          <div class="filters" style="grid-template-columns: minmax(0, 1fr) 220px 220px;">
            <div>
              <div class="small">Categoría actual</div>
              <select id="group_category">
                <option value="papeleria_ciber" {"selected" if group_category == "papeleria_ciber" else ""}>Papelería / Ciber</option>
                <option value="gestor" {"selected" if group_category == "gestor" else ""}>Gestor</option>
                <option value="otro" {"selected" if group_category == "otro" else ""}>Otro</option>
              </select>
            </div>
        
            <div style="display:flex;align-items:end;">
              <button type="button" class="btn btn-primary" style="width:100%;" onclick="saveGroupCategory('{group_jid}')">
                Guardar categoría
              </button>
            </div>
        
            <div style="display:flex;align-items:end;">
              <button type="button" class="btn btn-danger" style="width:100%;" onclick="removeGroupCategory('{group_jid}')">
                Quitar categoría
              </button>
            </div>
          </div>
        </div>
    """

    is_in_shared_promo = bool((promo.shared_key or "").strip()) if promo else False

    shared_remove_btn = (
        f'<button type="button" class="btn btn-danger" onclick="removeFromSharedPromotion(\'{group_jid}\')">Quitar de bolsa</button>'
        if is_in_shared_promo
        else ""
    )
    
    html += f"""
        <div class="box">
          <div class="head"><strong>Promoción del grupo</strong></div>
    
          <div class="filters" style="grid-template-columns: repeat(5, minmax(0, 1fr));">
            <div>
              <div class="small">Estado</div>
              <div style="margin-top:8px;">{promo_html}</div>
            </div>
            <div>
              <div class="small">Tipo</div>
              <div style="margin-top:8px;font-weight:800;">{promo_type_label}</div>
            </div>
            <div>
              <div class="small">Promoción</div>
              <div style="margin-top:8px;font-weight:800;">{promo_name or 'Sin nombre'}</div>
            </div>
            <div>
              <div class="small">Total / Usadas / Disponibles</div>
              <div style="margin-top:8px;font-weight:800;">{promo_total} / {promo_used} / {promo_available}</div>
            </div>
            <div>
              <div class="small">Precio</div>
              <div style="margin-top:8px;font-weight:800;">{promo_price or 'N/D'}</div>
            </div>
          </div>
    
          <div class="filters" style="grid-template-columns: repeat(6, minmax(0, 1fr));">
            <div>
              <div class="small">Nombre de promoción</div>
              <input id="promo_name" placeholder="" value="{promo_name}">
            </div>
        
            <div>
              <div class="small">Tipo</div>
              <select id="promo_type">
                <option value="paid" {"selected" if not promo_is_credit else ""}>Pagada</option>
                <option value="credit" {"selected" if promo_is_credit else ""}>Crédito</option>
              </select>
            </div>
        
            <div>
              <div class="small">Total de actas</div>
              <input id="promo_total" placeholder="" type="number" min="1" value="{promo_total if promo_total else ''}">
            </div>
          
            <div>
              <div class="small">Precio por pieza</div>
              <input id="promo_price" placeholder="" value="{promo_price}">
            </div>
        
            <div>
              <div class="small">Abono</div>
              <input id="promo_credit_abono" type="number" min="0"
              value="{promo_credit_abono if promo_is_credit else ''}"
              placeholder="N/A">
            </div>
        
            <div>
              <div class="small">Debe</div>
              <input id="promo_credit_debe" type="number" min="0"
              value="{promo_credit_debe if promo_is_credit else ''}"
              placeholder="N/A">
            </div>
          </div>
    
          <div class="filters">
            <button type="button" class="btn btn-success" onclick="savePromotion('{group_jid}')">Activar promoción</button>
            {shared_remove_btn}
          </div>
    
          <div class="filters" style="grid-template-columns: minmax(0, 1fr) 220px;">
            <div>
              <div class="small">Límite individual dentro de bolsa compartida</div>
              <input id="shared_group_limit" type="number" min="0"
                     placeholder="Sin límite"
                     value="{promo_shared_group_limit if promo_shared_group_limit else ''}">
            </div>
    
            <div style="display:flex;align-items:end;">
              <button type="button" class="btn btn-primary" style="width:100%;" onclick="setSharedGroupLimit('{group_jid}')">
                Guardar límite
              </button>
            </div>
          </div>
    
          <div class="filters" style="grid-template-columns: 1fr 220px 220px;">
            <input id="promo_recharge" placeholder="Recargar actas" type="number" min="1">
            <button type="button" class="btn btn-success" onclick="rechargePromotion('{group_jid}')">Recargar promoción</button>
            <button type="button" class="btn btn-danger" onclick="removePromotion('{group_jid}')">Quitar promoción</button>
          </div>
        </div>
    """

    html += f"""
        <div class="box">
          <div class="head"><strong>Precio de acta</strong></div>
          
          <div class="filters" style="grid-template-columns: 220px 180px;">
            <div>
              <div class="small">Precio por acta</div>
              <input 
                id="acta_price" 
                type="number" 
                step="0.01" 
                min="0" 
                value="{acta_price_num}"
              >
            </div>
    
            <div style="display:flex;align-items:end;">
              <button 
                type="button" 
                class="btn btn-primary" 
                style="width:100%;" 
                onclick="saveActaPrice('{group_jid}')">
                Guardar precio
              </button>
            </div>
          </div>
        </div>
    """

    html += f"""
        <div class="box">
          <table>
            <thead>
              <tr>
                <th>Día</th>
                <th>Fecha</th>
                <th class="right">Total</th>
                <th class="right">Hecho</th>
                <th class="right">Error</th>
                <th class="right">En cola</th>
                <th class="right">Procesando</th>
                <th class="right">Precio</th>
                <th class="right">$ Hecho</th>
              </tr>
            </thead>
            <tbody>
    """

    weekly_total = 0
    weekly_done = 0
    weekly_error = 0
    weekly_queued = 0
    weekly_processing = 0
    weekly_start = None
    
    for r in detail["rows"]:
        if weekly_start is None:
            weekly_start = r["date"]
    
        weekly_total += r["total"]
        weekly_done += r["done"]
        weekly_error += r["error"]
        weekly_queued += r["queued"]
        weekly_processing += r["processing"]
        done_amount = r["done"] * acta_price_num
    
        html += f"""
              <tr>
                <td>{_esc(r["day_name"])}</td>
                <td>{_esc(r["date"])}</td>
                <td class="right">{r["total"]}</td>
                <td class="right">{r["done"]}</td>
                <td class="right">{r["error"]}</td>
                <td class="right">{r["queued"]}</td>
                <td class="right">{r["processing"]}</td>
                <td class="right">${acta_price_num:,.2f}</td>
                <td class="right">${done_amount:,.2f}</td>
              </tr>
        """
    
        is_sunday = r["day_name"].upper() == "DOMINGO"
        is_last_day = r == detail["rows"][-1]
    
        if is_sunday or is_last_day:
            weekly_amount = weekly_done * acta_price_num
            html += f"""
              <tr class="weekly-row">
                <td>CORTE SEMANAL</td>
                <td>{_esc(weekly_start)} a {_esc(r["date"])}</td>
                <td class="right">{weekly_total}</td>
                <td class="right">{weekly_done}</td>
                <td class="right">{weekly_error}</td>
                <td class="right">{weekly_queued}</td>
                <td class="right">{weekly_processing}</td>
                <td class="right">${acta_price_num:,.2f}</td>
                <td class="right">${weekly_amount:,.2f}</td>
              </tr>
            """
    
            weekly_total = 0
            weekly_done = 0
            weekly_error = 0
            weekly_queued = 0
            weekly_processing = 0
            weekly_start = None

    t = detail["totals"]
    total_amount = t["done"] * acta_price_num
    html += f"""
              <tr class="total-row">
                <td colspan="2">TOTAL</td>
                <td class="right">{t["total"]}</td>
                <td class="right">{t["done"]}</td>
                <td class="right">{t["error"]}</td>
                <td class="right">{t["queued"]}</td>
                <td class="right">{t["processing"]}</td>
                <td class="right">${acta_price_num:,.2f}</td>
                <td class="right">${total_amount:,.2f}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <script>
          async function saveActaPrice(groupJid) {{
            const price = document.getElementById("acta_price")?.value?.trim() || "";

            if (!price) {{
              alert("Ingresa el precio del acta");
              return;
            }}

            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/acta-price`, {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  acta_price: price
                }})
              }});

              const data = await res.json();

              if (data.ok) {{
                alert("Precio guardado");
                location.reload();
              }} else {{
                alert(data.error || "Error guardando precio");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}
          
          async function savePromotion(groupJid) {{
            const promoName = document.getElementById("promo_name")?.value?.trim() || "";
            const totalActas = document.getElementById("promo_total")?.value?.trim() || "";
            const pricePerPiece = document.getElementById("promo_price")?.value?.trim() || "";

            const promoType = document.getElementById("promo_type")?.value || "paid";
            const isCredit = promoType === "credit";
            
            let creditAbono = document.getElementById("promo_credit_abono")?.value?.trim() || "";
            let creditDebe = document.getElementById("promo_credit_debe")?.value?.trim() || "";
            
            if (!isCredit) {{
              creditAbono = "0";
              creditDebe = "0";
            }} else {{
              if (creditAbono === "") creditAbono = "0";
              if (creditDebe === "") creditDebe = "0";
            }}

            if (!totalActas) {{
              alert("Ingresa el total de actas");
              return;
            }}

            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/promotion`, {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  promo_name: promoName,
                  total_actas: totalActas,
                  price_per_piece: pricePerPiece,
                  is_credit: isCredit,
                  credit_abono: creditAbono,
                  credit_debe: creditDebe
                }})
              }});

              const data = await res.json();

              if (data.ok) {{
                alert(data.message || "Promoción guardada");
                location.reload();
              }} else {{
                alert(data.error || "Error guardando promoción");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          async function removeFromSharedPromotion(groupJid) {{
            const ok = confirm("¿Seguro que deseas sacar este grupo de la bolsa compartida?");
            if (!ok) return;
        
            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/shared-promotion/remove`, {{
                method: "POST"
              }});
        
              const data = await res.json();
        
              if (data.ok) {{
                alert(data.message || "Grupo eliminado de la bolsa compartida");
                location.reload();
              }} else {{
                alert(data.error || "Error quitando el grupo de la bolsa compartida");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          async function saveGroupCategory(groupJid) {{
            const category = document.getElementById("group_category")?.value || "otro";
        
            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/category`, {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{ category }})
              }});
        
              const data = await res.json();
        
              if (data.ok) {{
                alert("Categoría guardada correctamente");
                location.reload();
              }} else {{
                alert(data.error || "Error guardando categoría");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}
        
          async function removeGroupCategory(groupJid) {{
            if (!confirm("¿Quitar categoría de este grupo?")) return;
        
            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/category/remove`, {{
                method: "POST"
              }});
        
              const data = await res.json();
        
              if (data.ok) {{
                alert("Categoría eliminada");
                location.reload();
              }} else {{
                alert(data.error || "Error quitando categoría");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          async function setSharedGroupLimit(groupJid) {{
            const limit = document.getElementById("shared_group_limit")?.value?.trim() || "0";

            try {{
              const res = await fetch("/panel/promotions/set-group-limit", {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  group_jid: groupJid,
                  limit_actas: Number(limit || 0)
                }})
              }});

              const data = await res.json();

              if (data.ok) {{
                alert(data.message || "Límite actualizado");
                location.reload();
              }} else {{
                alert(data.error || "Error actualizando límite");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          function toggleGroupCreditFields() {{
            const promoType = document.getElementById("promo_type");
            const isCredit = promoType && promoType.value === "credit";

            const abono = document.getElementById("promo_credit_abono");
            const debe = document.getElementById("promo_credit_debe");

            if (abono) {{
              if (isCredit) {{
                abono.disabled = false;
                if (!abono.value) abono.value = 0;
              }} else {{
                abono.disabled = true;
                abono.value = "";
              }}
            }}
        
            if (debe) {{
              if (isCredit) {{
                debe.disabled = false;
                if (!debe.value) debe.value = 0;
              }} else {{
                debe.disabled = true;
                debe.value = "";
              }}
            }}
          }}

          document.addEventListener("DOMContentLoaded", () => {{
            const promoType = document.getElementById("promo_type");
            if (promoType) {{
              promoType.addEventListener("change", toggleGroupCreditFields);
              toggleGroupCreditFields();
            }}
          }});

          async function rechargePromotion(groupJid) {{
            const extraActas = document.getElementById("promo_recharge")?.value?.trim() || "";

            if (!extraActas) {{
              alert("Ingresa cuántas actas deseas recargar");
              return;
            }}

            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/promotion/recharge`, {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  extra_actas: extraActas
                }})
              }});

              const data = await res.json();

              if (data.ok) {{
                alert(data.message || "Recarga aplicada");
                location.reload();
              }} else {{
                alert(data.error || "Error aplicando recarga");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          async function removePromotion(groupJid) {{
            const ok = confirm("¿Seguro que deseas quitar la promoción de este grupo?");
            if (!ok) return;

            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/promotion/remove`, {{
                method: "POST"
              }});

              const data = await res.json();

              if (data.ok) {{
                alert(data.message || "Promoción desactivada");
                location.reload();
              }} else {{
                alert(data.error || "Error quitando promoción");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}

          async function pingGroup(groupJid) {{
            if (!confirm("¿Enviar ping a este grupo?")) return;
        
            const res = await fetch("/panel/ping-group", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                group_jid: groupJid
              }})
            }});
        
            const data = await res.json();
          
            if (data.ok) {{
              alert("Ping enviado");
            }} else {{
              alert(data.error || "Error enviando ping");
            }}
          }}

          async function saveGroupName(groupJid) {{
            const customName = document.getElementById("group_custom_name")?.value?.trim() || "";
        
            if (!customName) {{
              alert("Ingresa el nombre del grupo");
              return;
            }}
        
            try {{
              const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/name`, {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  custom_name: customName
                }})
              }});
         
              const data = await res.json();
        
              if (data.ok) {{
                alert("Nombre guardado correctamente");
                location.reload();
              }} else {{
                alert(data.error || "Error guardando nombre");
              }}
            }} catch (e) {{
              alert("No se pudo conectar con el servidor");
            }}
          }}
      </script>
    </body>
    </html>
    """

    try:
        redis_conn.setex(cache_key, PANEL_GROUP_DETAIL_TTL, html)
    except Exception:
        pass

    return HTMLResponse(content=html)


BROADCAST_ACTIVAS_MSG = """🚀 *INICIAMOS CON EL SERVICIO*

⚡ *ACTAS SUPER RÁPIDAS SALIENDO EN SEGUNDOS*

💫 *MANDEN, MANDEN* 💫

*SOLICÍTALAS POR:*
• CURP
• CADENA
• CÓDIGO DE VERIFICACIÓN
• CON FOLIO O SIN FOLIO

🕘 *HORARIO*
Lunes a Domingo
07:00 AM a 12:00 AM
"""

BROADCAST_RESTABLECIDO_MSG = """⚡⚡⚡ *SERVICIO SUPER RÁPIDO* ⚡⚡⚡
🟢 *RESTABLECIDO*

💫 *MANDEN, MANDEN* 💫
"""

BROADCAST_SUSPENDIDO_MSG = """⛔ *DOCU EXPRES SUSPENDIDO TEMPORALMENTE*

Por el momento el servicio está suspendido temporalmente.
En cuanto vuelva a operar les avisaremos por este medio.
Gracias por su paciencia.
"""

BROADCAST_CERRADO_MSG = """⚡⚡⚡ *SISTEMA DE ACTAS CERRADO* ⚡⚡⚡

📌 *GRACIAS POR SU PREFERENCIA*
"""


GROUP_NAME_MAP = {
    "120363406806549379@g.us": "Actas Pruebas 1",
    "120363425323721713@g.us": "Actas Pruebas 2",
    "120363407066931119@g.us": "Actas Pruebas 3",
    "120363423379615090@g.us": "Prov Mesino",
    "120363426725671842@g.us": "Prov Pruebas 1",
    "120363408272742958@g.us": "Prov Pruebas 2",
    "120363423566277284@g.us": "Prov Normal 1",
    "120363423915019779@g.us": "Prov Normal 2",
    "120363424509175054@g.us": "Prov Normal 3",
    "120363426176817361@g.us": "Prov Normal 4",
    "120363409870423163@g.us": "Prov Especial 1",
    "120363408639542108@g.us": "AD 1",
    "120363427054214985@g.us": "AD 2",
    "120363409374690453@g.us": "AD 3",
    "120363424119914828@g.us": "SURESTE",
    "120363408943747132@g.us": "LUIS SID",
    "120363407592512859@g.us": "ESCALANTE",
    "120363422785755828@g.us": "Gpo. No. 4 Karen",
    "120363426949877636@g.us": "Gpo. No. 11 Morelos",
    "120363425014097597@g.us": "Gpo. No. 7 Karen Marvin",
    "120363425275514736@g.us": "Gpo. No. 8 Ana Marvin",
    "120363406182077605@g.us": "Gpo. No. 12 Marvin",
    "120363425721043776@g.us": "Gpo. No. 3 Gestoria Maya 1",
    "120363424204506742@g.us": "Gpo. No. 51 PR Mesino",
    "120363403551029435@g.us": "Gpo. No. 18 Barranco",
    "120363421166637606@g.us": "Gpo. No. 14 Hiro",
    "120363406888061577@g.us": "Gpo. No. 31 Barcelo",
    "120363407761523786@g.us": "Gpo. No. 59 Max",
    "120363425287655854@g.us": "Gpo. No. 28 David",
    "120363424740372709@g.us": "Gpo. No. 57 Isidro",
    "120363424031837828@g.us": "Gpo. No. 52 Pereyra",
    "120363408668441985@g.us": "Gpo. No. 42 Arturo",
    "120363404207028239@g.us": "Gpo. No. 24 Beto",
    "120363421694580090@g.us": "Gpo. No. 37 Loez",
    "120363427788039518@g.us": "Docify Mx 1 - Oziel",
    "120363424360403186@g.us": "Docify Mx 2 - Aaron",
    "120363406562422137@g.us": "Gpo. No. 1 Max",
    "120363406732530093@g.us": "Gpo. No. 2 Max",
    "120363424567042045@g.us": "Gpo. No. 3 Max",
    "120363425693310093@g.us": "Gpo. No. 4 Max",
    "120363409605873826@g.us": "Gpo. No. 5 Max",
    "120363405311596556@g.us": "Gpo. No. 6 Max",
    "120363425419227686@g.us": "Gpo. No. 7 Max",
    "120363424900187969@g.us": "Gpo. No. 8 Max",
    "120363405222548044@g.us": "Gpo. No. 9 Max",
    "120363407067510623@g.us": "Gpo. No. 10 Max",
    "120363404620511153@g.us": "Gpo. No. 11 Max",
    "120363424829883028@g.us": "Gpo. No. 12 Max",
    "120363407417260200@g.us": "Gpo. No. 13 Max",
    "120363422073988332@g.us": "Gpo. No. 13 Day",
    "120363423887399966@g.us": "Gpo. No. 2 Lesli",
    "120363407701598429@g.us": "Gpo. No. 20 Altas IMSS",
    "120363425702893567@g.us": "Gpo. No. 46 Papeleria MC",
    "120363424321234737@g.us": "Gpo. No. 56 Broder Zihua",
    "120363407168361684@g.us": "Gpo. No. 38 Tramites Ana",
    "120363406276735177@g.us": "Gpo. No. 22 Servi Todo",
    "120363423160777316@g.us": "Gpo. No. 15 Cancun",
    "120363406102408537@g.us": "Gpo. No. 10 Miguel",
    "120363422772430647@g.us": "Gpo. No. 19 Kedetalle",
    "120363408638261814@g.us": "Gpo. No. 30 Gestoria AC",
    "120363408050310070@g.us": "Gpo. No. 45 Sercomex",
    "120363406424667967@g.us": "Gpo. No. 53 Carlos Treviño",
    "120363423784091430@g.us": "Gpo. No. 62 Nordik Leal",
    "120363424864418952@g.us": "Gpo. No. 25 Gestoria Martinez",
    "120363427994370611@g.us": "Gpo. No. 61 Mely",
    "120363422330207518@g.us": "Gpo. No. 23 Delfino",
    "120363408311828293@g.us": "Gpo. No. 49 Armando",
    "120363405736245075@g.us": "Gpo. No. 43 Cibert San Luis",
    "120363404351044596@g.us": "Gpo. No. 34 Zenitran",
    "120363409641104856@g.us": "Gpo. No. 9 Diego",
    "120363422789316023@g.us": "Gpo. No. 16 Vallarta",
    "120363424015683577@g.us": "Gpo. No. 21 Ana Pineda",
    "120363424277043543@g.us": "Gpo. No. 26 Juan Carlos",
    "120363430748954270@g.us": "Gpo. No. 50 Yuni",
    "120363421058595249@g.us": "Gpo. No. 47 Airenet",
    "120363422560457092@g.us": "Gpo. No. 29 Elaine",
    "120363404803905766@g.us": "Gpo. No. 40 Imperio",
    "120363424595029370@g.us": "Gpo. No. 54 Adriana",
    "120363421296099572@g.us": "Gpo. No. 39 Susana",
    "120363424674106871@g.us": "Gpo. No. 32 Papeleria Leo",
    "120363424414421234@g.us": "Gpo. No. 5 Rosas Reclutador",
    "120363407025228491@g.us": "Gpo. No. 1 Gestoria Docu Express",
    "120363424851734635@g.us": "Gpo. No. 17 Svs. Digitales",
    "120363424333002785@g.us": "Gpo. No. 33 Miscelanea Batallon",
    "120363401894657087@g.us": "Gpo. No. 36 Belladira",
    "120363408050345917@g.us": "Gpo. No. 33 Docs",
    "120363423353879965@g.us": "Gpo. No. 44 Nadia",
    "120363422771877743@g.us": "Gpo. No. 48 Aliados Rurales",
    "120363427738529897@g.us": "Gpo. No. 63 Grupo Maya 2",
    "120363426763609841@g.us": "Gpo. No. 27 Comida Master",
    "120363425053127323@g.us": "Gpo. No. 64 Panchinko Actas",
    "120363407565721999@g.us": "Docify Mx 3 - General",
    "120363421862592214@g.us": "Gpo. No. 71 Lazaro 1",
    "120363425433931286@g.us": "Gpo. No. 65 Gestoria Guerrero",
    "120363425691947112@g.us": "Gpo. No. 70 Lazaro 2",
    "120363427243510324@g.us": "Gpo. Grupos SN Gestoria Educativa",
    "120363406217452557@g.us": "Gpo. No. 8 Cristina",
    "120363407739117517@g.us": "Gpo. No. 9 Cristina",
    "120363406363506819@g.us": "Gpo. No. 10 Cristina",
    "120363424847083960@g.us": "Gpo. No. 11 Cristina",
    "120363408346528746@g.us": "Gpo. No. 12 Cristina",
    "120363406341954870@g.us": "Gpo. No. 13 Cristina",
    "120363424448068009@g.us": "Gpo. No. 14 Cristina",
    "120363405818188792@g.us": "Gpo. No. 15 Cristina", 
}


def _group_name(jid: str, db: Session | None = None):
    if not jid:
        return ""

    if db:
        row = db.query(GroupAlias).filter(GroupAlias.group_jid == jid).first()
        if row and row.custom_name:
            return row.custom_name

    if jid in GROUP_NAME_MAP:
        return GROUP_NAME_MAP[jid]

    return "Grupo sin nombre"
    

@app.get("/panel/api")
def panel_api_actas(
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    status: str = "",
    act_type: str = "",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)

    rows = _query_requests_for_panel(
        db=db,
        time_min=time_min,
        time_max=time_max,
        group_jid=group_jid or None,
        provider_name=provider_name or None,
        status=status or None,
        act_type=act_type or None,
    ).order_by(RequestLog.created_at.desc()).all()

    summary = _panel_summary_from_rows(rows)
    by_group = _panel_group_rows(rows, db=db)
    by_provider = _panel_provider_rows(rows)
    by_type = _panel_type_rows(rows)

    latest = []
    for r in rows[:100]:
        latest.append({
            "id": r.id,
            "dato": r.curp,
            "tipo": r.act_type,
            "estado": r.status,
            "grupo": r.source_group_id,
            "proveedor": r.provider_name,
            "proveedor_grupo": r.provider_group_id,
            "mensaje_proveedor": r.provider_message,
            "pdf_url": r.pdf_url,
            "created_at": _fmt_dt(r.created_at),
            "updated_at": _fmt_dt(r.updated_at),
            "error_message": r.error_message or "",
        })

    return {
        "ok": True,
        "view": view,
        "date_from": date_from,
        "date_to": date_to,
        "summary": summary,
        "by_group": by_group,
        "by_provider": by_provider,
        "by_type": by_type,
        "latest": latest,
    }


@app.post("/panel/request/{request_id}/resend-pdf")
def panel_resend_request_pdf(
    request_id: int,
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    req = db.query(RequestLog).filter(RequestLog.id == request_id).first()

    if not req:
        return {"ok": False, "error": "REQUEST_NOT_FOUND"}

    if not req.source_group_id:
        return {"ok": False, "error": "NO_SOURCE_GROUP_ID"}

    instance = (req.instance_name or MAIN_PANEL_INSTANCE or "docifybot8").strip()
    filename = f"{req.curp or request_id}.pdf"

    pdf_bytes = None

    try:
        if req.pdf_url:
            url = generate_r2_presigned_download_url(req.pdf_url)
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            pdf_bytes = r.content

        elif req.provider_media_url and str(req.provider_media_url).startswith("http"):
            r = requests.get(req.provider_media_url, timeout=30)
            r.raise_for_status()
            pdf_bytes = r.content

        else:
            return {
                "ok": False,
                "error": "PDF_NOT_AVAILABLE",
                "detail": "No hay pdf_url/provider_media_url disponible para reenviar.",
            }

        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")

        caption = "📄 Reenvío de acta generada previamente."

        send_group_document_base64(
            req.source_group_id,
            pdf_b64,
            filename=filename,
            caption=caption,
            instance_name=instance,
        )

        req.status = "DONE"
        req.error_message = None
        req.updated_at = _utc_now_naive()
        db.commit()

        print("MANUAL_RESEND_PDF_OK =", {
            "req_id": req.id,
            "curp": req.curp,
            "act_type": req.act_type,
            "instance": instance,
            "source_group_id": req.source_group_id,
        }, flush=True)

        return {
            "ok": True,
            "request_id": req.id,
            "sent_to": req.source_group_id,
            "instance": instance,
        }

    except Exception as e:
        db.rollback()

        print("MANUAL_RESEND_PDF_ERROR =", {
            "req_id": request_id,
            "error": str(e),
        }, flush=True)

        return {
            "ok": False,
            "error": "RESEND_FAILED",
            "detail": str(e)[:500],
        }


def _broadcast_target_groups() -> list[str]:
    out = []

    excluded_words = (
        "PROV",
        "PRUEBA",
        "PRUEBAS",
        "TEST",
    )

    for gid, name in GROUP_NAME_MAP.items():
        name_up = (name or "").strip().upper()

        if any(word in name_up for word in excluded_words):
            continue

        out.append(gid)

    return out


def _clean_audio_base64(audio_base64: str | None) -> str:
    raw = (audio_base64 or "").strip()

    if not raw:
        return ""

    # El navegador normalmente manda:
    # data:audio/webm;codecs=opus;base64,AAAA...
    # Evolution acepta base64 limpio; por eso quitamos el encabezado.
    if "," in raw and raw.lower().startswith("data:"):
        raw = raw.split(",", 1)[1].strip()

    return raw


def _send_whatsapp_audio(
    number: str,
    audio_base64: str,
    instance_name: str = MAIN_PANEL_INSTANCE,
    delay: int = 1200,
):
    number = (number or "").strip()
    instance_name = (instance_name or MAIN_PANEL_INSTANCE).strip()
    audio_base64 = _clean_audio_base64(audio_base64)

    if not number:
        raise ValueError("Destino vacío para audio")

    if not audio_base64:
        raise ValueError("Audio vacío")

    url = f"{EVOLUTION_BASE_URL}/message/sendWhatsAppAudio/{instance_name}"

    payload = {
        "number": number,
        "audio": audio_base64,
        "delay": delay,
    }

    r = requests.post(
        url,
        headers={
            "apikey": EVOLUTION_APIKEY,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=45,
    )

    if r.status_code >= 400:
        raise RuntimeError(
            f"sendWhatsAppAudio/{instance_name} -> {r.status_code}: {r.text[:500]}"
        )

    try:
        return r.json()
    except Exception:
        return {"raw": r.text}


def _run_broadcast_job(
    message_text: str,
    target_groups: list[str],
    instance_name: str = MAIN_PANEL_INSTANCE,
    audio_base64: str = "",
):
    sent = []
    failed = []
    instance_name = (instance_name or MAIN_PANEL_INSTANCE).strip()
    message_text = (message_text or "").strip()
    audio_base64 = _clean_audio_base64(audio_base64)

    for gid in target_groups:
        try:
            # Si capturaste texto, manda primero texto.
            if message_text:
                send_group_text(
                    gid,
                    message_text,
                    instance_name=instance_name,
                )

            # Si grabaste audio, manda después audio como nota de voz.
            if audio_base64:
                _send_whatsapp_audio(
                    gid,
                    audio_base64,
                    instance_name=instance_name,
                )

            sent.append({
                "group_jid": gid,
                "group_name": _group_name(gid),
            })

            time.sleep(0.6)

        except Exception as e:
            failed.append({
                "group_jid": gid,
                "group_name": _group_name(gid),
                "error": str(e),
            })

    print(
        "BROADCAST_FINISHED",
        {
            "instance_name": instance_name,
            "target_count": len(target_groups),
            "sent_count": len(sent),
            "failed_count": len(failed),
            "has_text": bool(message_text),
            "has_audio": bool(audio_base64),
        },
        flush=True,
    )


@app.post("/panel/broadcast/activas")
async def panel_broadcast_activas(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(
            _run_broadcast_job,
            BROADCAST_ACTIVAS_MSG,
            target_groups,
            MAIN_PANEL_INSTANCE,
        )

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_activas error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.post("/panel/broadcast/restablecido")
async def panel_broadcast_mantenimiento(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(
            _run_broadcast_job,
            BROADCAST_RESTABLECIDO_MSG,
            target_groups,
            MAIN_PANEL_INSTANCE,
        )

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_mantenimiento error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.post("/panel/broadcast/suspendido")
async def panel_broadcast_suspendido(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(
            _run_broadcast_job,
            BROADCAST_SUSPENDIDO_MSG,
            target_groups,
            MAIN_PANEL_INSTANCE,
        )

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_suspendido error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.post("/panel/broadcast/cerrado")
async def panel_broadcast_cerrado(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(
            _run_broadcast_job,
            BROADCAST_CERRADO_MSG,
            target_groups,
            MAIN_PANEL_INSTANCE,
        )

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_cerrado error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/broadcast/free")
def botpanel_free_broadcast(
    token: str,
    payload: dict,
    db: Session = Depends(get_db),
):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        return {"ok": False, "error": "Panel no válido"}

    if not _is_child_bot(instance_name):
        return {"ok": False, "error": "No permitido"}

    message = (payload.get("message") or "").strip()

    if not message:
        return {"ok": False, "error": "Mensaje vacío"}

    groups = _bot_group_stats(db, instance_name) or []

    group_jids = []

    for g in groups:
        group_jid = g.get("group_jid")

        if not group_jid:
            continue

        if g.get("blocked"):
            continue

        if "@g.us" not in group_jid:
            continue

        group_jids.append(group_jid)

    if not group_jids:
        return {"ok": False, "error": "No hay grupos activos para enviar"}

    job_id = uuid.uuid4().hex

    broadcast_queue.enqueue(
        botpanel_broadcast_job,
        job_id,
        instance_name,
        message,
        group_jids,
    )

    return {
        "ok": True,
        "queued": True,
        "instance": instance_name,
        "job_id": job_id,
        "total": len(group_jids),
    }


@app.get("/botpanel/{token}/broadcast/progress/{job_id}")
def botpanel_broadcast_progress(token: str, job_id: str, db: Session = Depends(get_db)):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        return {"ok": False, "error": "Panel no válido"}

    key = f"botpanel:broadcast:{job_id}"
    raw = redis_conn.get(key)

    if not raw:
        return {
            "ok": True,
            "status": "pending",
            "instance": instance_name,
            "sent": 0,
            "errors": 0,
            "skipped": 0,
            "total": 0,
            "current": "",
        }

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")

    data = json.loads(raw)

    if data.get("instance") != instance_name:
        return {"ok": False, "error": "Job no pertenece a esta instancia"}

    return data


@app.post("/panel/broadcast/free")
async def panel_broadcast_free(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        message_text = (payload.get("message") or "").strip()
        audio_base64 = _clean_audio_base64(payload.get("audio_base64") or "")
        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []
        
        if not message_text and not audio_base64:
            return {"ok": False, "error": "Mensaje/audio vacío"}

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(
            _run_broadcast_job,
            message_text,
            target_groups,
            MAIN_PANEL_INSTANCE,
            audio_base64,
        )

        tipo_envio = "audio" if audio_base64 and not message_text else "mensaje/audio" if audio_base64 else "mensaje"

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo de {tipo_envio} iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_free error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.get("/panel/broadcast/private-bots/targets")
def panel_private_bots_targets(
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    return {
        "ok": True,
        "bots": _internal_bots_for_private_broadcast(db),
    }


@app.post("/panel/bots/{instance_name}/private-target")
def panel_set_bot_private_target(
    instance_name: str,
    payload: dict = Body(...),
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    instance_name = (instance_name or "").strip()

    if not instance_name:
        return {"ok": False, "error": "Instancia vacía"}

    # Seguridad: solo permitir bots existentes internos.
    allowed = {
        b["instance_name"]
        for b in _internal_bots_for_private_broadcast(db)
    }

    if instance_name not in allowed:
        return {"ok": False, "error": "Bot no encontrado o no permitido"}

    raw_target = (payload.get("jid") or payload.get("phone") or "").strip()
    jid = _normalize_private_wa_jid(raw_target)

    if not jid:
        return {
            "ok": False,
            "error": "Número/JID inválido. Usa 10 dígitos, 52 + número, o JID @s.whatsapp.net",
        }

    _set_bot_private_notify_jid(db, instance_name, jid)
    _clear_panel_cache()

    return {
        "ok": True,
        "instance_name": instance_name,
        "jid": jid,
    }


def _run_private_bots_broadcast_job(
    job_id: str,
    instance_name: str,
    message_text: str,
    recipients: list[dict],
    audio_base64: str = "",
):
    key = f"botpanel:broadcast:{job_id}"
    instance_name = (instance_name or MAIN_PANEL_INSTANCE).strip()
    message_text = (message_text or "").strip()
    audio_base64 = _clean_audio_base64(audio_base64)

    total = len(recipients or [])
    sent = 0
    errors = 0
    skipped = 0

    def save_progress(status: str, current: str = ""):
        redis_conn.setex(
            key,
            60 * 30,
            json.dumps(
                {
                    "ok": True,
                    "status": status,
                    "instance": instance_name,
                    "sent": sent,
                    "errors": errors,
                    "skipped": skipped,
                    "total": total,
                    "current": current,
                    "mode": "private_bots",
                    "has_text": bool(message_text),
                    "has_audio": bool(audio_base64),
                },
                ensure_ascii=False,
            ),
        )

    try:
        save_progress("running", "")

        for item in recipients or []:
            jid = (item.get("jid") or "").strip()
            label = (item.get("label") or item.get("instance_name") or jid).strip()

            if not jid:
                skipped += 1
                save_progress("running", f"{label}: sin JID")
                continue

            try:
                save_progress("running", label)

                if message_text:
                    send_text(
                        jid,
                        message_text,
                        instance_name=instance_name,
                    )

                if audio_base64:
                    _send_whatsapp_audio(
                        jid,
                        audio_base64,
                        instance_name=instance_name,
                    )

                sent += 1
                time.sleep(0.6)

            except Exception as e:
                errors += 1
                print(
                    "PRIVATE_BOTS_BROADCAST_SEND_ERROR =",
                    {
                        "job_id": job_id,
                        "jid": jid,
                        "label": label,
                        "error": str(e),
                    },
                    flush=True,
                )

        save_progress("done", "")

    except Exception as e:
        print("PRIVATE_BOTS_BROADCAST_JOB_ERROR =", repr(e), flush=True)
        save_progress("error", str(e))


@app.post("/panel/broadcast/private-bots")
async def panel_broadcast_private_bots(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    try:
        token = (request.query_params.get("token") or "").strip()

        if token != PANEL_TOKEN:
            return {"ok": False, "error": "UNAUTHORIZED"}

        try:
            payload = await request.json()
        except Exception:
            payload = {}

        message = (payload.get("message") or "").strip()
        audio_base64 = _clean_audio_base64(payload.get("audio_base64") or "")
        selected_instances = payload.get("selected_instances") or []
        
        if not message and not audio_base64:
            return {"ok": False, "error": "Mensaje/audio vacío"}

        if not isinstance(selected_instances, list):
            return {"ok": False, "error": "Selección inválida"}

        selected_instances = {
            str(x).strip()
            for x in selected_instances
            if str(x).strip()
        }

        if not selected_instances:
            return {"ok": False, "error": "Selecciona al menos un bot"}

        all_bots = _internal_bots_for_private_broadcast(db)

        recipients = []

        for bot in all_bots:
            inst = bot["instance_name"]

            if inst not in selected_instances:
                continue

            jid = (bot.get("jid") or "").strip()

            if not jid:
                continue

            recipients.append({
                "instance_name": inst,
                "label": bot.get("label") or inst,
                "jid": jid,
            })

        if not recipients:
            return {
                "ok": False,
                "error": "Los bots seleccionados no tienen número privado configurado",
            }

        job_id = uuid.uuid4().hex

        background_tasks.add_task(
            _run_private_bots_broadcast_job,
            job_id,
            MAIN_PANEL_INSTANCE,
            message,
            recipients,
            audio_base64,
        )

        return {
            "ok": True,
            "queued": True,
            "job_id": job_id,
            "instance": MAIN_PANEL_INSTANCE,
            "total": len(recipients),
            "message": f"Mensaje privado en cola para {len(recipients)} bot(s)",
        }

    except Exception as e:
        print("panel_broadcast_private_bots error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}


@app.get("/panel/broadcast/private-bots/progress/{job_id}")
def panel_private_bots_broadcast_progress(
    job_id: str,
    token: str = "",
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    key = f"botpanel:broadcast:{job_id}"
    raw = redis_conn.get(key)

    if not raw:
        return {
            "ok": True,
            "status": "pending",
            "instance": MAIN_PANEL_INSTANCE,
            "sent": 0,
            "errors": 0,
            "skipped": 0,
            "total": 0,
            "current": "",
            "mode": "private_bots",
        }

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")

    return json.loads(raw)
        

def _promotion_summary_map(db: Session) -> dict[str, dict]:
    cache_key = "panel:promotion_summary_map:v1"
    cached = _cache_get_json(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            GroupPromotion.group_jid,
            GroupPromotion.promo_name,
            GroupPromotion.total_actas,
            GroupPromotion.used_actas,
            GroupPromotion.shared_group_used_actas,
            GroupPromotion.is_active,
            GroupPromotion.client_key,
            GroupPromotion.shared_key,
            GroupPromotion.updated_at,
            GroupPromotion.id,
        )
        .order_by(GroupPromotion.updated_at.desc(), GroupPromotion.id.desc())
        .all()
    )

    shared_counts = Counter(
        (r.shared_key or "").strip()
        for r in rows
        if (r.shared_key or "").strip()
    )

    out = {}
    seen = set()

    for r in rows:
        raw_key = (r.group_jid or "").strip()
        if not raw_key or raw_key in seen:
            continue

        seen.add(raw_key)

        total_actas = int(r.total_actas or 0)
        shared_key = (r.shared_key or "").strip()
        
        if shared_key:
            shared_rows = [
                x for x in rows
                if (x.shared_key or "").strip() == shared_key
            ]
        
            used_actas = max((int(x.used_actas or 0) for x in shared_rows), default=0)
        
            if used_actas <= 0:
                used_actas = sum(int(x.shared_group_used_actas or 0) for x in shared_rows)
        else:
            used_actas = int(r.used_actas or 0)
        
        available = max(0, total_actas - used_actas)
        promo_name = (r.promo_name or "").strip()

        if not promo_name and total_actas == 0 and used_actas == 0:
            continue

        if available <= 0:
            badge_html = '<span style="display:inline-block;padding:6px 10px;border-radius:999px;font-weight:800;font-size:.82rem;color:#991b1b;background:#fee2e2;">Agotada · 0 disponibles</span>'
        elif available <= 10:
            badge_html = f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;font-weight:800;font-size:.82rem;color:#991b1b;background:#fee2e2;">Crítico · {available} disponibles</span>'
        elif available <= 50:
            badge_html = f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;font-weight:800;font-size:.82rem;color:#92400e;background:#fef3c7;">Precaución · {available} disponibles</span>'
        elif available <= 100:
            badge_html = f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;font-weight:800;font-size:.82rem;color:#92400e;background:#fef3c7;">Bajo · {available} disponibles</span>'
        else:
            badge_html = f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;font-weight:800;font-size:.82rem;color:#166534;background:#dcfce7;">Activa · {available} disponibles</span>'

        payload = {
            "promo_name": promo_name,
            "total_actas": total_actas,
            "used_actas": used_actas,
            "available": available,
            "is_active": bool(r.is_active),
            "client_key": (r.client_key or "").strip(),
            "shared_key": shared_key,
            "shared_count": shared_counts.get(shared_key, 0),
            "html": badge_html,
        }

        out[raw_key] = payload
        out[raw_key.replace("@g.us", "")] = payload

    _cache_set_json(cache_key, out, ttl=15)
    return out

                                                                                                        
def _panel_cache_key(
    view: str,
    group_jid: str,
    provider_name: str,
    status: str,
    act_type: str,
    group_mode: str,
    date_from: str = "",
    date_to: str = "",
) -> str:
    return f"panel:html:{PANEL_HTML_CACHE_VERSION}:" + "|".join([
        (view or "").strip(),
        (date_from or "").strip(),
        (date_to or "").strip(),
        (group_jid or "").strip(),
        (provider_name or "").strip(),
        (status or "").strip(),
        (act_type or "").strip(),
        (group_mode or "").strip(),
    ])
                                                                                                        

def _panel_delivery_metrics(db, time_min, time_max):
    try:
        maya_personal_provider = _personal_provider_filter_for_instance(db, "docifybot8maya")
    
        q = db.query(
            RequestLog.provider_processing_time,
            RequestLog.provider_to_webhook_lag_s,
            RequestLog.t_total_provider1_relay,
            RequestLog.total_delivery_time,
        ).filter(
            RequestLog.created_at >= time_min,
            RequestLog.created_at < time_max,
            RequestLog.provider_processing_time.isnot(None),
            RequestLog.provider_to_webhook_lag_s.isnot(None),
            RequestLog.t_total_provider1_relay.isnot(None),
            RequestLog.total_delivery_time.isnot(None),
        )

        if maya_personal_provider:
            q = q.filter(
                ~(
                    (RequestLog.instance_name == "docifybot8maya")
                    & (RequestLog.provider_name == maya_personal_provider)
                )
            )

        rows = q.all()

        if not rows:
            return None

        provider_times = [float(r[0]) for r in rows]
        whatsapp_times = [float(r[1]) for r in rows]
        bot_times = [float(r[2]) for r in rows]
        total_times = [float(r[3]) for r in rows]

        avg_provider = round(sum(provider_times) / len(provider_times), 2)
        avg_whatsapp = round(sum(whatsapp_times) / len(whatsapp_times), 2)
        avg_bot = round(sum(bot_times) / len(bot_times), 2)
        avg_total = round(sum(total_times) / len(total_times), 2)

        fastest = round(min(total_times), 2)
        slowest = round(max(total_times), 2)

        return {
            "avg_provider": avg_provider,
            "avg_whatsapp": avg_whatsapp,
            "avg_bot": avg_bot,
            "avg_total": avg_total,
            "fastest": fastest,
            "slowest": slowest,
            "processed": len(total_times),
        }

    except Exception as e:
        print("PANEL_DELIVERY_METRICS_ERROR =", repr(e), flush=True)
        return None


def _bot_recharge_history(db: Session, instance_name: str, limit: int = 30):
    return (
        db.query(BotRechargeLog)
        .filter(BotRechargeLog.instance_name == instance_name)
        .order_by(BotRechargeLog.created_at.desc())
        .limit(limit)
        .all()
    )


def _bot_credit_stats(db: Session, instance_name: str):
    try:
        instance_name = (instance_name or "").strip()

        limit_value = get_bot_limit(db, instance_name)
        used_value = get_bot_used(db, instance_name)

        available = max(int(limit_value or 0) - int(used_value or 0), 0)

        recharge_count = (
            db.query(BotRechargeLog)
            .filter(BotRechargeLog.instance_name == instance_name)
            .count()
        )

        return {
            "limit": int(limit_value or 0),
            "used": int(used_value or 0),
            "available": int(available or 0),
            "recharges": int(recharge_count or 0),
        }

    except Exception as e:
        print("BOT_CREDIT_STATS_ERROR =", instance_name, repr(e), flush=True)
        return {
            "limit": 0,
            "used": 0,
            "available": 0,
            "recharges": 0,
        }
        

@app.post("/botpanel/{token}/group/{group_jid}/block")
def panel_bot_block_group(token: str, group_jid: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        _assert_group_owned_by_bot(db, group_jid, instance_name)
        block_group(group_jid)
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/{group_jid}/unblock")
def panel_bot_unblock_group(token: str, group_jid: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        _assert_group_owned_by_bot(db, group_jid, instance_name)
        unblock_group(group_jid)
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/bot/block")
def botpanel_block_bot(token: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        block_instance(instance_name)
        _clear_panel_cache()

        return {
            "ok": True,
            "instance_name": instance_name,
            "blocked": True,
            "message": "Bot apagado correctamente",
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/bot/unblock")
def botpanel_unblock_bot(token: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        if is_instance_admin_blocked(instance_name):
            return {
                "ok": False,
                "error": "Este bot fue bloqueado desde el panel principal. Solo el panel principal puede desbloquearlo.",
                "instance_name": instance_name,
                "blocked": True,
                "admin_blocked": True,
            }

        unblock_instance(instance_name)
        _clear_panel_cache()

        return {
            "ok": True,
            "instance_name": instance_name,
            "blocked": False,
            "message": "Bot prendido correctamente",
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/{group_jid}/rename")
async def panel_bot_rename_group(token: str, group_jid: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        _assert_group_owned_by_bot(db, group_jid, instance_name)
        payload = await request.json()
        custom_name = (payload.get("custom_name") or "").strip()
        if not custom_name:
            return {"ok": False, "error": "Nombre vacío"}

        row = db.query(GroupAlias).filter_by(group_jid=group_jid).first()
        if row:
            row.custom_name = custom_name
            row.owner_instance = instance_name
            row.updated_at = _utc_now_naive()
        else:
            row = GroupAlias(
                group_jid=group_jid,
                custom_name=custom_name,
                owner_instance=instance_name,
                updated_at=_utc_now_naive(),
            )
            db.add(row)

        db.commit()
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/promotion/set")
async def panel_bot_set_promo(token: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        payload = await request.json()
        group_jid = (payload.get("group_jid") or "").strip()
        promo_name = (payload.get("promo_name") or "").strip()
        total_actas = int(payload.get("total_actas") or 0)
        price_per_piece = (payload.get("price_per_piece") or "").strip()

        group = db.query(AuthorizedGroup).filter(
            AuthorizedGroup.group_jid == group_jid
        ).first()
        
        if not group:
            return {"ok": False, "error": "Grupo no encontrado"}
        
        if group.owner_instance != instance_name:
            group.owner_instance = instance_name

        if total_actas < MIN_BOT_PROMO_ACTAS:
            return {"ok": False, "error": f"La promoción mínima es de {MIN_BOT_PROMO_ACTAS} actas"}

        row = db.query(GroupPromotion).filter_by(group_jid=group_jid).first()
        if row:
            row.promo_name = promo_name
            row.total_actas = total_actas
            row.used_actas = 0
            row.shared_group_used_actas = 0
            row.warning_sent_200 = False
            row.warning_sent_100 = False
            row.warning_sent_50 = False
            row.warning_sent_10 = False
            row.warning_sent_0 = False
            row.price_per_piece = price_per_piece
            row.is_active = True
            row.owner_instance = instance_name
            row.updated_at = _utc_now_naive()
        else:
            row = GroupPromotion(
                group_jid=group_jid,
                promo_name=promo_name,
                total_actas=total_actas,
                used_actas=0,
                shared_group_used_actas=0,
                price_per_piece=price_per_piece,
                is_active=True,
                owner_instance=instance_name,
                warning_sent_200=False,
                warning_sent_100=False,
                warning_sent_50=False,
                warning_sent_10=False,
                warning_sent_0=False,
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
            )
            db.add(row)

        available = max(0, int(total_actas or 0) - int(row.used_actas or 0))

        db.commit()

        try:
            redis_conn.delete(f"promo_notify:{group_jid}:0")
            redis_conn.delete(f"promo_notify:{group_jid}:10")
            redis_conn.delete(f"promo_notify:{group_jid}:50")
            redis_conn.delete(f"promo_notify:{group_jid}:100")
            redis_conn.delete(f"promo_notify:{group_jid}:200")
            redis_conn.delete(f"blocked_group_notify:{group_jid}")
        except Exception as e:
            print("BOT_PROMO_NOTIFY_KEYS_CLEAR_ERROR =", str(e), flush=True)

        try:
            unblock_group(group_jid)
        except Exception as unblock_exc:
            print("BOT_PROMO_AUTO_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

        try:
            promo_label = promo_name or "paquete promocional"

            send_group_text(
                group_jid,
                (
                    f"✅ *Promoción activada*\n\n"
                    f"Tu *{promo_label}* ya fue activada correctamente.\n"
                    f"Cuentas con *{available} actas disponibles*.\n\n"
                    f"Cuando el paquete se agote, el grupo será pausado automáticamente.\n"
                    f"Gracias por tu preferencia."
                ),
                instance_name=instance_name,
            )

        except Exception as notify_exc:
            print("BOT_PROMOTION_ACTIVATION_NOTIFY_ERROR =", str(notify_exc), flush=True)

        _clear_panel_cache()

        return {
            "ok": True,
            "message": "Promoción activada y notificada al grupo",
            "group_jid": group_jid,
            "promo_name": promo_name,
            "total_actas": total_actas,
            "used_actas": int(row.used_actas or 0),
            "available": available,
        }
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/promotion/remove")
def botpanel_remove_promotion(
    token: str,
    payload: dict,
    db: Session = Depends(get_db),
):
    try:
        instance_name = _bot_instance_from_token(db, token)

        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        if not _is_child_bot(instance_name):
            return {"ok": False, "error": "Panel no permitido"}

        group_jid = (payload.get("group_jid") or "").strip()

        if not group_jid:
            return {"ok": False, "error": "Falta group_jid"}

        group = db.query(AuthorizedGroup).filter(
            AuthorizedGroup.group_jid == group_jid
        ).first()

        if not group:
            return {"ok": False, "error": "Grupo no encontrado"}

        if group.owner_instance != instance_name:
            group.owner_instance = instance_name

        promo = (
            db.query(GroupPromotion)
            .filter(GroupPromotion.group_jid == group_jid)
            .first()
        )

        if not promo:
            db.commit()
            _clear_panel_cache()
            return {"ok": True, "message": "Este grupo no tenía promoción"}

        db.delete(promo)
        db.commit()
        
        try:
            send_group_text(
                group_jid,
                (
                    "⚠️ *Promoción retirada*\n\n"
                    "La promoción de este grupo fue retirada correctamente.\n"
                    "Para continuar con paquete promocional, contacta al administrador."
                ),
                instance_name=instance_name,
            )
        except Exception as notify_exc:
            print("BOT_PROMOTION_REMOVE_NOTIFY_ERROR =", str(notify_exc), flush=True)
        
        _clear_panel_cache()

        return {
            "ok": True,
            "message": "Promoción quitada correctamente"
        }

    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/add")
async def panel_bot_add_group(token: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        payload = await request.json()
        group_jid = (payload.get("group_jid") or "").strip()
        group_name = (payload.get("group_name") or "").strip()

        if not group_jid:
            return {"ok": False, "error": "Group JID vacío"}

        row = db.query(AuthorizedGroup).filter_by(group_jid=group_jid).first()

        if row:
            row.owner_instance = instance_name
            if group_name:
                row.group_name = group_name
        else:
            row = AuthorizedGroup(
                group_jid=group_jid,
                group_name=group_name or None,
                owner_instance=instance_name,
            )
            db.add(row)

        if group_name:
            alias = db.query(GroupAlias).filter_by(group_jid=group_jid).first()
            if alias:
                alias.custom_name = group_name
                alias.owner_instance = instance_name
                alias.updated_at = _utc_now_naive()
            else:
                alias = GroupAlias(
                    group_jid=group_jid,
                    custom_name=group_name,
                    owner_instance=instance_name,
                    updated_at=_utc_now_naive(),
                )
                db.add(alias)

        db.commit()
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.get("/botpanel/{token}/provider-mode")
def botpanel_get_provider_mode(token: str, db: Session = Depends(get_db)):
    instance_name = _bot_instance_from_token(db, token)
    if not instance_name:
        return {"ok": False, "error": "Panel no válido"}

    if instance_name != "docifybot8maya":
        return {"ok": False, "error": "Este ajuste solo está disponible para Gestoría Maya"}

    mode = _bot_provider_mode(db, instance_name)

    return {
        "ok": True,
        "instance_name": instance_name,
        "mode": mode,
        "label": BOT_PROVIDER_OPTIONS.get(mode, mode),
        "options": BOT_PROVIDER_OPTIONS,
    }


@app.post("/botpanel/{token}/provider-mode")
async def botpanel_set_provider_mode(token: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(db, token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        if instance_name != "docifybot8maya":
            return {"ok": False, "error": "Este ajuste solo está disponible para Gestoría Maya"}

        payload = await request.json()
        mode = (payload.get("mode") or "GLOBAL_POOL").strip().upper()

        _set_bot_provider_mode(db, instance_name, mode)

        _clear_panel_cache()
        _clear_group_name_cache()

        return {
            "ok": True,
            "instance_name": instance_name,
            "mode": mode,
            "label": BOT_PROVIDER_OPTIONS.get(mode, mode),
        }

    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.get("/botpanel/{token}/provider-mode-ui", response_class=HTMLResponse)
def botpanel_provider_mode_ui(token: str, db: Session = Depends(get_db)):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        return HTMLResponse("<h3>Panel no válido.</h3>", status_code=404)

    if instance_name != "docifybot8maya":
        return HTMLResponse("<h3>Este ajuste solo está disponible para Gestoría Maya.</h3>", status_code=403)

    mode = _bot_provider_mode(db, instance_name)
    label = BOT_PROVIDER_OPTIONS.get(mode, mode)
    title = _bot_title(db, instance_name)

    options_html = ""
    for value, text in BOT_PROVIDER_OPTIONS.items():
        selected = "selected" if value == mode else ""
        options_html += f'<option value="{_esc(value)}" {selected}>{_esc(text)}</option>'

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Modo de proveedor - {_esc(title)}</title>
      <style>
        body {{
          font-family: Arial, sans-serif;
          background:#f4f6f8;
          margin:0;
          padding:24px;
          color:#111827;
        }}
        .box {{
          max-width:560px;
          margin:40px auto;
          background:white;
          border-radius:18px;
          padding:22px;
          box-shadow:0 10px 28px rgba(15,23,42,.12);
        }}
        h2 {{
          margin:0 0 8px 0;
        }}
        .muted {{
          color:#64748b;
          font-size:14px;
          margin-bottom:18px;
        }}
        .current {{
          background:#f8fafc;
          border:1px solid #e5e7eb;
          border-radius:14px;
          padding:12px;
          margin:14px 0;
        }}
        select {{
          width:100%;
          padding:12px;
          border-radius:12px;
          border:1px solid #cbd5e1;
          font-size:15px;
          margin-top:8px;
        }}
        button {{
          width:100%;
          margin-top:14px;
          padding:12px;
          border:0;
          border-radius:12px;
          background:#2563eb;
          color:white;
          font-size:15px;
          font-weight:bold;
          cursor:pointer;
        }}
        .help {{
          margin-top:16px;
          font-size:13px;
          color:#64748b;
          line-height:1.45;
        }}
      </style>
    </head>
    <body>
      <div class="box">
        <h2>Modo de proveedor</h2>
        <div class="muted">{_esc(title)} · {_esc(instance_name)}</div>

        <div class="current">
          <strong>Modo actual:</strong><br>
          {_esc(label)}
        </div>

        <label>Seleccionar modo:</label>
        <select id="providerModeSelect">
          {options_html}
        </select>

        <button onclick="saveProviderMode()">Guardar modo</button>

        <div class="help">
          <strong>Global:</strong> usa proveedores del sistema principal y sí cuenta.<br>
          <strong>Privado:</strong> usa proveedor personal y no cuenta en panel, promociones ni límite.
        </div>
      </div>

      <script>
      async function saveProviderMode() {{
        const select = document.getElementById("providerModeSelect");
        const mode = select.value;

        if (!confirm("¿Guardar este modo de proveedor?")) {{
          return;
        }}

        const res = await fetch("/botpanel/{_esc(token)}/provider-mode", {{
          method: "POST",
          headers: {{"Content-Type": "application/json"}},
          body: JSON.stringify({{mode}})
        }});

        const data = await res.json();

        if (!data.ok) {{
          alert(data.error || "No se pudo guardar el modo.");
          return;
        }}

        alert("Modo actualizado: " + data.label);
        location.reload();
      }}
      </script>
    </body>
    </html>
    """

    return HTMLResponse(html)


@app.get("/gestoria-maya/proveedores", response_class=HTMLResponse)
def maya_provider_mode_ui(db: Session = Depends(get_db)):
    return botpanel_provider_mode_ui("as5613f4se", db)


@app.get("/botpanel/{token}")
def panel_bot(token: str, db: Session = Depends(get_db)):
    instance_name = _bot_instance_from_token(db, token)

    if not instance_name:
        return HTMLResponse("<h3>Panel no válido.</h3>", status_code=404)

    if not _is_child_bot(instance_name):
        return HTMLResponse("<h3>Este panel es solo para bots desde docifybot8 en adelante.</h3>", status_code=400)

    title = _bot_title(db, instance_name)
    today_sales = _bot_sales_today(db, instance_name)
    month_sales = _bot_sales_30d(db, instance_name)
    history_rows = _bot_sales_history_30d(db, instance_name)
    groups = _bot_group_stats(db, instance_name)
    credits = _bot_credit_stats(db, instance_name)
    recharge_rows = _bot_recharge_history(db, instance_name, limit=30)

    credits = credits or {}
    credits.setdefault("limit", 0)
    credits.setdefault("used", 0)
    credits["available"] = max(0, credits["limit"] - credits["used"])
    credits.setdefault("recharges", 0)

    groups = groups or []
    total_groups = len(groups)
    blocked_groups = sum(1 for g in groups if g["blocked"])
    active_promos = sum(1 for g in groups if g["promo_active"])

    bot_blocked = is_instance_blocked(instance_name)
    bot_admin_blocked = is_instance_admin_blocked(instance_name)
    
    if bot_admin_blocked:
        bot_status_label = "BLOQUEADO POR PANEL PRINCIPAL"
    else:
        bot_status_label = "APAGADO" if bot_blocked else "PRENDIDO"
    bot_status_badge = (
        '<span class="badge badge-danger">BOT APAGADO</span>'
        if bot_blocked else
        '<span class="badge badge-success">BOT PRENDIDO</span>'
    )

    html = f"""
    <html>
    <head>
      <title>Mini Panel {title}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{
          font-family: Arial, sans-serif;
          background: #f4f6f8;
          margin: 0;
          color: #1f2937;
        }}
        .wrap {{
          max-width: 1400px;
          margin: 0 auto;
          padding: 16px;
        }}
        .hero {{
          background: linear-gradient(135deg, #111827 0%, #334155 100%);
          color: white;
          border-radius: 20px;
          padding: 20px;
          margin-bottom: 16px;
        }}
        .cards {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
          gap: 12px;
          margin-bottom: 16px;
        }}
        .card {{
          background: white;
          border-radius: 16px;
          padding: 16px;
          border: 1px solid #e5e7eb;
        }}
        .label {{
          color: #6b7280;
          font-size: 13px;
          margin-bottom: 8px;
          font-weight: 700;
        }}
        .value {{
          font-size: 28px;
          font-weight: 800;
        }}
        .box {{
          background: white;
          border-radius: 18px;
          border: 1px solid #e5e7eb;
          margin-bottom: 16px;
          overflow: hidden;
        }}
        .head {{
          padding: 16px 18px;
          border-bottom: 1px solid #e5e7eb;
          background: #fafafa;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }}
        .table-wrap {{
          overflow-x: auto;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
        }}
        th, td {{
          padding: 12px;
          border-bottom: 1px solid #e5e7eb;
          text-align: left;
          vertical-align: top;
        }}
        th {{
          background: #111827;
          color: white;
        }}
        .btn {{
          border: none;
          border-radius: 10px;
          padding: 9px 12px;
          font-weight: 700;
          cursor: pointer;
        }}
        .btn-success {{ background: #166534; color: white; }}
        .btn-danger {{ background: #b91c1c; color: white; }}
        .btn-primary {{ background: #1d4ed8; color: white; }}
        .badge {{
          display: inline-flex;
          padding: 4px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 700;
        }}
        .badge-success {{ background: #dcfce7; color: #166534; }}
        .badge-danger {{ background: #fee2e2; color: #991b1b; }}
        .small {{ font-size: 12px; color: #6b7280; }}
        input {{
          width: 100%;
          padding: 10px 12px;
          border: 1px solid #d1d5db;
          border-radius: 10px;
          box-sizing: border-box;
        }}
        .recharge-box {{
          border: 1px solid #dbeafe;
        }}
        .recharge-box .head {{
          background: linear-gradient(135deg, #eff6ff 0%, #f8fafc 100%);
        }}
        .recharge-table th {{
          background: #1e3a8a;
          color: white;
          font-size: 13px;
        }}
        .recharge-table td {{
          font-size: 13px;
          white-space: nowrap;
        }}
        .recharge-amount {{
          font-weight: 800;
          color: #166534;
        }}
        .recharge-note {{
          color: #6b7280;
          font-size: 12px;
        }}
        @media (max-width: 900px) {{
          .cards {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }}
        }}
      </style>
    </head>
    """

    html += f"""
    <body>
      <div class="wrap">
        <div class="hero">
          <h1 style="margin:0 0 6px 0;">Mini Panel · {title}</h1>
          <div>Gestión independiente de grupos, promociones y ventas del bot {title}.</div>
        </div>

        <div class="cards">
          <div class="card">
            <div class="label">Vendidas hoy</div>
            <div class="value">{today_sales}</div>
          </div>

          <div class="card">
            <div class="label">Vendidas 30 días</div>
            <div class="value">{month_sales}</div>
          </div>
          <div class="card">
            <div class="label">Grupos</div>
            <div class="value">{total_groups}</div>
          </div>
          <div class="card">
            <div class="label">Grupos bloqueados</div>
            <div class="value">{blocked_groups}</div>
          </div>
          <div class="card">
            <div class="label">Promociones activas</div>
            <div class="value">{active_promos}</div>
          </div>
          <div class="card">
            <div class="label">Estado del bot</div>
            <div class="value" style="font-size:20px;">{bot_status_label}</div>
            <div style="margin-top:10px;">
              {bot_status_badge}
            </div>
          </div>
        </div>

        <div class="cards">
          <div class="card">
            <div class="label">Actas cargadas</div>
            <div class="value">{credits['limit']}</div>
          </div>
        
          <div class="card">
            <div class="label">Actas usadas</div>
            <div class="value">{credits['used']}</div>
          </div>
        
          <div class="card">
            <div class="label">Actas disponibles</div>
            <div class="value">{credits['available']}</div>
          </div>
        
          <div class="card">
            <div class="label">Recargas realizadas</div>
            <div class="value">{credits['recharges']}</div>
          </div>
        </div>

        <div class="box">
          <div class="head">
            <strong>Control del bot</strong>
            <span class="small">Apaga o prende temporalmente este bot sin cerrar sesión de WhatsApp.</span>
          </div>

          <div style="padding:16px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
            <div>
              Estado actual: {bot_status_badge}
            </div>

            {
              '<button class="btn btn-danger" disabled style="opacity:.6;cursor:not-allowed;">Bloqueado por panel principal</button>'
              if bot_admin_blocked else
              (
                '<button class="btn btn-success" onclick="unblockMiniBot()">Prender bot</button>'
                if bot_blocked else
                '<button class="btn btn-danger" onclick="blockMiniBot()">Apagar bot</button>'
              )
            }
          </div>
        </div>

        <div class="box">
          <div class="head">
            <strong>Agregar grupo manualmente</strong>
            <span class="small">Registra un grupo para este bot y asígnale nombre visible.</span>
          </div>
          <div style="padding:16px;">
            <div style="display:grid;grid-template-columns:1.4fr 1fr auto;gap:12px;align-items:end;">
              <div>
                <div class="small" style="margin-bottom:6px;">Group JID</div>
                <input id="manual_group_jid" placeholder="1203634XXXXXXXXXX@g.us">
              </div>
              <div>
                <div class="small" style="margin-bottom:6px;">Nombre del grupo</div>
                <input id="manual_group_name" placeholder="Nombre visible del grupo">
              </div>
              <div>
                <button class="btn btn-primary" onclick="addManualBotGroup()">Agregar grupo</button>
              </div>
            </div>
          </div>
        </div>

        <div class="box">
          <div class="head">
            <strong>Mensajes masivos</strong>
            <span class="small">Enviar mensaje libre solo a grupos de {title}.</span>
          </div>

          <div style="padding:16px;">
            <textarea
              id="botBroadcastMessage"
              placeholder="Escribe aquí el mensaje que deseas enviar..."
              style="width:100%;min-height:120px;padding:12px;border:1px solid #d1d5db;border-radius:12px;box-sizing:border-box;"
            ></textarea>

            <div style="display:flex;gap:8px;margin-top:12px;">
              <button class="btn btn-success" onclick="sendBotFreeBroadcast()">Enviar mensaje libre</button>
              <button class="btn" onclick="document.getElementById('botBroadcastMessage').value=''">Limpiar</button>
            </div>
            
            <div
              id="botBroadcastProgress"
              style="display:none;margin-top:12px;padding:12px;border-radius:12px;background:#f8fafc;border:1px solid #e5e7eb;font-size:13px;"
            ></div>
          </div>
        </div>

        <div class="box">
          <div class="head" style="align-items:flex-start;">
            <div>
              <strong style="font-size:18px;">Historial y evidencias</strong>
              <div class="small" style="margin-top:4px;">
                Consulta ventas, errores y movimientos de todos los grupos de {title}.
              </div>
            </div>
            <span class="badge badge-success">Mini panel</span>
          </div>
        
          <div style="padding:16px;">
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;">
        
              <a target="_blank"
                 href="/botpanel/{_esc(token)}/audit?period=day&status=DONE"
                 style="text-decoration:none;color:inherit;">
                <div style="border:1px solid #dbeafe;background:#eff6ff;border-radius:16px;padding:16px;">
                  <div style="font-size:13px;color:#1d4ed8;font-weight:800;">HOY</div>
                  <div style="font-size:22px;font-weight:900;margin-top:6px;">Historial diario</div>
                  <div class="small" style="margin-top:6px;">Ventas realizadas hoy</div>
                </div>
              </a>
        
              <a target="_blank"
                 href="/botpanel/{_esc(token)}/audit?period=30d&status=DONE"
                 style="text-decoration:none;color:inherit;">
                <div style="border:1px solid #bbf7d0;background:#f0fdf4;border-radius:16px;padding:16px;">
                  <div style="font-size:13px;color:#166534;font-weight:800;">30 DÍAS</div>
                  <div style="font-size:22px;font-weight:900;margin-top:6px;">Último mes</div>
                  <div class="small" style="margin-top:6px;">Ventas de todos los grupos</div>
                </div>
              </a>
        
              <a target="_blank"
                 href="/botpanel/{_esc(token)}/audit?period=month&status=DONE"
                 style="text-decoration:none;color:inherit;">
                <div style="border:1px solid #e9d5ff;background:#faf5ff;border-radius:16px;padding:16px;">
                  <div style="font-size:13px;color:#7e22ce;font-weight:800;">MES ACTUAL</div>
                  <div style="font-size:22px;font-weight:900;margin-top:6px;">Corte mensual</div>
                  <div class="small" style="margin-top:6px;">Movimientos del mes</div>
                </div>
              </a>
        
              <a target="_blank"
                 href="/botpanel/{_esc(token)}/audit?period=prev_month&status=DONE"
                 style="text-decoration:none;color:inherit;">
                <div style="border:1px solid #fed7aa;background:#fff7ed;border-radius:16px;padding:16px;">
                  <div style="font-size:13px;color:#c2410c;font-weight:800;">MES ANTERIOR</div>
                  <div style="font-size:22px;font-weight:900;margin-top:6px;">Histórico</div>
                  <div class="small" style="margin-top:6px;">Consulta el corte pasado</div>
                </div>
              </a>
        
            </div>
          </div>
        </div>

        <div class="box">
          <div class="head">
            <strong>Grupos del bot</strong>
            <span class="small">Bloquea, renombra y asigna promociones (mínimo 10 actas).</span>
          </div>

          <div style="padding:14px 16px;border-bottom:1px solid #e5e7eb;background:#f8fafc;">
            <input
              id="botGroupSearch"
              placeholder="Buscar grupo por nombre o ID..."
              oninput="filterBotGroups()"
              style="max-width:420px;"
            >
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Grupo</th>
                  <th>Hoy</th>
                  <th>30 días</th>
                  <th>Mes actual</th>
                  <th>Mes anterior</th>
                  <th>Promoción</th>
                  <th>Estado</th>
                  <th>Evidencia</th>
                  <th>Renombrar</th>
                  <th>Asignar promoción</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
    """

    if groups:
        for g in groups:
            promo_text = (
                f'{g["promo_used"]}/{g["promo_total"]}'
                if g["promo_total"] > 0 else "Sin promo"
            )
            status_badge = (
                '<span class="badge badge-danger">BLOQUEADO</span>'
                if g["blocked"] else
                '<span class="badge badge-success">ACTIVO</span>'
            )

            block_btn = (
                f'<button class="btn btn-success" onclick="unblockBotGroup(\'{_esc(g["group_jid"])}\')">Desbloquear</button>'
                if g["blocked"] else
                f'<button class="btn btn-danger" onclick="blockBotGroup(\'{_esc(g["group_jid"])}\')">Bloquear</button>'
                f'<button class="btn btn-light" onclick="hideBotGroup(\'{_esc(g["group_jid"])}\')">Ocultar</button>'
            )

            search_text = f'{g["group_name"]} {g["group_jid"]}'.lower()

            html += f"""
                <tr class="bot-group-row" data-search="{_esc(search_text)}">
                  <td>
                    <strong>{_esc(g["group_name"])}</strong><br>
                    <span class="small">{_esc(g["group_jid"])}</span>
                  </td>
                  <td>{g["today_done"]}</td>
                  <td>{g["done_30d"]}</td>
                  <td>{g["month_done"]}</td>
                  <td>{g["prev_month_done"]}</td>
                  <td>{promo_text}</td>
                  <td>{status_badge}</td>

                  <td>
                    <a target="_blank"
                       href="/botpanel/{_esc(token)}/audit?period=day&status=DONE&group_jid={_esc(g['group_jid'])}"
                       class="btn btn-primary"
                       style="color:white;text-decoration:none;padding:6px 10px;font-size:12px;border-radius:10px;">
                       Hoy
                    </a>
                    <br><br>
                    
                    <a target="_blank"
                       href="/botpanel/{_esc(token)}/audit?period=30d&status=DONE&group_jid={_esc(g['group_jid'])}"
                       class="btn btn-success"
                       style="color:white;text-decoration:none;padding:6px 10px;font-size:12px;border-radius:10px;">
                       30 días
                    </a>
                    <br><br>
                    
                    <a target="_blank"
                       href="/botpanel/{_esc(token)}/audit?period=month&status=DONE&group_jid={_esc(g['group_jid'])}"
                       class="btn btn-primary"
                       style="color:white;text-decoration:none;padding:6px 10px;font-size:12px;border-radius:10px;">
                       Mes actual
                    </a>
                    <br><br>
                    
                    <a target="_blank"
                       href="/botpanel/{_esc(token)}/audit?period=prev_month&status=DONE&group_jid={_esc(g['group_jid'])}"
                       class="btn btn-success"
                       style="color:white;text-decoration:none;padding:6px 10px;font-size:12px;border-radius:10px;">
                       Mes anterior
                    </a>
                  </td>
  
                  <td>
                    <div style="display:flex;gap:8px;min-width:220px;">
                      <input id="rename_{_esc(g["group_jid"])}" placeholder="Nuevo nombre">
                      <button class="btn btn-primary" onclick="renameBotGroup('{_esc(g["group_jid"])}')">Guardar</button>
                    </div>
                  </td>

                  <td>
                    <div style="display:grid;gap:8px;min-width:260px;">
                      <input id="promo_name_{_esc(g["group_jid"])}" placeholder="Nombre promo">
                      <input id="promo_total_{_esc(g["group_jid"])}" type="number" min="10" step="1" placeholder="Total actas (mín. 10)">
                      <input id="promo_price_{_esc(g["group_jid"])}" placeholder="Precio por acta">
                
                      <button class="btn btn-success"
                        onclick="assignBotPromo('{_esc(g["group_jid"])}')">
                        Aplicar promo
                      </button>
                
                      {
                        f"""
                        <button class="btn btn-danger"
                          onclick="removeBotPromo('{_esc(g["group_jid"])}')">
                          Quitar promo
                        </button>
                        """
                        if g["promo_total"] > 0 else ""
                      }
                    </div>
                  </td>

                  <td>
                    {block_btn}
                  </td>
                </tr>
            """
    else:
        html += '<tr><td colspan="11">Este bot aún no tiene grupos asignados.</td></tr>'

    html += """
              </tbody>
            </table>
          </div>
        </div>
    """

    html += """
        <div class="box">
          <div class="head">
            <strong>Historial de recargas</strong>
            <span class="small">Últimas 30 recargas aplicadas desde el panel principal.</span>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Recarga</th>
                  <th>Límite anterior</th>
                  <th>Nuevo límite</th>
                  <th>Usadas</th>
                  <th>Disponibles</th>
                  <th>Origen</th>
                </tr>
              </thead>
              <tbody>
    """

    if recharge_rows:
        for r in recharge_rows:
            html += f"""
                <tr>
                  <td>{_esc(_fmt_dt(r.created_at))}</td>
                  <td class="recharge-amount">+{int(r.amount or 0)}</td>
                  <td>{int(r.previous_limit or 0)}</td>
                  <td>{int(r.new_limit or 0)}</td>
                  <td>{int(r.used_at_recharge or 0)}</td>
                  <td>{int(r.available_after or 0)}</td>
                  <td>{_esc(r.source or "")}</td>
                </tr>
            """
    else:
        html += """
                <tr>
                  <td colspan="7">Este bot aún no tiene historial de recargas.</td>
                </tr>
        """

    html += """
              </tbody>
            </table>
          </div>
        </div>

        <div class="box">
          <div class="head">
            <strong>Historial último mes</strong>
            <span class="small">Ventas realizadas por día.</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Total vendidas</th>
                </tr>
              </thead>
              <tbody>
    """

    if history_rows:
        for day, total in history_rows:
            html += f"""
                <tr>
                  <td>{_esc(str(day))}</td>
                  <td>{int(total or 0)}</td>
                </tr>
            """
    else:
        html += '<tr><td colspan="2">Sin ventas en los últimos 30 días.</td></tr>'

    html += """
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <script>
        const BOT_PANEL_BASE = window.location.pathname;

        function filterBotGroups() {
          const q = (document.getElementById("botGroupSearch")?.value || "").trim().toLowerCase();
          const rows = document.querySelectorAll(".bot-group-row");

          rows.forEach(row => {
            const text = (row.dataset.search || "").toLowerCase();
            row.style.display = (!q || text.includes(q)) ? "" : "none";
          });
        }

        async function blockMiniBot() {
          const ok = confirm("¿Apagar este bot? Mientras esté apagado no responderá solicitudes.");
          if (!ok) return;

          const res = await fetch(`${BOT_PANEL_BASE}/bot/block`, {
            method: "POST"
          });

          const data = await res.json();

          if (data.ok) {
            alert("Bot apagado correctamente.");
            location.reload();
          } else {
            alert(data.error || "No se pudo apagar el bot.");
          }
        }

        async function unblockMiniBot() {
          const ok = confirm("¿Prender este bot nuevamente?");
          if (!ok) return;

          const res = await fetch(`${BOT_PANEL_BASE}/bot/unblock`, {
            method: "POST"
          });

          const data = await res.json();

          if (data.ok) {
            alert("Bot prendido correctamente.");
            location.reload();
          } else {
            alert(data.error || "No se pudo prender el bot.");
          }
        }

        async function blockBotGroup(groupJid) {
          const res = await fetch(`${BOT_PANEL_BASE}/group/${encodeURIComponent(groupJid)}/block`, { method: "POST" });
          const data = await res.json();
          if (data.ok) location.reload();
          else alert(data.error || "No se pudo bloquear");
        }

        async function unblockBotGroup(groupJid) {
          const res = await fetch(`${BOT_PANEL_BASE}/group/${encodeURIComponent(groupJid)}/unblock`, { method: "POST" });
          const data = await res.json();
          if (data.ok) location.reload();
          else alert(data.error || "No se pudo desbloquear");
        }

        async function hideBotGroup(groupJid) {
          const ok = confirm("¿Quitar este grupo del mini panel?");
          if (!ok) return;
        
          const res = await fetch(`${BOT_PANEL_BASE}/group/${encodeURIComponent(groupJid)}/hide`, {
            method: "POST"
          });
        
          const data = await res.json();
          if (data.ok) {
            location.reload();
          } else {
            alert(data.error || "No se pudo quitar el grupo.");
          }
        }

        let botBroadcastProgressTimer = null;

        async function sendBotFreeBroadcast() {
          const message = document.getElementById("botBroadcastMessage").value.trim();
        
          if (!message) {
            alert("Escribe un mensaje.");
            return;
          }
        
          const ok = confirm("¿Enviar este mensaje a todos los grupos activos de este bot?");
          if (!ok) return;
        
          const res = await fetch(`${BOT_PANEL_BASE}/broadcast/free`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              message: message
            })
          });
        
          const data = await res.json();
        
          if (data.ok) {
            document.getElementById("botBroadcastMessage").value = "";
            alert(`Mensaje masivo en cola para ${data.instance}. Total: ${data.total}`);
            startBotBroadcastProgress(data.job_id);
          } else {
            alert(data.error || "No se pudo enviar el mensaje.");
          }
        }
        
        function startBotBroadcastProgress(jobId) {
          const box = document.getElementById("botBroadcastProgress");
        
          if (box) {
            box.style.display = "block";
            box.innerHTML = "Enviando mensajes...";
          }
        
          if (botBroadcastProgressTimer) {
            clearInterval(botBroadcastProgressTimer);
          }
        
          botBroadcastProgressTimer = setInterval(async () => {
            const res = await fetch(`${BOT_PANEL_BASE}/broadcast/progress/${jobId}`);
            const data = await res.json();
        
            if (!data.ok) {
              if (box) box.innerHTML = data.error || "Error consultando progreso.";
              clearInterval(botBroadcastProgressTimer);
              return;
            }
        
            if (box) {
              box.innerHTML = `
                <strong>Estado:</strong> ${data.status || "pending"}<br>
                <strong>Instancia:</strong> ${data.instance || ""}<br>
                <strong>Enviados:</strong> ${data.sent || 0}/${data.total || 0}<br>
                <strong>Errores:</strong> ${data.errors || 0}<br>
                <strong>Saltados:</strong> ${data.skipped || 0}<br>
                <strong>Actual:</strong> ${data.current || ""}
              `;
            }
        
            if (data.status === "done") {
              box.innerHTML += "<br><strong style='color:green;'>✔ Envío terminado</strong>";
            }
            
            if (data.status === "done" || data.status === "error") {
              clearInterval(botBroadcastProgressTimer);
            }
          }, 2000);
        }

        async function addManualBotGroup() {
          const groupJid = document.getElementById("manual_group_jid").value.trim();
          const groupName = document.getElementById("manual_group_name").value.trim();
        
          if (!groupJid) {
            alert("Escribe el Group JID.");
            return;
          }
        
          const res = await fetch(`${BOT_PANEL_BASE}/group/add`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              group_jid: groupJid,
              group_name: groupName
            })
          });
        
          const data = await res.json();
          if (data.ok) {
            location.reload();
          } else {
            alert(data.error || "No se pudo agregar el grupo.");
          }
        }

        async function renameBotGroup(groupJid) {
          const name = document.getElementById(`rename_${groupJid}`).value.trim();
          if (!name) {
            alert("Escribe un nombre");
            return;
          }

          const res = await fetch(`${BOT_PANEL_BASE}/group/${encodeURIComponent(groupJid)}/rename`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ custom_name: name })
          });

          const data = await res.json();
          if (data.ok) location.reload();
          else alert(data.error || "No se pudo renombrar");
        }

        async function assignBotPromo(groupJid) {
          const promoName = document.getElementById(`promo_name_${groupJid}`).value.trim();
          const totalActas = Number(document.getElementById(`promo_total_${groupJid}`).value.trim());
          const pricePerPiece = document.getElementById(`promo_price_${groupJid}`).value.trim();

          if (!totalActas || totalActas < 10) {
            alert("La promoción mínima es de 10 actas");
            return;
          }

          const res = await fetch(`${BOT_PANEL_BASE}/promotion/set`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              group_jid: groupJid,
              promo_name: promoName,
              total_actas: totalActas,
              price_per_piece: pricePerPiece
            })
          });

          const data = await res.json();
          if (data.ok) {
            alert(data.message || "Promoción activada y mensaje enviado al grupo.");
            location.reload();
          } else {
            alert(data.error || "No se pudo aplicar la promoción");
          }
        }

        async function removeBotPromo(groupJid) {
          const ok = confirm("¿Seguro que deseas quitar la promoción de este grupo?");
          if (!ok) return;
        
          const res = await fetch(`${BOT_PANEL_BASE}/promotion/remove`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              group_jid: groupJid
            })
          });
        
          const data = await res.json();
        
          if (data.ok) {
            alert(data.message || "Promoción quitada correctamente.");
            location.reload();
          } else {
            alert(data.error || "No se pudo quitar la promoción");
          }
        }
      </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


def _is_hidden_panel_group(gid: str | None, name: str | None) -> bool:
    gid = (gid or "").strip()
    if gid in HIDDEN_PANEL_GROUPS:
        return True

    name_up = (name or "").strip().upper()

    # Evita falsos positivos como:
    # BELLADIRA, NADIA, ADRIANA, RECLUTADOR.
    excluded_patterns = (
        r"\bPROV\b",
        r"\bPROVEEDOR\b",
        r"\bPROVEEDORES\b",
        r"\bPRUEBA\b",
        r"\bPRUEBAS\b",
        r"\bTEST\b",
        r"\bSOPORTE\b",
        r"\bSTAFF\b",
        r"\bADMIN\b",
        r"\bADMINISTRACION\b",
        r"\bADMINISTRACIÓN\b",
    )

    return any(re.search(pattern, name_up) for pattern in excluded_patterns)


@app.post("/panel/provider-weight")
def panel_provider_weight(payload: dict, db: Session = Depends(get_db)):
    provider_name = str(payload.get("provider_name") or "").strip().upper()
    weight = float(payload.get("weight") or 0)

    if provider_name not in {
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
    }:
        return {"ok": False, "error": "Proveedor inválido"}

    row = _get_or_create_provider(db, provider_name, True)
    row.weight = max(0, weight)
    row.updated_at = _utc_now_naive()
    db.commit()

    _clear_panel_cache()

    return {
        "ok": True,
        "provider_name": provider_name,
        "weight": row.weight,
    }

                    
@app.get("/panel", response_class=HTMLResponse)
def panel_actas(
    request: Request,
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    status: str = "",
    act_type: str = "",
    group_mode: str = "active",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return HTMLResponse("No autorizado", status_code=403)

    current_token = (request.query_params.get("token") or "").strip()

    try:
        cache_key = _panel_cache_key(
            view=view,
            group_jid=group_jid,
            provider_name=provider_name,
            status=status,
            act_type=act_type,
            group_mode=group_mode,
            date_from=date_from,
            date_to=date_to,
        )

        cached_panel = redis_conn.get(cache_key)
        if cached_panel:
            if isinstance(cached_panel, bytes):
                cached_panel = cached_panel.decode("utf-8", errors="ignore")
            return HTMLResponse(
                content=cached_panel,
                headers={
                    "Cache-Control": "no-store, max-age=0, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )

        time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)

        local_start_label = _to_panel_tz(time_min)
        local_end_label = _to_panel_tz(time_max)
        
        if view == "custom" and local_start_label and local_end_label:
            period_label = (
                f"Rango personalizado: "
                f"{local_start_label.strftime('%Y-%m-%d')} "
                f"a {(local_end_label - timedelta(days=1)).strftime('%Y-%m-%d')}"
            )
        else:
            period_label = {
                "day": "Hoy",
                "30d": "Últimos 30 días",
                "month": "Mes actual",
                "prev_month": "Mes anterior",
            }.get(view, view)

        base_q = _query_requests_for_panel(
            db=db,
            time_min=time_min,
            time_max=time_max,
            group_jid=group_jid or None,
            provider_name=provider_name or None,
            status=status or None,
            act_type=act_type or None,
        )

        group_base_q = base_q.filter(RequestLog.instance_name == MAIN_PANEL_INSTANCE)
        
        group_cache = _build_group_name_cache(db)
        delivery_metrics = _panel_delivery_metrics(db, time_min, time_max)
        bot_status_rows = _bot_status_rows(db)
        bot_labels_map = {
            b["instance_name"]: b["label"]
            for b in bot_status_rows
        }

        hidden_main_group_ids = {
            g.group_jid
            for g in (
                db.query(AuthorizedGroup.group_jid)
                .filter(AuthorizedGroup.hidden_in_main == True)
                .all()
            )
        }
        
        status_rows = (
            base_q.with_entities(
                RequestLog.status,
                func.count(RequestLog.id)
            )
            .group_by(RequestLog.status)
            .all()
        )
        
        summary = {
            "total": 0,
            "queued": 0,
            "processing": 0,
            "done": 0,
            "error": 0,
        }
        for st, cnt in status_rows:
            cnt = int(cnt or 0)
            summary["total"] += cnt
            if st == "QUEUED":
                summary["queued"] = cnt
            elif st == "PROCESSING":
                summary["processing"] = cnt
            elif st == "DONE":
                summary["done"] = cnt
            elif st == "ERROR":
                summary["error"] = cnt
        
        include_all_groups = (group_mode == "all")
        has_active_filters = any([
            (group_jid or "").strip(),
            (provider_name or "").strip(),
            (status or "").strip(),
            (act_type or "").strip(),
        ])
        
        group_rows_raw = (
            group_base_q.with_entities(
                RequestLog.source_group_id,
                RequestLog.status,
                func.count(RequestLog.id),
                func.max(RequestLog.updated_at),
            )
            .group_by(RequestLog.source_group_id, RequestLog.status)
            .all()
        )
        
        group_map = {}

        if include_all_groups and not has_active_filters:
            for gid in (set(GROUP_NAME_MAP.keys()) | set(group_cache.keys())):
                gid = gid or "PRIVADO"
                group_name = _group_name_cached(gid, group_cache)
        
                if gid in hidden_main_group_ids:
                    continue
                    
                row = db.query(AuthorizedGroup).filter_by(group_jid=gid).first()
                owner = (row.owner_instance or "").strip() if row else ""
                
                if gid != "PRIVADO" and owner != MAIN_PANEL_INSTANCE:
                    continue
        
                group_map[gid] = {
                    "group_jid": gid,
                    "group_name": group_name,
                    "total": 0,
                    "queued": 0,
                    "processing": 0,
                    "done": 0,
                    "error": 0,
                    "last_update": None,
                }
        
        for gid, st, cnt, last_upd in group_rows_raw:
            gid = gid or "PRIVADO"
            group_name = _group_name_cached(gid, group_cache)
        
            if gid in hidden_main_group_ids:
                continue
        
            row = db.query(AuthorizedGroup).filter_by(group_jid=gid).first()
            owner = (row.owner_instance or "").strip() if row else ""
            
            if gid != "PRIVADO" and owner not in ("", MAIN_PANEL_INSTANCE):
                continue
        
            item = group_map.setdefault(gid, {
                "group_jid": gid,
                "group_name": group_name,
                "total": 0,
                "queued": 0,
                "processing": 0,
                "done": 0,
                "error": 0,
                "last_update": None,
            })
        
            cnt = int(cnt or 0)
            item["total"] += cnt
        
            if st == "QUEUED":
                item["queued"] += cnt
            elif st == "PROCESSING":
                item["processing"] += cnt
            elif st == "DONE":
                item["done"] += cnt
            elif st == "ERROR":
                item["error"] += cnt
        
            if last_upd and (not item["last_update"] or last_upd > item["last_update"]):
                item["last_update"] = last_upd
        
        by_group = list(group_map.values())
        if has_active_filters or not include_all_groups:
            by_group = [x for x in by_group if x["total"] > 0]
        by_group = [x for x in by_group if x["group_jid"] != "PRIVADO" or x["total"] > 0]
        by_group.sort(key=lambda x: ((x["total"] == 0), -x["total"], x["group_name"]))
        
        by_provider_raw = (
            base_q.with_entities(
                RequestLog.provider_name,
                RequestLog.status,
                func.count(RequestLog.id),
            )
            .group_by(RequestLog.provider_name, RequestLog.status)
            .all()
        )
        
        provider_map = {}

        provider_weight_map = {
            r.provider_name: float(r.weight or 0)
            for r in db.query(ProviderSetting).all()
        }
        
        for name, st, cnt in by_provider_raw:
            name = name or "NO IDENTIFICADO"
            item = provider_map.setdefault(
                name,
                {
                    "provider_name": name,
                    "total": 0,
                    "queued": 0,
                    "processing": 0,
                    "done": 0,
                    "error": 0,
                }
            )
        
            cnt = int(cnt or 0)
            item["total"] += cnt
        
            if st == "QUEUED":
                item["queued"] += cnt
            elif st == "PROCESSING":
                item["processing"] += cnt
            elif st == "DONE":
                item["done"] += cnt
            elif st == "ERROR":
                item["error"] += cnt
        
        by_provider = list(provider_map.values())
        by_provider.sort(key=lambda x: (-x["total"], x["provider_name"]))

        provider_accounting = _provider_accounting_data(
            db,
            time_min=time_min,
            time_max=time_max,
            group_jid=group_jid,
            provider_name=provider_name,
            act_type=act_type,
        )

        provider_control_rows = provider_accounting["provider_control_rows"]
        provider_control_totals = provider_accounting["provider_control_totals"]
        
        by_type_raw = (
            base_q.with_entities(
                RequestLog.act_type,
                RequestLog.status,
                func.count(RequestLog.id),
            )
            .group_by(RequestLog.act_type, RequestLog.status)
            .all()
        )
        
        type_map = {}
        
        for name, st, cnt in by_type_raw:
            name = name or "SIN_TIPO"
            item = type_map.setdefault(
                name,
                {
                    "act_type": name,
                    "total": 0,
                    "queued": 0,
                    "processing": 0,
                    "done": 0,
                    "error": 0,
                }
            )
        
            cnt = int(cnt or 0)
            item["total"] += cnt
        
            if st == "QUEUED":
                item["queued"] += cnt
            elif st == "PROCESSING":
                item["processing"] += cnt
            elif st == "DONE":
                item["done"] += cnt
            elif st == "ERROR":
                item["error"] += cnt
        
        by_type = list(type_map.values())
        by_type.sort(key=lambda x: (-x["total"], x["act_type"]))

        by_instance_raw = (
            base_q.with_entities(
                RequestLog.instance_name,
                RequestLog.status,
                func.count(RequestLog.id),
            )
            .group_by(RequestLog.instance_name, RequestLog.status)
            .all()
        )
    
        instance_map = {}
    
        for name, st, cnt in by_instance_raw:
            name = name or "docifybot8"
            item = instance_map.setdefault(
                name,
                {
                    "instance_name": name,
                    "total": 0,
                    "queued": 0,
                    "processing": 0,
                    "done": 0,
                    "error": 0,
                }
            )
    
            cnt = int(cnt or 0)
            item["total"] += cnt
    
            if st == "QUEUED":
                item["queued"] += cnt
            elif st == "PROCESSING":
                item["processing"] += cnt
            elif st == "DONE":
                item["done"] += cnt
            elif st == "ERROR":
                item["error"] += cnt
    
        by_instance = list(instance_map.values())
        by_instance.sort(key=lambda x: (-x["total"], x["instance_name"]))
        
        promo_map = _promotion_summary_map(db)
        
        latest = (
            base_q.with_entities(
                RequestLog.id,
                RequestLog.curp,
                RequestLog.act_type,
                RequestLog.status,
                RequestLog.source_group_id,
                RequestLog.instance_name,
                RequestLog.provider_name,
                RequestLog.provider_group_id,
                RequestLog.created_at,
                RequestLog.updated_at,
                RequestLog.error_message,
            )
            .order_by(RequestLog.created_at.desc())
            .limit(10)
            .all()
        )
        
        subtitle = f"{period_label} ({PANEL_TZ})"
        
        provider_states = _esc(_providers_status_text(db)).replace("\n", "<br>")

        cleanup_enabled = _cleanup_enabled(db)
        cleanup_max_age_minutes = _cleanup_max_age_minutes(db)
        
        cleanup_panel_html = f"""
        <div class="box">
          <div class="head">
            <div>
              <strong>🧹 Cleanup / Atoradas</strong>
              <span class="small">
                Configura el auto-cierre de app.cleanup y limpia solicitudes EN COLA/PROCESANDO viejas.
              </span>
            </div>
          </div>
        
          <div class="cards" style="padding:16px;grid-template-columns:repeat(2,minmax(0,1fr));">
            <div class="card">
              <div class="label">Auto cleanup</div>
              <div class="value" style="font-size:1.6rem;">
                {"ACTIVO" if cleanup_enabled else "DESACTIVADO"}
              </div>
        
              <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
                <label style="display:flex;align-items:center;gap:8px;font-weight:800;">
                  <input id="cleanupEnabled" type="checkbox" {"checked" if cleanup_enabled else ""}>
                  Activar app.cleanup
                </label>
        
                <input
                  id="cleanupMaxAgeMinutes"
                  type="number"
                  min="1"
                  max="1440"
                  step="1"
                  value="{cleanup_max_age_minutes}"
                  style="width:120px;padding:10px;border:1px solid #d1d5db;border-radius:10px;"
                >
        
                <span class="small">minutos máximos antes de cerrar atoradas</span>
        
                <button type="button" class="btn btn-primary" onclick="saveCleanupSettings()">
                  Guardar configuración
                </button>
              </div>
            </div>
        
            <div class="card">
              <div class="label">Borrado manual Redis/RQ + DB</div>
              <div class="value" style="font-size:1.6rem;">EN COLA / PROCESANDO</div>
        
              <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
                <input
                  id="cleanupPurgeMinutes"
                  type="number"
                  min="1"
                  max="1440"
                  step="1"
                  value="{cleanup_max_age_minutes}"
                  style="width:120px;padding:10px;border:1px solid #d1d5db;border-radius:10px;"
                >
        
                <span class="small">borrar con más de X minutos</span>
        
                <button type="button" class="btn btn-warning" onclick="previewCleanupPurge()">
                  Revisar cuántas
                </button>
        
                <button type="button" class="btn btn-danger" onclick="purgeCleanupStuck()">
                  Borrar atoradas
                </button>
              </div>
        
              <div id="cleanupStatusBox" class="small" style="margin-top:12px;"></div>
            </div>
          </div>
        </div>
        """

        metrics_html = ""
        if delivery_metrics:
            metrics_html = f"""
            <div class="box">
              <div class="head">
                <strong>⚡ Métricas de entrega</strong>
                <span class="small">Tiempos promedio del periodo seleccionado.</span>
              </div>
        
              <div class="cards" style="padding:16px; grid-template-columns: repeat(3, minmax(0, 1fr));">
        
                <div class="card">
                  <div class="label">Tiempo proveedor</div>
                  <div class="value">{delivery_metrics["avg_provider"]} s</div>
                </div>
        
                <div class="card">
                  <div class="label">WhatsApp / Evolution</div>
                  <div class="value">{delivery_metrics["avg_whatsapp"]} s</div>
                </div>
        
                <div class="card">
                  <div class="label">Procesamiento bot</div>
                  <div class="value">{delivery_metrics["avg_bot"]} s</div>
                </div>
        
                <div class="card">
                  <div class="label">Entrega total promedio</div>
                  <div class="value">{delivery_metrics["avg_total"]} s</div>
                </div>
        
                <div class="card">
                  <div class="label">Entrega más rápida</div>
                  <div class="value">{delivery_metrics["fastest"]} s</div>
                </div>
        
                <div class="card">
                  <div class="label">Entrega más lenta</div>
                  <div class="value">{delivery_metrics["slowest"]} s</div>
                </div>
        
              </div>
            </div>
            """

        bot_status_html = """
        <div class="box">
          <div class="head">
            <strong>Estado de bots WhatsApp</strong>
            <span class="small">Monitorea el estado, uso y actividad de cada bot en tiempo real.</span>
          </div>
        
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Bot</th>
                  <th>Instancia</th>
                  <th>Estado WhatsApp</th>
                  <th>Bloqueado</th>
                  <th>Uso</th>
                  <th>Solicitudes</th>
                  <th>Token</th>
                  <th>QR</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
        """
        
        for b in bot_status_rows:
            state = b["state"]
            color = "green" if state == "open" else "red" if state == "close" else "#92400e"

            status_label = "🟢 Conectado" if state == "open" else "🔴 Desconectado" if state == "close" else "🟡 Desconocido"
        
            used_txt = f'{b["used"]}/{b["limit"]}' if b["limit"] else str(b["used"])

            if state != "open":
                action_html = f'<button class="btn btn-primary" type="button" onclick="getBotQr(\'{_esc(b["instance_name"])}\')">Reconectar / QR</button>'
            else:
                action_html = '<span class="badge badge-success">Conectado</span>'

            actions_html = f"""
            <button class="btn btn-warning" onclick="disconnectBot('{_esc(b["instance_name"])}')">Desconectar</button>
            <button class="btn btn-danger" onclick="hideBot('{_esc(b["instance_name"])}')">Ocultar</button>
            """
        
            bot_status_html += f"""
                <tr>
                  <td>{_esc(b["label"])}</td>
                  <td class="mono">{_esc(b["instance_name"])}</td>
                  <td style="font-weight:800;color:{color};">{status_label}</td>
                  <td>{'Sí' if b["blocked"] else 'No'}</td>
                  <td>{_esc(used_txt)}</td>
                  <td>{b["total_requests"]}</td>
                  <td class="mono">{_esc(b.get("panel_token") or "-")}</td>
                  <td>{action_html}</td>
                  <td>{actions_html}</td>
                </tr>
            """
        
        bot_status_html += """
              </tbody>
            </table>
          </div>
        
          <div class="box" style="margin-top:14px;">
            <div class="head"><strong>Nuevo Bot</strong></div>
        
            <div class="filters" style="grid-template-columns: 1fr 1fr 180px;">
              <div>
                <input id="newBotLabel" placeholder="Nombre">
              </div>
        
              <div>
                <input id="newBotInstance" placeholder="Instancia">
              </div>
        
              <div>
                <button class="btn btn-primary" onclick="createBot()">Crear</button>
              </div>
            </div>
          </div>
        
          <div id="botQrBox" style="margin-top:14px;"></div>
        </div>
        """

        api_clients_panel_html = """
        <div class="box" id="apiClientsSection">
          <div class="head">
            <div>
              <strong>🔑 API externa · clientes y saldos</strong>
              <div class="small">
                Cada cliente API se asigna automáticamente a DOCIFYBOT8
                como grupo virtual: API - Nombre del cliente.
              </div>
            </div>
        
            <button type="button" class="btn btn-primary" onclick="loadApiClients()">
              Actualizar API
            </button>
          </div>
        
          <div style="padding:16px;border-bottom:1px solid #e5e7eb;">
            <strong>Crear cliente API</strong>
        
            <div
              style="
                display:grid;
                grid-template-columns:2fr 1fr 1fr auto;
                gap:10px;
                margin-top:12px;
              "
            >
              <input
                id="apiNewName"
                class="input"
                placeholder="Nombre del cliente / programador"
              >
        
              <input
                id="apiNewCredit"
                class="input"
                type="number"
                min="0"
                step="0.01"
                value=""
                placeholder="Saldo inicial"
              >
        
              <input
                id="apiNewPrice"
                class="input"
                type="number"
                min="0.01"
                step="0.01"
                value=""
                placeholder="Precio por acta"
              >
        
              <button
                type="button"
                class="btn btn-success"
                onclick="createApiClient()"
              >
                Crear API key
              </button>
            </div>
        
            <div
              id="apiKeyCreatedBox"
              style="
                display:none;
                margin-top:14px;
                padding:12px;
                border-radius:12px;
                background:#ecfdf5;
                border:1px solid #86efac;
              "
            ></div>
          </div>
        
          <div id="apiClientsWrap" style="padding:16px;">
            <div class="small">Cargando clientes API...</div>
          </div>
        </div>
        """

        api_clients_panel_js = r"""
        const apiPanelToken = new URLSearchParams(window.location.search).get("token") || "";
        
        function apiMoney(value) {
          const n = Number(value || 0);
        
          return n.toLocaleString("es-MX", {
            style: "currency",
            currency: "MXN"
          });
        }
        
        function escapeApiHtml(value) {
          return String(value ?? "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#039;");
        }
        
        function escapeApiJs(value) {
          return String(value ?? "")
            .replaceAll("\\", "\\\\")
            .replaceAll("'", "\\'")
            .replaceAll("\n", "\\n")
            .replaceAll("\r", "");
        }
        
        async function apiPanelFetch(path, options = {}) {
          const joiner = path.includes("?") ? "&" : "?";
          const url = `${path}${joiner}token=${encodeURIComponent(apiPanelToken)}`;
        
          const response = await fetch(url, options);
        
          let data = {};
        
          try {
            data = await response.json();
          } catch (_) {}
        
          if (!response.ok || !data.ok) {
            throw new Error(
              data.error ||
              data.message ||
              "No se pudo completar la acción."
            );
          }
        
          return data;
        }
        
        async function loadApiClients() {
          const wrap = document.getElementById("apiClientsWrap");
        
          if (!wrap) return;
        
          wrap.innerHTML = '<div class="small">Actualizando clientes API...</div>';
        
          try {
            const data = await apiPanelFetch("/panel/api/clients");
            const rows = data.items || [];
        
            if (!rows.length) {
              wrap.innerHTML = `
                <div class="small">
                  Aún no hay clientes API registrados.
                </div>
              `;
              return;
            }
        
            let output = `
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Cliente</th>
                      <th>Estado</th>
                      <th>Grupo DOCIFYBOT8</th>
                      <th>Saldo</th>
                      <th>Reservado</th>
                      <th>Disponible</th>
                      <th>Precio</th>
                      <th>DONE</th>
                      <th>Pendientes</th>
                      <th>Errores</th>
                      <th>Acciones</th>
                    </tr>
                  </thead>
                  <tbody>
            `;
        
            for (const item of rows) {
              const clientId = Number(item.id);
        
              const status = item.is_active
                ? '<span class="badge badge-success">ACTIVO</span>'
                : '<span class="badge badge-danger">INACTIVO</span>';
        
              output += `
                <tr>
                  <td>
                    <strong>${escapeApiHtml(item.name)}</strong><br>
                    <span class="small">ID ${clientId}</span>
                  </td>
        
                  <td>${status}</td>
        
                  <td>
                    <strong>${escapeApiHtml(item.panel_instance_name || "docifybot8")}</strong><br>
                    <span class="small">${escapeApiHtml(item.panel_group_jid || "")}</span>
                  </td>
        
                  <td>${apiMoney(item.balance)}</td>
                  <td>${apiMoney(item.reserved)}</td>
                  <td><strong>${apiMoney(item.available)}</strong></td>
                  <td>${apiMoney(item.price_per_done)}</td>
                  <td>${Number(item.done_count || 0)}</td>
                  <td>${Number(item.pending_count || 0)}</td>
                  <td>${Number(item.error_count || 0)}</td>
        
                  <td>
                    <div style="display:flex;flex-wrap:wrap;gap:6px;min-width:250px;">
                      <button
                        type="button"
                        class="btn btn-success"
                        style="padding:7px 9px;font-size:12px;"
                        onclick="rechargeApiClient(${clientId})"
                      >
                        Recargar
                      </button>
        
                      <button
                        type="button"
                        class="btn btn-primary"
                        style="padding:7px 9px;font-size:12px;"
                        onclick="editApiClient(
                          ${clientId},
                          '${escapeApiJs(item.name)}',
                          '${item.price_per_done}',
                          ${item.is_active ? "true" : "false"}
                        )"
                      >
                        Configurar
                      </button>
        
                      <button
                        type="button"
                        class="btn btn-warning"
                        style="padding:7px 9px;font-size:12px;"
                        onclick="rotateApiKey(
                          ${clientId},
                          '${escapeApiJs(item.name)}'
                        )"
                      >
                        Regenerar key
                      </button>
                    </div>
                  </td>
                </tr>
              `;
            }
        
            output += `
                  </tbody>
                </table>
              </div>
            `;
        
            wrap.innerHTML = output;
        
          } catch (error) {
            wrap.innerHTML = `
              <div style="color:#b91c1c;font-weight:700;">
                Error API: ${escapeApiHtml(error.message || String(error))}
              </div>
            `;
          }
        }
        
        async function createApiClient() {
          const name = document.getElementById("apiNewName")?.value.trim() || "";
          const creditBalance = document.getElementById("apiNewCredit")?.value.trim() || "0";
          const pricePerDone = document.getElementById("apiNewPrice")?.value.trim() || "5";
          const keyBox = document.getElementById("apiKeyCreatedBox");
        
          if (!name) {
            alert("Escribe el nombre del cliente API.");
            return;
          }
        
          if (!confirm(`¿Crear API key para ${name}? Quedará asociado a DOCIFYBOT8.`)) {
            return;
          }
        
          try {
            const data = await apiPanelFetch("/panel/api/clients", {
              method: "POST",
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                name,
                credit_balance: creditBalance,
                price_per_done: pricePerDone
              })
            });
        
            if (keyBox) {
              keyBox.style.display = "block";
              keyBox.innerHTML = `
                <strong>✅ Cliente creado: ${escapeApiHtml(data.name)}</strong><br>
                <span>Grupo virtual: <code>${escapeApiHtml(data.panel_group_jid)}</code></span><br>
                <span>Precio: ${apiMoney(data.price_per_done)} por acta DONE</span><br><br>
                <strong>API key — cópiala ahora:</strong><br>
                <code
                  style="
                    display:block;
                    overflow-wrap:anywhere;
                    margin-top:8px;
                    padding:9px;
                    background:#fff;
                    border:1px solid #bbf7d0;
                    border-radius:8px;
                  "
                >${escapeApiHtml(data.api_key)}</code>
              `;
            }
        
            document.getElementById("apiNewName").value = "";
            document.getElementById("apiNewCredit").value = "0";
            document.getElementById("apiNewPrice").value = "5";
        
            await loadApiClients();
        
          } catch (error) {
            alert(error.message || String(error));
          }
        }
        
        async function rechargeApiClient(clientId) {
          const amount = prompt("Monto de recarga en MXN:");
        
          if (amount === null) return;
        
          const note = prompt(
            "Nota de la recarga:",
            "Recarga desde panel principal"
          );
        
          if (note === null) return;
        
          try {
            const data = await apiPanelFetch(
              `/panel/api/clients/${clientId}/recharge`,
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json"
                },
                body: JSON.stringify({
                  amount,
                  note
                })
              }
            );
        
            alert(`Recarga aplicada. Saldo actual: ${apiMoney(data.balance)}`);
            await loadApiClients();
        
          } catch (error) {
            alert(error.message || String(error));
          }
        }
        
        async function editApiClient(
          clientId,
          currentName,
          currentPrice,
          currentActive
        ) {
          const name = prompt("Nombre del cliente:", currentName);
        
          if (name === null) return;
        
          const price = prompt("Precio por acta DONE:", currentPrice);
        
          if (price === null) return;
        
          const activeText = prompt(
            "¿Cliente activo? Escribe SI o NO:",
            currentActive ? "SI" : "NO"
          );
        
          if (activeText === null) return;
        
          const isActive = [
            "SI",
            "S",
            "YES",
            "Y",
            "1",
            "TRUE"
          ].includes(activeText.trim().toUpperCase());
        
          try {
            const data = await apiPanelFetch(
              `/panel/api/clients/${clientId}/settings`,
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json"
                },
                body: JSON.stringify({
                  name,
                  price_per_done: price,
                  is_active: isActive
                })
              }
            );
        
            alert(
              `Cliente actualizado.\n` +
              `Bot: ${data.panel_instance_name}\n` +
              `Grupo: ${data.panel_group_jid}`
            );
        
            await loadApiClients();
        
          } catch (error) {
            alert(error.message || String(error));
          }
        }
        
        async function rotateApiKey(clientId, clientName) {
          if (!confirm(
            `¿Regenerar API key de ${clientName}?\n\n` +
            "La key anterior dejará de funcionar inmediatamente."
          )) {
            return;
          }
        
          try {
            const data = await apiPanelFetch(
              `/panel/api/clients/${clientId}/rotate-key`,
              {
                method: "POST"
              }
            );
        
            const box = document.getElementById("apiKeyCreatedBox");
        
            if (box) {
              box.style.display = "block";
              box.innerHTML = `
                <strong>🔐 Nueva API key para ${escapeApiHtml(clientName)}</strong><br>
                <span>La API key anterior ya quedó invalidada.</span><br><br>
                <code
                  style="
                    display:block;
                    overflow-wrap:anywhere;
                    padding:9px;
                    background:#fff;
                    border:1px solid #fde68a;
                    border-radius:8px;
                  "
                >${escapeApiHtml(data.api_key)}</code>
              `;
            }
        
          } catch (error) {
            alert(error.message || String(error));
          }
        }
        
        document.addEventListener("DOMContentLoaded", () => {
          loadApiClients();
        });
        """
    
        html = f"""
        <!doctype html>
        <html lang="es">
        <head>
          <meta charset="utf-8">
          <title>Panel Actas</title>
          <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
              :root {{
                --bg: #f4f6f8;
                --card: #ffffff;
                --text: #1f2937;
                --muted: #6b7280;
                --line: #e5e7eb;
            
                --primary: #334155;
                --primary-dark: #1e293b;
            
                --success: #166534;
                --success-dark: #14532d;
            
                --warning: #a16207;
                --warning-dark: #854d0e;
            
                --danger: #991b1b;
                --danger-dark: #7f1d1d;
            
                --shadow: 0 8px 24px rgba(15, 23, 42, 0.07);
                --radius: 18px;
              }}
            
              * {{
                box-sizing: border-box;
              }}
            
              body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: var(--bg);
                color: var(--text);
              }}
            
              .wrap {{
                max-width: 1500px;
                margin: 0 auto;
                padding: 16px;
              }}
            
              .hero {{
                background: linear-gradient(135deg, #1f2937 0%, #334155 55%, #475569 100%);
                color: white;
                border-radius: 24px;
                padding: 22px;
                margin-bottom: 18px;
                box-shadow: var(--shadow);
              }}
            
              .hero-top {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 16px;
                flex-wrap: wrap;
              }}
            
              .hero h1 {{
                margin: 0 0 8px;
                font-size: 1.9rem;
              }}
            
              .hero-sub {{
                color: rgba(255,255,255,.88);
                font-size: .98rem;
              }}
            
              .toolbar {{
                margin-top: 16px;
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
              }}
            
              .tool-link {{
                text-decoration: none;
                padding: 10px 16px;
                border-radius: 12px;
                background: rgba(255,255,255,.10);
                color: white;
                font-weight: 700;
                border: 1px solid rgba(255,255,255,.14);
                transition: .2s ease;
              }}
            
              .tool-link:hover {{
                background: rgba(255,255,255,.16);
              }}
            
              .tool-link-active {{
                background: #ffffff;
                color: var(--primary-dark);
                border-color: #ffffff;
              }}
            
              .grid-hero {{
                display: grid;
                grid-template-columns: 1.2fr 1fr;
                gap: 16px;
                margin-top: 18px;
              }}
            
              .glass {{
                background: rgba(255,255,255,.08);
                border: 1px solid rgba(255,255,255,.10);
                border-radius: 20px;
                padding: 18px;
                backdrop-filter: blur(8px);
              }}
            
              .section-title {{
                margin: 0 0 14px;
                font-size: 1rem;
                font-weight: 800;
                letter-spacing: .2px;
              }}
            
              .provider-grid {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 12px;
              }}
            
              .provider-card {{
                background: rgba(255,255,255,.08);
                border: 1px solid rgba(255,255,255,.12);
                border-radius: 16px;
                padding: 14px;
              }}
            
              .provider-name {{
                font-weight: 800;
                margin-bottom: 10px;
                font-size: .98rem;
              }}
            
              .provider-actions {{
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
              }}
            
              .status-panel {{
                margin-top: 14px;
                padding: 12px 14px;
                border-radius: 14px;
                background: rgba(255,255,255,.08);
                border: 1px solid rgba(255,255,255,.10);
                color: rgba(255,255,255,.94);
                font-size: .92rem;
                line-height: 1.5;
              }}
            
              .broadcast-grid {{
                display: grid;
                gap: 12px;
              }}
            
              .broadcast-buttons {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 10px;
              }}
            
              .broadcast-free {{
                display: grid;
                gap: 10px;
              }}
            
              .broadcast-free textarea {{
                width: 100%;
                min-height: 140px;
                border: 1px solid #d1d5db;
                border-radius: 14px;
                padding: 12px 14px;
                resize: vertical;
                font: inherit;
                color: var(--text);
                background: white;
              }}
            
              .box {{
                background: var(--card);
                border-radius: var(--radius);
                box-shadow: var(--shadow);
                overflow: hidden;
                margin-bottom: 16px;
                border: 1px solid #eef2f7;
              }}
            
              .head {{
                padding: 16px 18px;
                border-bottom: 1px solid var(--line);
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 10px;
                flex-wrap: wrap;
                background: #fafbfc;
              }}
            
              .head strong {{
                font-size: 1rem;
              }}
            
              .filters {{
                display: grid;
                grid-template-columns: repeat(5, minmax(0, 1fr));
                gap: 10px;
                padding: 16px;
              }}
            
              .filters input,
              .filters select,
              .input,
              .textarea {{
                width: 100%;
                padding: 11px 12px;
                border: 1px solid #d1d5db;
                border-radius: 12px;
                font: inherit;
                background: white;
                color: var(--text);
                outline: none;
              }}
            
              .filters input:focus,
              .filters select:focus,
              .input:focus,
              .textarea:focus {{
                border-color: var(--primary);
                box-shadow: 0 0 0 3px rgba(51, 65, 85, .10);
              }}
            
              .cards {{
                display: grid;
                grid-template-columns: repeat(5, minmax(0, 1fr));
                gap: 12px;
                margin-bottom: 16px;
              }}
            
              .card {{
                background: var(--card);
                border-radius: 18px;
                padding: 16px;
                box-shadow: var(--shadow);
                border: 1px solid var(--line);
                position: relative;
              }}
            
              .card::before {{
                content: "";
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 4px;
                border-radius: 18px 18px 0 0;
                background: #cbd5e1;
              }}
            
              .label {{
                color: var(--muted);
                font-size: .88rem;
                margin-bottom: 8px;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: .3px;
              }}
            
              .value {{
                font-size: 1.9rem;
                font-weight: 800;
                line-height: 1;
              }}
            
              .table-wrap {{
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
              }}
            
              .table-wrap table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 1100px;
              }}
            
              th, td {{
                padding: 12px;
                border-bottom: 1px solid var(--line);
                text-align: left;
                vertical-align: top;
                font-size: .95rem;
              }}
            
              th {{
                background: #1f2937;
                color: white;
                position: sticky;
                top: 0;
                z-index: 1;
              }}
            
              tr:hover td {{
                background: #f9fafb;
              }}
            
              .right {{
                text-align: right;
              }}
            
              .mono {{
                font-family: Consolas, Monaco, monospace;
                font-size: .9rem;
              }}
            
              .small {{
                color: var(--muted);
                font-size: .84rem;
                line-height: 1.45;
              }}
            
              .status-q {{
                color: #a16207;
                font-weight: 800;
              }}
            
              .status-p {{
                color: #334155;
                font-weight: 800;
              }}
            
              .status-d {{
                color: #166534;
                font-weight: 800;
              }}
            
              .status-e {{
                color: #991b1b;
                font-weight: 800;
              }}
            
              .btn {{
                border: none;
                border-radius: 12px;
                padding: 10px 14px;
                font-weight: 800;
                font-size: .95rem;
                cursor: pointer;
                transition: .2s ease;
                font-family: inherit;
              }}
            
              .btn:hover {{
                transform: translateY(-1px);
              }}
            
              .btn-primary {{
                background: var(--primary);
                color: white;
              }}
            
              .btn-primary:hover {{
                background: var(--primary-dark);
              }}
            
              .btn-success {{
                background: var(--success);
                color: white;
              }}
            
              .btn-success:hover {{
                background: var(--success-dark);
              }}
            
              .btn-danger {{
                background: var(--danger);
                color: white;
              }}
            
              .btn-danger:hover {{
                background: var(--danger-dark);
              }}
            
              .btn-warning {{
                background: var(--warning);
                color: white;
              }}
            
              .btn-warning:hover {{
                background: var(--warning-dark);
              }}
            
              .btn-light {{
                background: #e5e7eb;
                color: #111827;
              }}
            
              .btn-light:hover {{
                background: #d1d5db;
              }}

              .btn-closed {{
                background: #374151;
                color: white;
              }}
            
              .btn-closed:hover {{
                background: #1f2937;
              }}
            
              .actions-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
              }}
            
              .helper {{
                color: rgba(255,255,255,.82);
                font-size: .86rem;
                line-height: 1.45;
              }}
    
              a.btn {{
                text-decoration: none !important;
              }}
            
              a.btn:hover {{
                text-decoration: none !important;
              }}
    
              .group-mode-bar {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                padding: 16px;
              }}
            
              .group-mode-link {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 10px 14px;
                border-radius: 12px;
                background: #f8fafc;
                border: 1px solid #dbe3ee;
                color: #1d4ed8;
                font-weight: 700;
                text-decoration: none !important;
                transition: .2s ease;
              }}
            
              .group-mode-link:hover {{
                background: #eff6ff;
                border-color: #bfdbfe;
                text-decoration: none !important;
              }}
            
              .group-mode-link-active {{
                background: #dbeafe;
                border-color: #93c5fd;
                color: #1e3a8a;
              }}
    
              .table-wrap td a {{
                color: #1d4ed8;
                text-decoration: none !important;
                font-weight: 700;
              }}
            
              .table-wrap td a:hover {{
                color: #1e3a8a;
              }}

              .badge {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 4px 10px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 700;
                white-space: nowrap;
              }}
            
              .badge-light {{
                background: #eef2ff;
                color: #3730a3;
              }}

              .badge-success{{
                background:#dcfce7;
                color:#166534;
              }}
            
              .badge-warning {{
                background: #fff7ed;
                color: #c2410c;
              }}
            
              .badge-danger {{
                background: #fef2f2;
                color: #b91c1c;
              }}

              .shared-promo-actions{{
                display:flex;
                justify-content:center;
                align-items:center;
                gap:14px;
                margin-top:18px;
                padding:12px 0 4px 0;
              }}
            
              .shared-promo-actions .btn{{
                min-width:220px;
              }}

              .collapsible-head{{
                display:flex;
                align-items:center;
                justify-content:space-between;
                cursor:pointer;
                user-select:none;
              }}
            
              .collapse-icon{{
                font-size:14px;
                font-weight:700;
                transition:transform .18s ease;
              }}
            
              .collapsible-head.closed .collapse-icon{{
                transform:rotate(-90deg);
              }}
            
              .collapsible-body.open{{
                display:block;
              }}
            
              .collapsible-body.closed{{
                display:none;
              }}

              .broadcast-header {{
                display: flex;
                justify-content: space-between;
                align-items: end;
                gap: 16px;
                margin-bottom: 18px;
                flex-wrap: wrap;
              }}
            
              .broadcast-target {{
                min-width: 240px;
                max-width: 320px;
                width: 100%;
              }}
            
              .broadcast-label {{
                display: block;
                font-size: .9rem;
                font-weight: 700;
                margin-bottom: 6px;
                color: #e5e7eb;
              }}
            
              .broadcast-select {{
                width: 100%;
                border: 1px solid rgba(255,255,255,.14);
                background: rgba(255,255,255,.08);
                color: white;
                border-radius: 12px;
                padding: 11px 12px;
                font: inherit;
                outline: none;
              }}
            
              .broadcast-select option {{
                color: #111827;
                background: white;
              }}
            
              .broadcast-section {{
                display: grid;
                gap: 18px;
              }}
            
              .broadcast-block {{
                background: rgba(255,255,255,.06);
                border: 1px solid rgba(255,255,255,.08);
                border-radius: 18px;
                padding: 16px;
              }}
            
              .broadcast-block-title {{
                font-size: 1rem;
                font-weight: 800;
                margin-bottom: 6px;
                color: white;
              }}
            
              .broadcast-buttons-grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 12px;
              }}
            
              .broadcast-buttons-grid .btn {{
                width: 100%;
                min-height: 52px;
                white-space: normal;
                line-height: 1.2;
                text-align: center;
              }}
            
              .broadcast-textarea {{
                width: 100%;
                min-height: 120px;
                resize: vertical;
                border: 1px solid rgba(255,255,255,.12);
                background: white;
                color: #111827;
                border-radius: 16px;
                padding: 14px 16px;
                font: inherit;
                box-sizing: border-box;
                outline: none;
              }}
            
              .broadcast-textarea:focus {{
                border-color: rgba(255,255,255,.35);
                box-shadow: 0 0 0 3px rgba(255,255,255,.10);
              }}
            
              .broadcast-actions {{
                display: flex;
                gap: 10px;
                margin-top: 14px;
                flex-wrap: wrap;
              }}

              .table-wrap input[type="number"]{{
                width: 100%;
                padding: 10px 12px;
                border: 1px solid #d1d5db;
                border-radius: 10px;
                font: inherit;
                background: white;
                color: #1f2937;
                outline: none;
                box-sizing: border-box;
              }}
            
              .table-wrap input[type="number"]:focus{{
                border-color: #334155;
                box-shadow: 0 0 0 3px rgba(51, 65, 85, .10);
              }}

              .provider-placeholder {{
                visibility: hidden;
                pointer-events: none;
                box-shadow: none;
                background: transparent;
                border: none;
              }}
            
              @media (max-width: 1200px) {{
                .grid-hero {{
                  grid-template-columns: 1fr;
                }}
            
                .provider-grid {{
                  grid-template-columns: 1fr;
                }}
            
                .broadcast-buttons {{
                  grid-template-columns: 1fr;
                }}
            
                .cards {{
                  grid-template-columns: repeat(3, minmax(0, 1fr));
                }}
              }}
            
              @media (max-width: 900px) {{
                .wrap {{
                  padding: 12px;
                }}
            
                .hero {{
                  padding: 18px;
                  border-radius: 20px;
                }}
            
                .hero h1 {{
                  font-size: 1.45rem;
                }}
            
                .cards {{
                  grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
            
                .filters {{
                  grid-template-columns: 1fr;
                }}
            
                .head {{
                  padding: 14px 16px;
                }}
            
                .card {{
                  padding: 14px;
                }}
            
                .value {{
                  font-size: 1.6rem;
                }}

                .broadcast-buttons-grid {{
                  grid-template-columns: 1fr;
                }}
            
                .broadcast-actions {{
                  flex-direction: column;
                }}
            
                .broadcast-actions .btn {{
                  width: 100%;
                }}
              }}
            
              @media (max-width: 560px) {{
                .cards {{
                  grid-template-columns: 1fr;
                }}
            
                .tool-link,
                .btn {{
                  width: 100%;
                  justify-content: center;
                }}
            
                .provider-actions,
                .actions-row {{
                  flex-direction: column;
                }}
              }}
            </style>
        </head>
        
        <body>
          <div class="wrap">
        
            <div class="hero">
              <div class="hero-top">
                <div>
                  <h1>Panel de Actas</h1>
                  <div class="hero-sub">{_esc(subtitle)}</div>
                </div>
              </div>
        
              <div class="toolbar">
                <a href="/panel?token=docifymx2026&view=day&group_mode={_esc(group_mode)}" class="tool-link {'tool-link-active' if view == 'day' else ''}">Hoy</a>
                <a href="/panel?token=docifymx2026&view=month&group_mode={_esc(group_mode)}" class="tool-link {'tool-link-active' if view == 'month' else ''}">Mes actual</a>
                <a href="/panel?token=docifymx2026&view=prev_month&group_mode={_esc(group_mode)}" class="tool-link {'tool-link-active' if view == 'prev_month' else ''}">Mes anterior</a>
                <a href="/panel/promotions/report" class="tool-link" target="_blank">Promociones</a>
              </div>

              <form method="get" action="/panel" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px;">
                <input type="hidden" name="token" value="{_esc(settings.ADMIN_PANEL_TOKEN)}">
                <input type="hidden" name="view" value="custom">
                <input type="hidden" name="group_jid" value="{_esc(group_jid)}">
                <input type="hidden" name="provider_name" value="{_esc(provider_name)}">
                <input type="hidden" name="status" value="{_esc(status)}">
                <input type="hidden" name="act_type" value="{_esc(act_type)}">
                <input type="hidden" name="group_mode" value="{_esc(group_mode)}">

                <input
                  type="date"
                  name="date_from"
                  value="{_esc(date_from)}"
                  style="padding:8px 10px;border-radius:10px;border:1px solid #cbd5e1;"
                >

                <input
                  type="date"
                  name="date_to"
                  value="{_esc(date_to)}"
                  style="padding:8px 10px;border-radius:10px;border:1px solid #cbd5e1;"
                >

                <button
                  type="submit"
                  class="tool-link {'tool-link-active' if view == 'custom' else ''}"
                  style="border:0;cursor:pointer;"
                >
                  Aplicar rango
                </button>
              </form>
        
              <div class="grid-hero">
                <div class="glass">
                  <h3 class="section-title">Proveedores</h3>
            
                  <div class="provider-grid">

                    <div class="provider-card">
                      <div class="provider-name">ESCALANTE</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER6" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER6', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER6')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER6','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER6','off')">Desactivar</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">ADMIN</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER1" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER1', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER1')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER1','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER1','off')">Desactivar</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">LUIS</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER5" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER5', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER5')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER5','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER5','off')">Desactivar</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">VILLAFUERTE</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER12" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER12', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER12')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER12','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER12','off')">Desactivar</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">RL</div>
                    
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">
                          Prioridad de uso
                        </div>
                    
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input
                              id="weight_PROVIDER13"
                              type="number"
                              min="0"
                              step="0.1"
                              value="{provider_weight_map.get('PROVIDER13', 0)}"
                              style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;"
                            >
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                    
                          <button
                            class="btn btn-primary"
                            onclick="saveProviderWeight('PROVIDER13')"
                          >
                            Aplicar
                          </button>
                        </div>
                    
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">
                          Más alto = este proveedor se usa más seguido
                        </div>
                      </div>
                    
                      <div class="provider-actions">
                        <button
                          class="btn btn-success"
                          onclick="toggleProvider('PROVIDER13','on')"
                        >
                          Activar
                        </button>
                    
                        <button
                          class="btn btn-danger"
                          onclick="toggleProvider('PROVIDER13','off')"
                        >
                          Desactivar
                        </button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">E-BOT</div>
                    
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">
                          Prioridad de uso
                        </div>
                    
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input
                              id="weight_PROVIDER14"
                              type="number"
                              min="0"
                              step="0.1"
                              value="{provider_weight_map.get('PROVIDER14', 0)}"
                              style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;"
                            >
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                    
                          <button
                            class="btn btn-primary"
                            onclick="saveProviderWeight('PROVIDER14')"
                          >
                            Aplicar
                          </button>
                        </div>
                    
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">
                          Más alto = este proveedor se usa más seguido
                        </div>
                      </div>
                    
                      <div class="provider-actions">
                        <button
                          class="btn btn-success"
                          onclick="toggleProvider('PROVIDER14','on')"
                        >
                          Activar
                        </button>
                    
                        <button
                          class="btn btn-danger"
                          onclick="toggleProvider('PROVIDER14','off')"
                        >
                          Desactivar
                        </button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">EMILIANO</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER9" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER9', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER9')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER9','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER9','off')">Desactivar</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">LAZARO 1</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER4" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER4', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER4')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER4','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER4','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshHID()">Actualizar HID</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">LAZARO 2</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER10" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER10', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER10')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER10','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER10','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshHID10()">Actualizar HID</button>
                      </div>
                    </div>

                    <div class="provider-card">
                      <div class="provider-name">LAZARO 3</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Prioridad de uso</div>
                        <div style="display:flex;align-items:center;justify-content:flex-start;gap:8px;flex-wrap:wrap;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER11" type="number" min="0" step="0.1" value="{provider_weight_map.get('PROVIDER11', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">nivel</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER11')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más alto = este proveedor se usa más seguido</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER11','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER11','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshHID11()">Actualizar HID</button>
                      </div>
                    </div>
                  </div>
                  
                  <div class="status-panel">
                    <strong>Estado actual</strong><br><br>
                    {provider_states}
                  </div>
                </div>
        
                <div class="glass">
                  <div class="broadcast-header">
                    <div>
                      <h3 class="section-title" style="margin-bottom:6px;">Mensajes masivos</h3>
                    </div>
                
                    <div class="broadcast-target">
                      <label for="broadcastCategory" class="broadcast-label">Enviar a</label>
                      <select id="broadcastCategory" class="broadcast-select">
                        <option value="all">Todos</option>
                        <option value="papeleria_ciber">Papelería / Ciber</option>
                        <option value="gestor">Gestores</option>
                        <option value="otro">Otros</option>
                      </select>
                    </div>
                  </div>
                
                  <div class="broadcast-section">
                    <div class="broadcast-block">
                      <div class="broadcast-block-title">Mensajes predefinidos</div>
                
                      <div class="broadcast-buttons-grid">
                        <button class="btn btn-success" onclick="sendBroadcast('activas')">Servicio activo</button>
                        <button class="btn btn-warning" onclick="sendBroadcast('restablecido')">Servicio restablecido</button>
                        <button class="btn btn-danger" onclick="sendBroadcast('suspendido')">Servicio suspendido</button>
                        <button class="btn btn-closed" onclick="sendBroadcast('cerrado')">Servicio cerrado</button>
                      </div>
                    </div>
                
                    <div class="broadcast-block">
                      <div class="broadcast-block-title">Mensaje libre</div>
                
                      <textarea
                        id="broadcastMessage"
                        class="broadcast-textarea"
                        placeholder="Escribe aquí el mensaje que deseas enviar..."
                      ></textarea>

                      <div style="margin-top:10px;padding:10px;border:1px dashed #e5e7eb;border-radius:12px;background:rgba(255,255,255,.08);">
                        <div style="font-size:12px;font-weight:700;margin-bottom:8px;">
                          Audio opcional
                        </div>
                    
                        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
                          <button type="button" class="btn btn-warning" onclick="startPanelAudioRecording('free')">
                            Grabar audio
                          </button>
                    
                          <button type="button" class="btn btn-danger" onclick="stopPanelAudioRecording()" disabled id="freeAudioStopBtn">
                            Detener
                          </button>
                    
                          <button type="button" class="btn btn-light" onclick="clearPanelAudio('free')">
                            Quitar audio
                          </button>
                    
                          <span id="freeAudioStatus" style="font-size:12px;color:#d1d5db;font-weight:700;">
                            Sin audio
                          </span>
                        </div>
                    
                        <audio id="freeAudioPreview" controls style="display:none;width:100%;margin-top:8px;"></audio>
                      </div>
                
                      <div class="broadcast-actions">
                        <button class="btn btn-success" onclick="sendFreeBroadcast()">Enviar mensaje libre</button>
                        <button class="btn btn-light" onclick="clearBroadcast()">Limpiar</button>
                      </div>
                    </div>

                    <div class="broadcast-block">
                      <div class="broadcast-block-title">Mensaje privado a bots internos</div>
                    
                      <div style="font-size:12px;color:#d1d5db;margin-bottom:8px;font-weight:600;">
                        Selecciona qué bots internos recibirán el aviso por privado.
                      </div>

                      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px;">
                        <button
                          type="button"
                          class="btn btn-light"
                          style="font-size:11px;padding:6px 9px;"
                          onclick="selectAllPrivateBots(true)"
                        >
                          Seleccionar todos configurados
                        </button>
                    
                        <button
                          type="button"
                          class="btn btn-light"
                          style="font-size:11px;padding:6px 9px;"
                          onclick="selectAllPrivateBots(false)"
                        >
                          Quitar selección
                        </button>
                    
                        <span
                          id="privateBotSelectedCount"
                          style="font-size:11px;color:#d1d5db;font-weight:700;"
                        >
                          0 seleccionados
                        </span>
                      </div>
                    
                      <div
                        id="privateBotTargets"
                        style="max-height:220px;overflow:auto;border:1px solid #e5e7eb;border-radius:12px;padding:10px;background:#f8fafc;color:#111827;"
                      >
                        Cargando bots...
                      </div>
                    
                      <textarea
                        id="privateBotsBroadcastMessage"
                        class="broadcast-textarea"
                        placeholder="Escribe aquí el mensaje privado para los bots seleccionados..."
                        style="margin-top:10px;"
                      ></textarea>

                      <div style="margin-top:10px;padding:10px;border:1px dashed #e5e7eb;border-radius:12px;background:#f8fafc;color:#111827;">
                        <div style="font-size:12px;font-weight:800;margin-bottom:8px;">
                          Audio opcional
                        </div>
                    
                        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
                          <button type="button" class="btn btn-warning" onclick="startPanelAudioRecording('private')">
                            Grabar audio
                          </button>
                    
                          <button type="button" class="btn btn-danger" onclick="stopPanelAudioRecording()" disabled id="privateAudioStopBtn">
                            Detener
                          </button>
                    
                          <button type="button" class="btn btn-light" onclick="clearPanelAudio('private')">
                            Quitar audio
                          </button>
                    
                          <span id="privateAudioStatus" style="font-size:12px;color:#64748b;font-weight:800;">
                            Sin audio
                          </span>
                        </div>
                    
                        <audio id="privateAudioPreview" controls style="display:none;width:100%;margin-top:8px;"></audio>
                      </div>
                    
                      <div
                        id="privateBotsBroadcastProgress"
                        style="display:none;margin-top:10px;padding:10px;border-radius:10px;background:#f8fafc;border:1px solid #e5e7eb;font-size:12px;color:#111827;font-weight:600;"
                      ></div>
                    
                      <div class="broadcast-actions">
                        <button class="btn btn-success" onclick="sendPrivateBotsBroadcast()">Enviar privado a seleccionados</button>
                        <button class="btn btn-light" onclick="document.getElementById('privateBotsBroadcastMessage').value=''">Limpiar</button>
                      </div>
                    </div>
                  </div>
                </div>
                
              </div>
            </div>
        
            <form class="box" method="get" action="/panel">
              <input type="hidden" name="token" value="docifymx2026">
              
              <div class="head">
                <strong>Filtros</strong>
                <span class="small">Aplica filtros para localizar información específica rápidamente.</span>
              </div>
              
              <div class="filters">
                <input type="hidden" name="view" value="{_esc(view)}">
                <input type="hidden" name="group_mode" value="{_esc(group_mode)}">
                <input type="hidden" name="date_from" value="{_esc(date_from)}">
                <input type="hidden" name="date_to" value="{_esc(date_to)}">
                
                <input name="group_jid" placeholder="Grupo cliente" value="{_esc(group_jid)}">
                <input name="provider_name" placeholder="Proveedor" value="{_esc(provider_name)}">
                <input name="status" placeholder="Estado" value="{_esc(status)}">
                <input name="act_type" placeholder="Tipo de acta" value="{_esc(act_type)}">
                
                <button type="submit" class="btn btn-primary">Filtrar</button>
              </div>
            </form>
        """
        
        html += """
        <div class="box">
          <div class="head collapsible-head open" onclick="toggleSection('promoCompartidaBody', this)">
            <div>
              <strong>Promoción compartida</strong>
              <span class="small">
                Permite asignar un paquete de actas a varios grupos para compartir el mismo saldo.
              </span>
            </div>
            <span class="collapse-icon">▼</span>
          </div>
          <div id="promoCompartidaBody" class="collapsible-body open">

            <div class="filters" style="margin-bottom:12px;">
              <input id="sharedPromoName" placeholder="Nombre de la promoción">
              <input id="sharedPromoClientKey" placeholder="Nombre de la bolsa compartida">
              <input id="sharedPromoTotalActas" type="number" placeholder="Total de actas del paquete">
              <input id="sharedPromoPricePerPiece" placeholder="Precio por acta">
            </div>
        
            <div class="box" style="padding:14px;margin-top:8px;background:#f8fafc;border:1px solid #e5e7eb;">
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;align-items:end;">
                
                <div>
                  <label style="display:block;font-size:13px;font-weight:600;margin-bottom:6px;color:#374151;">
                    Tipo de promoción
                  </label>
                  <select id="sharedPromoType" style="width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:10px;">
                    <option value="paid">Pagada</option>
                    <option value="credit">Crédito</option>
                  </select>
                </div>
            
                <div>
                  <label style="display:block;font-size:13px;font-weight:600;margin-bottom:6px;color:#374151;">
                    Abono
                  </label>
                  <input id="sharedPromoCreditAbono" type="number" min="0" placeholder="N/A" value="" disabled>
                </div>
            
                <div>
                  <label style="display:block;font-size:13px;font-weight:600;margin-bottom:6px;color:#374151;">
                    Debe
                  </label>
                  <input id="sharedPromoCreditDebe" type="number" min="0" placeholder="N/A" value="" disabled>
                </div>
            
              </div>
            
              <div class="helper" style="margin-top:12px;">
                Selecciona los grupos que usarán la misma bolsa compartida. Si un grupo consume actas, se descuentan del mismo saldo para todos.
              </div>
            
              <div style="margin-top:10px;font-size:13px;color:#6b7280;">
                Ejemplo: si 4 grupos comparten una bolsa de 1000 actas y uno consume 50,
                el saldo disponible será 950 para todos los grupos asociados.
              </div>
            </div>
        
            <div class="box" style="padding:14px; margin-top:8px; background:#f8fafc; border:1px solid #e5e7eb;">
              <div style="display:grid; grid-template-columns: 1.2fr auto auto auto; gap:10px; align-items:center;">
                <input
                  id="sharedPromoSearch"
                  placeholder="Buscar grupo por nombre..."
                  oninput="filterSharedPromoGroups()"
                >
        
                <label style="display:flex;align-items:center;gap:6px;font-size:14px;">
                  <input type="checkbox" id="filterNormalGroups" checked onchange="filterSharedPromoGroups()">
                  Normales
                </label>
        
                <label style="display:flex;align-items:center;gap:6px;font-size:14px;">
                  <input type="checkbox" id="filterTestGroups" onchange="filterSharedPromoGroups()">
                  Pruebas
                </label>
        
                <label style="display:flex;align-items:center;gap:6px;font-size:14px;">
                  <input type="checkbox" id="filterProviderGroups" onchange="filterSharedPromoGroups()">
                  Proveedores
                </label>
              </div>
        
              <div class="helper" style="margin-top:10px;">
                Selecciona los grupos que compartirán el mismo saldo. Por defecto se muestran solo grupos normales.
              </div>
            </div>
        
            <div
              id="sharedPromoGroups"
              style="max-height:360px;overflow:auto;border:1px solid #e5e7eb;padding:12px;border-radius:14px;background:#fff;margin-top:12px;"
            >
        """
        group_ids = set(GROUP_NAME_MAP.keys())
        group_ids.update(group_cache.keys())

        for gid in sorted(group_ids, key=lambda x: _group_name_cached(x, group_cache).lower()):
            group_name = _group_name_cached(gid, group_cache)
            upper_name = group_name.upper()
        
            is_test = (
                "PRUEBA" in upper_name
                or "PRUEBAS" in upper_name
                or "TEST" in upper_name
            )
        
            is_provider = (
                upper_name.startswith("PROV ")
                or "PROV " in upper_name
                or "PROVEEDOR" in upper_name
            )
        
            group_kind = "normal"
            badge_text = "Normal"
            badge_class = "badge-light"
        
            if is_test:
                group_kind = "test"
                badge_text = "Prueba"
                badge_class = "badge-warning"
            elif is_provider:
                group_kind = "provider"
                badge_text = "Proveedor"
                badge_class = "badge-danger"
        
            html += f'''
            <label
              class="shared-promo-item"
              data-name="{_esc(group_name).lower()}"
              data-kind="{group_kind}"
              style="display:flex;justify-content:space-between;align-items:center;gap:12px;padding:10px 12px;border:1px solid #eef2f7;border-radius:12px;margin-bottom:8px;background:#fff;"
            >
              <span style="display:flex;align-items:center;gap:10px;min-width:0;">
                <input type="checkbox" class="shared-promo-group" value="{gid}">
                <span style="display:flex;flex-direction:column;min-width:0;">
                  <span style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{_esc(group_name)}</span>
                  <span style="font-size:12px;color:#6b7280;">{gid}</span>
                </span>
              </span>
              <span class="badge {badge_class}">{badge_text}</span>
            </label>
            '''
        html += """
            </div>
        
            <div class="shared-promo-actions">
              <button class="btn btn-success" onclick="applySharedPromotion()">
                Aplicar promoción compartida
              </button>

              <button class="btn btn-primary" type="button" onclick="addGroupToSharedPromotion()">
                Agregar grupo a bolsa existente
              </button>
        
              <button class="btn btn-light" type="button" onclick="clearSharedPromotionSelection()">
                Limpiar selección
              </button>
            </div>
          </div>
        </div>
        """

        html += cleanup_panel_html

        html += metrics_html

        html += f"""
        <div class="box">
          <div class="head">
            <strong>📊 Estado de solicitudes</strong>
            <span class="small">Resumen del periodo seleccionado</span>
          </div>
        
          <div class="cards" style="padding:16px; grid-template-columns: repeat(5, minmax(0, 1fr));">
            <div class="card">
              <div class="label">Total</div>
              <div class="value">{summary["total"]}</div>
            </div>
        
            <div class="card">
              <div class="label">En cola</div>
              <div class="value">{summary["queued"]}</div>
            </div>
        
            <div class="card">
              <div class="label">Procesando</div>
              <div class="value">{summary["processing"]}</div>
            </div>
        
            <div class="card">
              <div class="label">Hecho</div>
              <div class="value">{summary["done"]}</div>
            </div>
        
            <div class="card">
              <div class="label">Error</div>
              <div class="value">{summary["error"]}</div>
            </div>
          </div>
        </div>
        """

        html += bot_status_html

        html += api_clients_panel_html

        html += """
        <div class="box">
          <div class="head">
            <strong>Control por bot</strong>
            <span class="small">Configura límite, consumo, bloqueo y recarga por instancia.</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Gestor</th>
                  <th>Precio</th>
                  <th>Bot</th>
                  <th class="right">Solicitudes</th>
                  <th class="right">Usadas</th>
                  <th class="right">Límite</th>
                  <th class="right">Disponibles</th>
                  <th>Estado</th>
                  <th>Nuevo límite</th>
                  <th>Recarga</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
        """

        commercial_data_by_instance = {}

        for item in by_instance:
            item_inst = (item["instance_name"] or "").strip()

            if not item_inst:
                continue

            commercial_data_by_instance[item_inst] = _get_bot_manager_data(
                db,
                item_inst,
            )

        for r in by_instance:
            inst = (r["instance_name"] or "").strip()
        
            commercial_data = commercial_data_by_instance.get(inst, {})
            manager_name = commercial_data.get("manager_name") or ""
            manager_price = commercial_data.get("manager_price") or ""
        
            bot_credit = _bot_credit_stats(db, inst)
            bot_used = bot_credit["used"]
            bot_limit = bot_credit["limit"]
            bot_available = bot_credit["available"] if bot_limit > 0 else "∞"
        
            bot_blocked = is_instance_blocked(inst)
        
            status_badge = (
                '<span class="badge badge-danger">BLOQUEADO</span>'
                if bot_blocked else
                '<span class="badge badge-success">ACTIVO</span>'
            )
        
            html += f"""
                <tr>        
                  <td>
                    <input
                      id="bot_manager_name_{_esc(inst)}"
                      type="text"
                      maxlength="100"
                      value="{_esc(manager_name)}"
                      placeholder="Nombre del gestor"
                      style="min-width:170px;width:100%;"
                    >
                  </td>
            
                  <td>
                    <div style="display:flex;gap:8px;align-items:center;min-width:180px;">
                      <input
                        id="bot_manager_price_{_esc(inst)}"
                        type="number"
                        min="0"
                        step="0.01"
                        value="{_esc(manager_price)}"
                        placeholder="Ej. 3.50"
                        style="width:100%;"
                      >
                      <button
                        class="btn btn-primary"
                        type="button"
                        onclick="saveBotManagerData('{_esc(inst)}')"
                      >
                        Guardar
                      </button>
                    </div>
                  </td>
                
                  <td><strong>{_esc(bot_labels_map.get(inst) or inst)}</strong></td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{bot_used}</td>
                  <td class="right">{bot_limit}</td>
                  <td class="right">{bot_available}</td>
                  <td>{status_badge}</td>
        
                  <td>
                    <div style="display:flex;gap:8px;align-items:center;min-width:180px;">
                      <input
                        id="bot_limit_{_esc(inst)}"
                        type="number"
                        min="0"
                        step="1"
                        value="{bot_limit}"
                        placeholder="Ej. 1000"
                        style="width:100%;"
                      >
                      <button class="btn btn-primary" onclick="saveBotLimit('{_esc(inst)}')">
                        Guardar
                      </button>
                    </div>
                  </td>
        
                  <td>
                    <div style="display:flex;gap:8px;align-items:center;min-width:180px;">
                      <input
                        id="bot_recharge_{_esc(inst)}"
                        type="number"
                        min="1"
                        step="1"
                        placeholder="Ej. 250"
                        style="width:100%;"
                      >
                      <button class="btn btn-success" onclick="rechargeBotLimit('{_esc(inst)}')">
                        Recargar
                      </button>
                    </div>
                  </td>
        
                  <td>
                    <div style="display:flex;flex-wrap:wrap;gap:8px;">
                      <button class="btn btn-light" onclick="resetBotUsage('{_esc(inst)}')">
                        Reset usadas
                      </button>
                      {
                        f'<button class="btn btn-success" onclick="unblockBot(\'{_esc(inst)}\')">Desbloquear</button>'
                        if bot_blocked else 
                        f'<button class="btn btn-danger" onclick="blockBot(\'{_esc(inst)}\')">Bloquear</button>'
                      }
                    </div>
                  </td>
                </tr>
            """
        
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """

        html += """
        <div class="box">
          <div class="head">
            <strong>Resumen por bot</strong>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Bot</th>
                  <th class="right">Total</th>
                  <th class="right">HECHO</th>
                  <th class="right">ERROR</th>
                </tr>
              </thead>
              <tbody>
        """
    
        if by_instance:
            for r in by_instance:
                html += f"""
                <tr>
                  <td>{_esc(bot_labels_map.get(r["instance_name"]) or r["instance_name"])}</td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="4">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """

        html += f"""
        <div class="box">
          <div class="head"><strong>Resumen por proveedor</strong></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Proveedor</th>
                  <th class="right">Total</th>
                  <th class="right">HECHO</th>
                  <th class="right">ERROR</th>
                </tr>
              </thead>
              <tbody>
        """
    
        if by_provider:
            for r in by_provider:
                html += f"""
                <tr>
                  <td>{_esc(_provider_label(r["provider_name"]))}</td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="4">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """

        audit_params = {
            "token": current_token,
            "view": view,
            "group_jid": group_jid or "",
            "provider_name": provider_name or "",
            "act_type": act_type or "",
            "date_from": date_from or "",
            "date_to": date_to or "",
        }

        audit_url = "/panel/auditoria-proveedores?" + urlencode(audit_params)

        html += f"""
        <div class="box">
          <div class="head">
            <div>
              <strong>Control contable por proveedor</strong>
              <div class="small">
                Cuadre global de actas recibidas/procesadas por todos los bots en el periodo: {_esc(period_label)}.
              </div>
            </div>

            <a class="btn btn-primary" href="{_esc(audit_url)}">
              📊 Ver auditoría completa
            </a>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr style="background:#fff200;">
                  <th>Proveedor</th>
                  <th class="right">Total con éxito</th>
                  <th class="right">Actas sin registro en sistema</th>
                  <th class="right">Actas erróneas / duplicadas</th>
                  <th class="right">Otros errores</th>
                  <th class="right">Pendientes</th>
                  <th class="right">Total de solicitudes</th>
                </tr>
              </thead>
              <tbody>
        """

        if provider_control_rows:
            for r in provider_control_rows:
                html += f"""
                <tr>
                  <td><strong>{_esc(_provider_label(r["provider_name"]))}</strong></td>
                  <td class="right">{r["total_exito"]}</td>
                  <td class="right">{r["sin_registro"]}</td>
                  <td class="right">{r["actas_erroneas"]}</td>
                  <td class="right">{r["otros_errores"]}</td>
                  <td class="right">{r["pendientes"]}</td>
                  <td class="right"><strong>{r["total_solicitudes"]}</strong></td>
                </tr>
                """

            html += f"""
                <tr style="background:#f1f5f9;font-weight:900;">
                  <td>TOTAL GENERAL</td>
                  <td class="right">{provider_control_totals["total_exito"]}</td>
                  <td class="right">{provider_control_totals["sin_registro"]}</td>
                  <td class="right">{provider_control_totals["actas_erroneas"]}</td>
                  <td class="right">{provider_control_totals["otros_errores"]}</td>
                  <td class="right">{provider_control_totals["pendientes"]}</td>
                  <td class="right">{provider_control_totals["total_solicitudes"]}</td>
                </tr>
            """
        else:
            html += '<tr><td colspan="7">Sin datos para este periodo.</td></tr>'

        html += """
              </tbody>
            </table>
          </div>

          <div class="small" style="margin-top:10px;color:#64748b;">
            Fórmula de cuadre:
            Total de solicitudes = Total con éxito + Sin registro + Actas erróneas/duplicadas + Otros errores + Pendientes.
          </div>
        </div>
        """

        html += """
        <div class="box">
          <div class="head"><strong>Resumen por tipo de acta</strong></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Tipo</th>
                  <th class="right">Total</th>
                  <th class="right">HECHO</th>
                  <th class="right">ERROR</th>
                </tr>
              </thead>
              <tbody>
        """
    
        if by_type:
            for r in by_type:
                html += f"""
                <tr>
                  <td>{_esc(r["act_type"])}</td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="4">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """

        html += f"""
        <div class="box">
          <div class="head">
            <strong>Vista de grupos</strong>
            <span class="small">Consulta los grupos cliente y cambia la vista según su actividad.</span>
          </div>
          <div class="group-mode-bar">
            <a class="group-mode-link {'group-mode-link-active' if group_mode == 'all' else ''}"
               href="/panel?token=docifymx2026&view={_esc(view)}&group_mode=all&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}&date_from={_esc(date_from)}&date_to={_esc(date_to)}">
              Ver todos los grupos
            </a>
            <a class="group-mode-link {'group-mode-link-active' if group_mode == 'active' else ''}"
               href="/panel?token=docifymx2026&view={_esc(view)}&group_mode=active&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}&date_from={_esc(date_from)}&date_to={_esc(date_to)}">
              Solo grupos con compras del periodo
            </a>
          </div>
        </div>
        """

        all_blocked = are_all_client_groups_blocked()

        toggle_all_btn = (
            '<button class="btn btn-success" onclick="toggleAllGroups()">Desbloquear todos los grupos</button>'
            if all_blocked
            else '<button class="btn btn-danger" onclick="toggleAllGroups()">Bloquear todos los grupos</button>'
        )
        
        html += f"""
        <div class="box">
          <div class="head">
            <strong>Control masivo de grupos</strong>
            <span class="small">Bloquea o desbloquea todos los grupos cliente con un solo clic.</span>
          </div>
          <div class="group-mode-bar">
            {toggle_all_btn}
          </div>
        </div>
        """

        html += """
        <div class="box">
          <div class="head">
            <strong>Agregar grupo manualmente</strong>
            <span class="small">Registra un grupo nuevo indicando su ID, nombre y categoría.</span>
          </div>
        
          <div class="filters" style="grid-template-columns: 1.2fr 1fr 220px 220px;">
            <div>
              <div class="small">Group JID</div>
              <input id="manualGroupJid" placeholder="1203634XXXXXXXXXX@g.us">
            </div>
        
            <div>
              <div class="small">Nombre del grupo</div>
              <input id="manualGroupName" placeholder="Nombre del grupo">
            </div>
        
            <div>
              <div class="small">Categoría</div>
              <select id="manualGroupCategory">
                <option value="papeleria_ciber">Papelería / Ciber</option>
                <option value="gestor">Gestor</option>
                <option value="otro" selected>Otro</option>
              </select>
            </div>
        
            <div style="display:flex;align-items:end;">
              <button type="button" class="btn btn-primary" style="width:100%;" onclick="addManualGroup()">
                Agregar grupo
              </button>
            </div>
          </div>
        </div>
        """

        html += """
        <div class="box">
          <div class="head collapsible-head open" onclick="toggleSection('grupoClienteBody', this)">
            <div>
              <strong>Resumen por grupo cliente</strong>
              <span class="small">
                Consulta el rendimiento y estado de las solicitudes por proveedor.
              </span>
            </div>
            <span class="collapse-icon">▼</span>
          </div>
          <div id="grupoClienteBody" class="collapsible-body open">
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Grupo</th>
                  <th class="right">Total</th>
                  <th class="right">HECHO</th>
                  <th class="right">ERROR</th>
                  <th>Promoción</th>
                  <th>Última actualización</th>
                  <th>Bloqueo</th>
                  <th>Evidencia</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
        """
    
        if by_group:
            for r in by_group:
                blocked = is_group_blocked(r["group_jid"])
                blocked_text = "BLOQUEADO" if blocked else "ACTIVO"
                
                block_btn = (
                    f'<button class="btn btn-success" onclick="toggleGroupBlock(\'{r["group_jid"]}\', \'unblock\')">Desbloquear</button>'
                    if blocked else 
                    f'<button class="btn btn-danger" onclick="toggleGroupBlock(\'{r["group_jid"]}\', \'block\')">Bloquear</button>'
                    f'<button class="btn btn-light" onclick="hideGroupFromPanel(\'{r["group_jid"]}\')">Ocultar</button>'
                )
                
                action_btn = f'''
                <div style="display:flex;align-items:center;gap:8px;">
                  {block_btn}
                </div>
                '''
        
                group_key = (r["group_jid"] or "").replace("@g.us", "").strip()
                promo_info = (
                    promo_map.get(r["group_jid"])
                    or promo_map.get(group_key)
                )
        
                if promo_info:
                    status = "Activa" if promo_info["available"] > 0 else "Agotada"
                    promo_badge_class = "badge-success" if promo_info["available"] > 0 else "badge-danger"
                
                    is_shared = bool((promo_info.get("shared_key") or "").strip()) and (promo_info.get("shared_count", 0) > 1)
                    shared_text = "Compartida" if is_shared else "Individual"
                    shared_badge_class = "badge-warning" if is_shared else "badge-light"

                    client_key = (promo_info.get("client_key") or "").strip()
                    client_line = (
                        f'<div class="small" style="margin-top:4px;color:#6b7280;">{_esc(client_key)}</div>'
                        if is_shared and client_key else ""
                    )
                
                    promo_cell = f"""
                    <span class="badge {promo_badge_class}">{status}</span>
                    <span class="badge {shared_badge_class}" style="margin-left:6px;">{shared_text}</span><br>
                    <b>{promo_info["used_actas"]} / {promo_info["total_actas"]}</b>
                    {client_line}
                    """
                else:
                    promo_cell = f"""
                    <a href="/panel/group-detail?group_jid={r['group_jid']}&view={view}&date_from={_esc(date_from)}&date_to={_esc(date_to)}"
                       class="btn btn-success"
                       style="color:white;display:inline-flex;align-items:center;justify-content:center;padding:6px 12px; font-size:13px; border-radius:16px; text-decoration:none;">
                       +Promoción
                    </a>
                    """
        
                html += f"""
                <tr>
                  <td>
                    <a href="/panel/group-detail?group_jid={r['group_jid']}&view={view}&date_from={_esc(date_from)}&date_to={_esc(date_to)}">
                      {_esc(r["group_name"])}
                    </a>
                  </td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                  <td>{promo_cell}</td>
                  <td>{_esc(_fmt_dt(r["last_update"]))}</td>
                  <td>{blocked_text}</td>
                  <td>
                    <a target="_blank"
                       href="/panel/audit/group?token={settings.ADMIN_PANEL_TOKEN}&view={view}&group_jid={r['group_jid']}&date_from={_esc(date_from)}&date_to={_esc(date_to)}"
                       class="btn btn-primary"
                       style="color:white;text-decoration:none;padding:6px 10px;font-size:12px;border-radius:10px;">
                       Ver evidencia
                    </a>
                  </td>
                  <td>{action_btn}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="8">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
          </div>
        </div>
        """

        
    
        html += """
        <div class="box">
          <div class="head collapsible-head open" onclick="toggleSection('recentRequestsWrap', this)">
            <div>
              <strong>Solicitudes recientes</strong>
              <span class="small">
                Consulta las solicitudes recientes y su avance de procesamiento.
              </span>
            </div>
            <span class="collapse-icon">▼</span>
          </div>
          <div id="recentRequestsWrap" class="collapsible-body open">
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Dato</th>
                  <th>Tipo</th>
                  <th>Estado</th>
                  <th>Grupo cliente</th>
                  <th>Bot</th>
                  <th>Proveedor</th>
                  <th>Creado</th>
                  <th>Actualizado</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
        """
    
        if latest:
            for r in latest:
                status_class = {
                    "QUEUED": "status-q",
                    "PROCESSING": "status-p",
                    "DONE": "status-d",
                    "ERROR": "status-e",
                }.get(r.status, "")
    
                html += f"""
                <tr>
                  <td>{r.id}</td>
                  <td class="mono">{_esc(r.curp)}</td>
                  <td>{_esc(r.act_type)}</td>
                  <td class="{status_class}">{_esc(r.status)}</td>
                  <td>{_esc(_group_name_cached(r.source_group_id, group_cache) if (r.instance_name or "docifybot8") == "docifybot8" else "OCULTO")}</td>
                  <td>{_esc(bot_labels_map.get(r.instance_name or "docifybot8") or (r.instance_name or "docifybot8"))}</td>
                  <td>{_esc(_provider_label(r.provider_name))}</td>
                  <td>{_esc(_fmt_dt(r.created_at))}</td>
                  <td>{_esc(_fmt_dt(r.updated_at))}</td>
                  <td class="small">{_esc(r.error_message)}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="10">Sin solicitudes en este periodo.</td></tr>'
    
        html += f"""
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    
      <script>
        const PANEL_STREAM_ENABLED = {json.dumps(PANEL_STREAM_ENABLED)};
        let broadcastRunning = false;

        function getPanelToken() {{
          return new URLSearchParams(window.location.search).get("token") || "";
        }}
        
        async function saveCleanupSettings() {{
          const enabled = !!document.getElementById("cleanupEnabled")?.checked;
          const maxAge = Number(document.getElementById("cleanupMaxAgeMinutes")?.value || 0);
          const panelToken = getPanelToken();
        
          if (!maxAge || maxAge < 1 || maxAge > 1440) {{
            alert("Ingresa minutos válidos entre 1 y 1440.");
            return;
          }}
        
          try {{
            const res = await fetch(`/panel/cleanup/settings?token=${{encodeURIComponent(panelToken)}}`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                enabled,
                max_age_minutes: maxAge
              }})
            }});
        
            const data = await res.json();
        
            if (!data.ok) {{
              alert(data.error || "No se pudo guardar configuración.");
              return;
            }}
        
            alert(`Cleanup actualizado: ${{data.enabled ? "ACTIVO" : "DESACTIVADO"}} · ${{data.max_age_minutes}} min`);
            location.reload();
        
          }} catch (e) {{
            alert("Error conectando con el servidor.");
          }}
        }}
        
        async function previewCleanupPurge() {{
          const minutes = Number(document.getElementById("cleanupPurgeMinutes")?.value || 0);
          const box = document.getElementById("cleanupStatusBox");
          const panelToken = getPanelToken();
        
          if (!minutes || minutes < 1 || minutes > 1440) {{
            alert("Ingresa minutos válidos entre 1 y 1440.");
            return;
          }}
        
          if (box) {{
            box.innerHTML = "Revisando atoradas...";
          }}
        
          try {{
            const res = await fetch(
              `/panel/cleanup/status?token=${{encodeURIComponent(panelToken)}}&older_than_minutes=${{encodeURIComponent(minutes)}}`
            );
        
            const data = await res.json();
        
            if (!data.ok) {{
              alert(data.error || "No se pudo revisar.");
              return;
            }}
        
            if (box) {{
              box.innerHTML = `
                Encontradas: <strong>${{data.stuck_count}}</strong>
                solicitudes EN COLA/PROCESANDO con más de
                <strong>${{data.check_minutes}}</strong> minutos.
              `;
            }}
        
          }} catch (e) {{
            alert("Error conectando con el servidor.");
          }}
        }}
        
        async function purgeCleanupStuck() {{
          const minutes = Number(document.getElementById("cleanupPurgeMinutes")?.value || 0);
          const panelToken = getPanelToken();
        
          if (!minutes || minutes < 1 || minutes > 1440) {{
            alert("Ingresa minutos válidos entre 1 y 1440.");
            return;
          }}
        
          const ok = confirm(
            `¿Seguro que deseas BORRAR de Redis/RQ y base de datos las solicitudes ` +
            `EN COLA/PROCESANDO con más de ${{minutes}} minutos?\n\n` +
            `Esta acción no se puede deshacer.`
          );
        
          if (!ok) return;
        
          try {{
            const res = await fetch(`/panel/cleanup/purge-stuck?token=${{encodeURIComponent(panelToken)}}`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                older_than_minutes: minutes
              }})
            }});
        
            const data = await res.json();
        
            if (!data.ok) {{
              alert(data.error || "No se pudo borrar.");
              return;
            }}
        
            alert(
              `Limpieza lista.\n\n` +
              `Encontradas: ${{data.found}}\n` +
              `Borradas DB: ${{data.deleted_db}}\n` +
              `Jobs RQ borrados: ${{data.rq?.deleted || 0}}`
            );
        
            location.reload();
        
          }} catch (e) {{
            alert("Error conectando con el servidor.");
          }}
        }}

        let panelAudioRecorder = null;
        let panelAudioChunks = [];
        let panelAudioTarget = null;
        
        let panelAudioBase64 = {{
          free: "",
          private: ""
        }};
        
        function setPanelAudioStatus(target, text) {{
          const id = target === "private" ? "privateAudioStatus" : "freeAudioStatus";
          const el = document.getElementById(id);
          if (el) el.textContent = text;
        }}
        
        function setPanelAudioStopEnabled(target, enabled) {{
          const id = target === "private" ? "privateAudioStopBtn" : "freeAudioStopBtn";
          const btn = document.getElementById(id);
          if (btn) btn.disabled = !enabled;
        }}
        
        function setPanelAudioPreview(target, blob) {{
          const id = target === "private" ? "privateAudioPreview" : "freeAudioPreview";
          const audio = document.getElementById(id);
          if (!audio) return;
        
          if (!blob) {{
            audio.style.display = "none";
            audio.removeAttribute("src");
            return;
          }}
        
          audio.src = URL.createObjectURL(blob);
          audio.style.display = "block";
        }}
        
        function blobToDataUrl(blob) {{
          return new Promise((resolve, reject) => {{
            const reader = new FileReader();
            reader.onloadend = () => resolve(reader.result || "");
            reader.onerror = reject;
            reader.readAsDataURL(blob);
          }});
        }}
        
        async function startPanelAudioRecording(target) {{
          if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {{
            alert("Tu navegador no permite grabar audio aquí.");
            return;
          }}
        
          if (panelAudioRecorder && panelAudioRecorder.state === "recording") {{
            alert("Ya hay una grabación activa.");
            return;
          }}
        
          panelAudioTarget = target;
          panelAudioChunks = [];
        
          try {{
            const stream = await navigator.mediaDevices.getUserMedia({{ audio: true }});
        
            let options = {{}};
        
            if (MediaRecorder.isTypeSupported("audio/webm;codecs=opus")) {{
              options = {{ mimeType: "audio/webm;codecs=opus" }};
            }} else if (MediaRecorder.isTypeSupported("audio/ogg;codecs=opus")) {{
              options = {{ mimeType: "audio/ogg;codecs=opus" }};
            }}
        
            panelAudioRecorder = new MediaRecorder(stream, options);
        
            panelAudioRecorder.ondataavailable = (event) => {{
              if (event.data && event.data.size > 0) {{
                panelAudioChunks.push(event.data);
              }}
            }};
        
            panelAudioRecorder.onstop = async () => {{
              try {{
                const blob = new Blob(panelAudioChunks, {{
                  type: panelAudioRecorder.mimeType || "audio/webm"
                }});
        
                const dataUrl = await blobToDataUrl(blob);
        
                panelAudioBase64[panelAudioTarget] = dataUrl;
        
                setPanelAudioPreview(panelAudioTarget, blob);
                setPanelAudioStatus(panelAudioTarget, "Audio listo para enviar");
              }} catch (e) {{
                console.error(e);
                setPanelAudioStatus(panelAudioTarget, "Error preparando audio");
              }}
        
              try {{
                stream.getTracks().forEach(track => track.stop());
              }} catch (e) {{}}
        
              setPanelAudioStopEnabled(panelAudioTarget, false);
              panelAudioRecorder = null;
            }};
        
            panelAudioRecorder.start();
            panelAudioBase64[target] = "";
            setPanelAudioPreview(target, null);
            setPanelAudioStatus(target, "Grabando...");
            setPanelAudioStopEnabled(target, true);
        
          }} catch (e) {{
            console.error(e);
            alert("No se pudo acceder al micrófono.");
            setPanelAudioStopEnabled(target, false);
          }}
        }}
        
        function stopPanelAudioRecording() {{
          if (panelAudioRecorder && panelAudioRecorder.state === "recording") {{
            panelAudioRecorder.stop();
          }}
        }}
        
        function clearPanelAudio(target) {{
          panelAudioBase64[target] = "";
          setPanelAudioPreview(target, null);
          setPanelAudioStatus(target, "Sin audio");
        }}
    
        async function toggleProvider(provider, action) {{
          const url = `/panel/provider/${{provider}}/${{action}}`;
    
          try {{
            const res = await fetch(url, {{ method: "POST" }});
            const data = await res.json();
    
            if (data.ok) {{
              location.reload();
            }} else {{
              alert("Error cambiando proveedor");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}
    
        async function refreshSID() {{
          const sid = prompt("Pega el nuevo PHPSESSID");
          if (!sid) return;
    
          try {{
            const res = await fetch("/panel/provider3/session", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                phpsessid: sid
              }})
            }});
    
            const data = await res.json();
    
            if (data.ok) {{
              alert("SID actualizada");
              location.reload();
            }} else {{
              alert(data.error || "Error actualizando SID");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function refreshHID() {{
          const hid = prompt("Pega el nuevo HID de LAZARO WEB 1");
          if (!hid) return;
        
          try {{
            const res = await fetch("/panel/provider4/hid", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                hid: hid
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert("HID de LAZARO WEB 1 actualizado");
              location.reload();
            }} else {{
              alert(data.error || "Error actualizando HID");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function refreshHID10() {{
          const hid = prompt("Pega el nuevo HID de LAZARO WEB 2", "D0cuExprRServ2");
          if (!hid) return;
        
          try {{
            const res = await fetch("/panel/provider10/hid", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                hid: hid
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert("HID de LAZARO WEB 2 actualizado");
              location.reload();
            }} else {{
              alert(data.error || "Error actualizando HID");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function refreshHID11() {{
          const hid = prompt("Pega el nuevo HID de LAZARO WEB 3", "D0cuExprRServ3");
          if (!hid) return;
        
          try {{
            const res = await fetch("/panel/provider11/hid", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                hid: hid
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert("HID de LAZARO WEB 3 actualizado");
              location.reload();
            }} else {{
              alert(data.error || "Error actualizando HID");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function saveProviderWeight(providerName) {{
          const input = document.getElementById("weight_" + providerName);
          const weight = input ? input.value : 0;
        
          const res = await fetch("/panel/provider-weight", {{
            method: "POST",
            headers: {{"Content-Type": "application/json"}},
            body: JSON.stringify({{
              provider_name: providerName,
              weight: weight
            }})
          }});
        
          const data = await res.json();
        
          if (!data.ok) {{
            alert("Error: " + (data.error || "No se pudo guardar"));
            return;
          }}
        
          alert("Peso actualizado: " + providerName + " = " + data.weight);
        }}

        async function saveBotManagerData(instanceName) {{
          const managerInput = document.getElementById(
            `bot_manager_name_${{instanceName}}`
          );

          const priceInput = document.getElementById(
            `bot_manager_price_${{instanceName}}`
          );

          const managerName = (managerInput?.value || "").trim();
          const managerPrice = (priceInput?.value || "").trim();

          if (managerPrice !== "") {{
            const numericPrice = Number(managerPrice);

            if (Number.isNaN(numericPrice) || numericPrice < 0) {{
              alert("Ingresa un precio válido mayor o igual a cero.");
              return;
            }}
          }}

          const panelToken = new URLSearchParams(
            window.location.search
          ).get("token") || "";

          try {{
            const response = await fetch(
              `/panel/instance/${{encodeURIComponent(instanceName)}}/manager-price?token=${{encodeURIComponent(panelToken)}}`,
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json"
                }},
                body: JSON.stringify({{
                  manager_name: managerName,
                  manager_price: managerPrice
                }})
              }}
            );

            const data = await response.json();

            if (!data.ok) {{
              alert(data.error || "No se pudieron guardar los datos del gestor.");
              return;
            }}

            alert(`Datos guardados para ${{instanceName}}.`);
            location.reload();

          }} catch (error) {{
            console.error("BOT_MANAGER_SAVE_ERROR", error);
            alert("Error de conexión al guardar los datos del gestor.");
          }}
        }}

        async function saveBotLimit(instanceName) {{
          const input = document.getElementById(`bot_limit_${{instanceName}}`);
          const value = Number((input?.value || "0").trim());
        
          if (Number.isNaN(value) || value < 0) {{
            alert("Ingresa un límite válido.");
            return;
          }}
        
          try {{
            const res = await fetch(`/panel/instance/${{encodeURIComponent(instanceName)}}/limit`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                limit: value
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(`Límite actualizado para ${{instanceName}}: ${{data.limit}}`);
              location.reload();
            }} else {{
              alert(data.error || "No se pudo guardar el límite.");
            }}
          }} catch (e) {{
            alert("Error de conexión al guardar el límite.");
          }}
        }}

        async function hideGroupFromPanel(groupJid) {{
          const ok = confirm("¿Quitar este grupo visualmente del panel?");
          if (!ok) return;
        
          const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/hide`, {{
            method: "POST"
          }});
        
          const data = await res.json();
          if (data.ok) {{
            location.reload();
          }} else {{
            alert(data.error || "No se pudo quitar el grupo.");
          }}
        }}

        function normalizeQrImage(qr) {{
          const value = String(qr || "").trim();
        
          if (!value) return "";
        
          if (value.startsWith("data:image")) {{
            return value;
          }}
        
          // Base64 normal de PNG/JPG/WebP.
          // Antes solo aceptabas /9j, pero muchos QR PNG empiezan con iVBORw0KGgo.
          const looksBase64 =
            value.length > 100 &&
            /^[A-Za-z0-9+/=]+$/.test(value);
        
          if (looksBase64) {{
            return `data:image/png;base64,${{value}}`;
          }}
        
          return "";
        }}
        
        async function getBotQr(instanceName) {{
          const box = document.getElementById("botQrBox");
          if (!box) return;
        
          box.innerHTML = "<strong>Generando QR...</strong>";
        
          try {{
            const panelToken = new URLSearchParams(window.location.search).get("token") || "";

            const res = await fetch(
              `/panel/instance/${{encodeURIComponent(instanceName)}}/qr?token=${{encodeURIComponent(panelToken)}}`
            );
            
            const data = await res.json();
        
            if (!data.ok) {{
              box.innerHTML = `
                <div style="color:red;font-weight:800;margin-bottom:10px;">
                  Error: ${{data.error || "No se pudo generar QR"}}
                </div>
                <pre style="white-space:pre-wrap;background:#111827;color:white;padding:14px;border-radius:12px;">${{JSON.stringify(data, null, 2)}}</pre>
              `;
              return;
            }}
        
            const payload = data.data || {{}};
        
            const qr =
              data.qr_image ||
              data.qr ||
              payload.base64 ||
              payload.qrcode?.base64 ||
              payload.qrcode?.code ||
              payload.qrcode ||
              payload.qr ||
              payload.qrCode ||
              payload.code ||
              payload.pairingCode ||
              payload.instance?.qrcode ||
              payload.instance?.qr ||
              payload.instance?.base64 ||
              "";
        
            const imgSrc = normalizeQrImage(qr);
        
            if (imgSrc) {{
              box.innerHTML = `
                <div style="padding:14px;border:1px solid #e5e7eb;border-radius:14px;background:white;">
                  <strong>QR para ${{instanceName}}</strong><br><br>
                  <img src="${{imgSrc}}" style="max-width:280px;width:100%;border-radius:12px;">
                  <div style="margin-top:10px;color:#64748b;font-size:13px;">
                    Escanéalo desde WhatsApp &gt; Dispositivos vinculados.
                  </div>
                </div>
              `;
              return;
            }}
        
            if (qr) {{
              box.innerHTML = `
                <div style="padding:14px;border:1px solid #e5e7eb;border-radius:14px;background:white;">
                  <strong>Código recibido para ${{instanceName}}</strong>
                  <pre style="white-space:pre-wrap;background:#111827;color:white;padding:14px;border-radius:12px;">${{qr}}</pre>
                </div>
              `;
              return;
            }}
        
            box.innerHTML = `
              <div style="color:#b45309;font-weight:800;margin-bottom:10px;">
                Evolution respondió OK, pero no mandó QR.
              </div>
              <pre style="white-space:pre-wrap;background:#111827;color:white;padding:14px;border-radius:12px;">${{JSON.stringify(data, null, 2)}}</pre>
            `;
        
          }} catch (e) {{
            box.innerHTML = `<div style="color:red;font-weight:800;">Error de conexión</div>`;
          }}
        }}

        async function disconnectBot(i){{
          if(!confirm("Desconectar?")) return;
          await fetch(`/panel/bots/${{i}}/disconnect?token=docifymx2026`,{{method:"POST"}});
          location.reload();
        }}
        
        async function hideBot(i){{
          if(!confirm("Ocultar?")) return;
          await fetch(`/panel/bots/${{i}}/hide?token=docifymx2026`,{{method:"POST"}});
          location.reload();
        }}
        
        async function createBot(){{
          const label=document.getElementById("newBotLabel").value;
          const instance=document.getElementById("newBotInstance").value;
        
          const r=await fetch(`/panel/bots/create?token=docifymx2026`,{{
            method:"POST",
            headers:{{"Content-Type":"application/json"}},
            body:JSON.stringify({{label,instance_name:instance}})
          }});
        
          const d=await r.json();
        
          if(!d.ok){{
            alert(d.error||"error");
            return;
          }}
        
          alert("Token: "+d.token);
          location.reload();
        }}

        async function updateProvider7Credentials() {{
          const access_token = prompt("PROVIDER7_ACCESS_TOKEN:");
          if (access_token === null) return;
        
          const jsessionid = prompt("PROVIDER7_JSESSIONID:");
          if (jsessionid === null) return;
        
          const oficialia = prompt("PROVIDER7_OFICIALIA:");
          if (oficialia === null) return;
        
          const rfc_usuario = prompt("PROVIDER7_RFC_USUARIO:");
          if (rfc_usuario === null) return;
        
          try {{
            const res = await fetch("/panel/provider7/update-credentials", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                access_token,
                jsessionid,
                oficialia,
                rfc_usuario
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert("Credenciales de Provider7 actualizadas");
              location.reload();
            }} else {{
              alert(data.error || "No se pudieron actualizar las credenciales");
            }}
          }} catch (e) {{
            alert("Error de conexión al actualizar Provider7");
          }}
        }}
        
        async function rechargeBotLimit(instanceName) {{
          const input = document.getElementById(`bot_recharge_${{instanceName}}`);
          const value = Number((input?.value || "").trim());
        
          if (Number.isNaN(value) || value <= 0) {{
            alert("Ingresa una recarga válida mayor a 0.");
            return;
          }}
        
          try {{
            const res = await fetch(`/panel/instance/${{encodeURIComponent(instanceName)}}/recharge`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                amount: value
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(`Recarga aplicada a ${{instanceName}}. Nuevo límite: ${{data.limit}}`);
              location.reload();
            }} else {{
              alert(data.error || "No se pudo recargar el bot.");
            }}
          }} catch (e) {{
            alert("Error de conexión al recargar el bot.");
          }}
        }}
        
        async function resetBotUsage(instanceName) {{
          const ok = confirm(`¿Seguro que deseas resetear las usadas de ${{instanceName}}?`);
          if (!ok) return;
        
          try {{
            const res = await fetch(`/panel/instance/${{encodeURIComponent(instanceName)}}/reset-usage`, {{
              method: "POST"
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(`Usadas reseteadas para ${{instanceName}}.`);
              location.reload();
            }} else {{
              alert(data.error || "No se pudo resetear el consumo.");
            }}
          }} catch (e) {{
            alert("Error de conexión al resetear el consumo.");
          }}
        }}
        
        async function blockBot(instanceName) {{
          const ok = confirm(`¿Bloquear ${{instanceName}} para nuevas solicitudes?`);
          if (!ok) return;
        
          try {{
            const res = await fetch(`/panel/instance/${{encodeURIComponent(instanceName)}}/block`, {{
              method: "POST"
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(`${{instanceName}} bloqueado.`);
              location.reload();
            }} else {{
              alert(data.error || "No se pudo bloquear el bot.");
            }}
          }} catch (e) {{
            alert("Error de conexión al bloquear el bot.");
          }}
        }}
        
        async function unblockBot(instanceName) {{
          try {{
            const res = await fetch(`/panel/instance/${{encodeURIComponent(instanceName)}}/unblock`, {{
              method: "POST"
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(`${{instanceName}} desbloqueado.`);
              location.reload();
            }} else {{
              alert(data.error || "No se pudo desbloquear el bot.");
            }}
          }} catch (e) {{
            alert("Error de conexión al desbloquear el bot.");
          }}
        }}
        
        async function addGroupToSharedPromotion() {{
          const selected = Array.from(document.querySelectorAll(".shared-promo-group:checked"))
            .map(el => el.value);
        
          if (selected.length !== 1) {{
            alert("Selecciona solo un grupo para agregarlo a una bolsa existente");
            return;
          }}
        
          const shared_key = prompt("Ingresa la clave de la bolsa compartida existente:");
          if (!shared_key) return;
        
          try {{
            const res = await fetch("/panel/promotions/add-group", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                group_jid: selected[0],
                shared_key: shared_key
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Grupo agregado correctamente");
              location.reload();
            }} else {{
              alert(data.error || "No se pudo agregar el grupo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function addManualGroup() {{
          const group_jid = (document.getElementById("manualGroupJid")?.value || "").trim();
          const custom_name = (document.getElementById("manualGroupName")?.value || "").trim();
          const category = (document.getElementById("manualGroupCategory")?.value || "otro").trim();
        
          if (!group_jid) {{
            alert("Ingresa el Group JID");
            return;
          }}
        
          if (!group_jid.endsWith("@g.us")) {{
            alert("El Group JID debe terminar en @g.us");
            return;
          }}
        
          try {{
            const res = await fetch("/panel/groups/manual-add", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                group_jid,
                custom_name,
                category
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Grupo agregado");
              location.reload();
            }} else {{
              alert(data.error || "No se pudo agregar el grupo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function setSharedGroupLimit(groupJid) {{
          const value = prompt("Ingresa el límite individual de actas para este grupo dentro de la bolsa compartida:");
          if (value === null) return;
        
          try {{
            const res = await fetch("/panel/promotions/set-group-limit", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                group_jid: groupJid,
                limit_actas: Number(value || 0)
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Límite actualizado");
              location.reload();
            }} else {{
              alert(data.error || "No se pudo actualizar el límite");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function sendBroadcast(type) {{
          const ok = confirm("¿Seguro que deseas enviar este mensaje masivamente?");
          if (!ok) return;
        
          if (broadcastRunning) return;
          broadcastRunning = true;
        
          const category = document.getElementById("broadcastCategory")?.value || "all";
        
          try {{
            const res = await fetch(`/panel/broadcast/${{type}}`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                category: category
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Envío iniciado");
            }} else {{
              alert(data.error || "Error en envío masivo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        
          broadcastRunning = false;
        }}
    
        async function sendFreeBroadcast() {{
          const textarea = document.getElementById("broadcastMessage");
          const message = textarea.value.trim();
          const audioBase64 = panelAudioBase64.free || "";
          const category = document.getElementById("broadcastCategory")?.value || "all";
        
          if (!message && !audioBase64) {{
            alert("Escribe un mensaje o graba un audio");
            return;
          }}
        
          const ok = confirm("¿Seguro que deseas enviar este mensaje masivamente?");
          if (!ok) return;
        
          if (broadcastRunning) return;
          broadcastRunning = true;
        
          try {{
            const res = await fetch("/panel/broadcast/free", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                message: message,
                audio_base64: audioBase64,
                category: category
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Envío iniciado");
              textarea.value = "";
              clearPanelAudio("free");
            }} else {{
              alert(data.error || "Error en envío masivo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        
          broadcastRunning = false;
        }}

        let privateBotsProgressTimer = null;

        function getPrivateBotChecks() {{
          return Array.from(document.querySelectorAll(".privateBotCheck"));
        }}
        
        function syncPrivateBotSelectCounter() {{
          const counter = document.getElementById("privateBotSelectedCount");
          if (!counter) return;
        
          const checks = getPrivateBotChecks();
          const enabled = checks.filter(x => !x.disabled);
          const selected = enabled.filter(x => x.checked);
        
          counter.textContent = `${{selected.length}} de ${{enabled.length}} configurados seleccionados`;
        }}
        
        function selectAllPrivateBots(checked) {{
          const checks = getPrivateBotChecks();
        
          checks.forEach(chk => {{
            if (!chk.disabled) {{
              chk.checked = checked;
            }}
          }});
        
          syncPrivateBotSelectCounter();
        }}

        async function loadPrivateBotTargets() {{
          const box = document.getElementById("privateBotTargets");
          if (!box) return;
        
          try {{
            const res = await fetch("/panel/broadcast/private-bots/targets?token=docifymx2026");
            const data = await res.json();
        
            if (!data.ok) {{
              box.innerHTML = `<div style="color:#b91c1c;">${{data.error || "No se pudieron cargar los bots"}}</div>`;
              return;
            }}
        
            const bots = data.bots || [];
        
            if (!bots.length) {{
              box.innerHTML = `<div style="color:#64748b;">No hay bots internos extra registrados.</div>`;
              return;
            }}
        
            box.innerHTML = bots.map(bot => {{
              const disabled = bot.configured ? "" : "disabled";
              const checked = bot.configured ? "" : "";
              const status = bot.configured
                ? `<span style="color:#15803d;font-weight:700;">Configurado</span>`
                : `<span style="color:#b91c1c;font-weight:700;">Sin número privado</span>`;
        
              return `
                <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding:8px;border-bottom:1px solid #e5e7eb;">
                  <label style="display:flex;align-items:center;gap:8px;flex:1;cursor:pointer;">
                    <input
                      type="checkbox"
                      class="privateBotCheck"
                      value="${{bot.instance_name}}"
                      onchange="syncPrivateBotSelectCounter()"
                      ${{disabled}}
                      ${{checked}}
                    >
                    <span>
                      <strong>${{bot.label || bot.instance_name}}</strong><br>
                      <span style="font-size:11px;color:#64748b;">${{bot.instance_name}}</span><br>
                      <span style="font-size:11px;color:#64748b;">${{bot.jid || "Sin JID"}}</span>
                    </span>
                  </label>
        
                  <div style="text-align:right;">
                    <div style="font-size:11px;">${{status}}</div>
                    <button
                      class="btn btn-light"
                      style="font-size:11px;padding:5px 8px;margin-top:4px;"
                      onclick="setPrivateBotTarget('${{bot.instance_name}}')"
                    >
                      Configurar
                    </button>
                  </div>
                </div>
              `;
            }}).join("");

            syncPrivateBotSelectCounter();
        
          }} catch (e) {{
            box.innerHTML = `<div style="color:#b91c1c;">Error cargando bots internos</div>`;
          }}
        }}
        
        async function setPrivateBotTarget(instanceName) {{
          const value = prompt(
            "Ingresa el número privado/JID que recibirá avisos para " + instanceName + "\\n\\nEjemplo: 8991234567, 528991234567 o 528991234567@s.whatsapp.net"
          );
        
          if (value === null) return;
        
          const clean = value.trim();
        
          if (!clean) {{
            alert("Número vacío");
            return;
          }}
        
          try {{
            const res = await fetch(`/panel/bots/${{encodeURIComponent(instanceName)}}/private-target?token=docifymx2026`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                jid: clean
              }})
            }});
        
            const data = await res.json();
        
            if (!data.ok) {{
              alert(data.error || "No se pudo guardar el número privado");
              return;
            }}
        
            alert("Número privado guardado: " + data.jid);
            loadPrivateBotTargets();
        
          }} catch (e) {{
            alert("Error de conexión al guardar número privado");
          }}
        }}
        
        async function sendPrivateBotsBroadcast() {{
          const textarea = document.getElementById("privateBotsBroadcastMessage");
          const message = textarea.value.trim();
          const audioBase64 = panelAudioBase64.private || "";
        
          if (!message && !audioBase64) {{
            alert("Escribe un mensaje privado o graba un audio.");
            return;
          }}
        
          const selected = Array.from(document.querySelectorAll(".privateBotCheck:checked"))
            .map(x => x.value)
            .filter(Boolean);
        
          if (!selected.length) {{
            alert("Selecciona al menos un bot interno.");
            return;
          }}
        
          const ok = confirm(`¿Enviar este mensaje privado a ${{selected.length}} bot(s) interno(s)?`);
          if (!ok) return;
        
          try {{
            const res = await fetch("/panel/broadcast/private-bots?token=docifymx2026", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                message: message,
                audio_base64: audioBase64,
                selected_instances: selected
              }})
            }});
        
            const data = await res.json();
        
            if (!data.ok) {{
              alert(data.error || "No se pudo enviar el mensaje privado");
              return;
            }}
        
            alert(data.message || "Mensaje privado en cola");
            textarea.value = "";
            clearPanelAudio("private");
            startPrivateBotsBroadcastProgress(data.job_id);
        
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}
        
        function startPrivateBotsBroadcastProgress(jobId) {{
          const box = document.getElementById("privateBotsBroadcastProgress");
        
          if (box) {{
            box.style.display = "block";
            box.innerHTML = "Enviando privados...";
          }}
        
          if (privateBotsProgressTimer) {{
            clearInterval(privateBotsProgressTimer);
          }}
        
          privateBotsProgressTimer = setInterval(async () => {{
            try {{
              const res = await fetch(`/panel/broadcast/private-bots/progress/${{jobId}}?token=docifymx2026`);
              const data = await res.json();
        
              if (!box) return;
        
              box.innerHTML = `
                Estado: <strong>${{data.status || "pending"}}</strong><br>
                Enviados: <strong>${{data.sent || 0}}</strong> /
                Total: <strong>${{data.total || 0}}</strong><br>
                Errores: <strong>${{data.errors || 0}}</strong> |
                Saltados: <strong>${{data.skipped || 0}}</strong><br>
                Actual: ${{data.current || ""}}
              `;
        
              if (data.status === "done" || data.status === "error") {{
                clearInterval(privateBotsProgressTimer);
                privateBotsProgressTimer = null;
              }}
        
            }} catch (e) {{
              if (box) {{
                box.innerHTML = "No se pudo consultar el progreso.";
              }}
              clearInterval(privateBotsProgressTimer);
              privateBotsProgressTimer = null;
            }}
          }}, 2000);
        }}
        
        document.addEventListener("DOMContentLoaded", () => {{
          loadPrivateBotTargets();
        }});
    
        function clearBroadcast() {{
          document.getElementById("broadcastMessage").value = "";
        }}

        async function toggleGroupBlock(groupJid, action) {{
          const msg = action === "block"
            ? "¿Bloquear este grupo? El bot dejará de responder silenciosamente."
            : "¿Desbloquear este grupo?";
        
          const ok = confirm(msg);
          if (!ok) return;
        
          try {{
            const res = await fetch(`/panel/group/${{encodeURIComponent(groupJid)}}/${{action}}`, {{
              method: "POST"
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              location.reload();
            }} else {{
              alert(data.error || "Error cambiando estado del grupo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function toggleAllGroups() {{
          const ok = confirm("¿Seguro que deseas cambiar el estado de todos los grupos cliente?");
          if (!ok) return;
        
          try {{
            const res = await fetch("/panel/groups/toggle-all", {{
              method: "POST"
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Estado actualizado");
              location.reload();
            }} else {{
              alert(data.error || "Error actualizando grupos");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        async function applySharedPromotion() {{
          const selected = Array.from(document.querySelectorAll(".shared-promo-group:checked"))
            .map(el => el.value);
        
          const promo_name = document.getElementById("sharedPromoName").value || "";
          const client_key = document.getElementById("sharedPromoClientKey").value || "";
          const shared_key = client_key.trim().toUpperCase();
          const total_actas = Number(document.getElementById("sharedPromoTotalActas").value || 0);
          const price_per_piece = document.getElementById("sharedPromoPricePerPiece").value || "";
        
          const promo_type = document.getElementById("sharedPromoType").value || "paid";
          const is_credit = promo_type === "credit";
        
          let credit_abono_raw = document.getElementById("sharedPromoCreditAbono").value || "";
          let credit_debe_raw = document.getElementById("sharedPromoCreditDebe").value || "";
        
          if (!is_credit) {{
            credit_abono_raw = "0";
            credit_debe_raw = "0";
          }} else {{
            if (credit_abono_raw === "") credit_abono_raw = "0";
            if (credit_debe_raw === "") credit_debe_raw = "0";
          }}
        
          const credit_abono = Number(credit_abono_raw);
          const credit_debe = Number(credit_debe_raw);
        
          if (!selected.length) {{
            alert("Selecciona al menos un grupo");
            return;
          }}
        
          if (!total_actas || total_actas <= 0) {{
            alert("Ingresa un total de actas válido");
            return;
          }}
        
          if (!shared_key) {{
            alert("Ingresa una bolsa compartida");
            return;
          }}
        
          try {{
            const res = await fetch("/panel/promotions/apply", {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{
                selected_group_jids: selected,
                promo_name,
                client_key,
                shared_key,
                total_actas,
                price_per_piece,
                is_credit,
                credit_abono,
                credit_debe
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert("Promoción compartida aplicada correctamente");
              location.reload();
            }} else {{
              alert(data.error || "No se pudo aplicar la promoción");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        }}

        function toggleSharedPromoCreditFields() {{
          const promoType = document.getElementById("sharedPromoType");
          const isCredit = promoType && promoType.value === "credit";
        
          const abono = document.getElementById("sharedPromoCreditAbono");
          const debe = document.getElementById("sharedPromoCreditDebe");
        
          if (abono) {{
            if (isCredit) {{
              abono.disabled = false;
              if (!abono.value) abono.value = 0;
            }} else {{
              abono.disabled = true;
              abono.value = "";
            }}
          }}
        
          if (debe) {{
            if (isCredit) {{
              debe.disabled = false;
              if (!debe.value) debe.value = 0;
            }} else {{
              debe.disabled = true;
              debe.value = "";
            }}
          }}
        }}
        
        document.addEventListener("DOMContentLoaded", () => {{
          const promoType = document.getElementById("sharedPromoType");
          if (promoType) {{
            promoType.addEventListener("change", toggleSharedPromoCreditFields);
            toggleSharedPromoCreditFields();
          }}
        
          filterSharedPromoGroups();
          if (PANEL_STREAM_ENABLED && !document.hidden) {{
            startRecentRequestsStream();
          }}
        
          const sections = [
            "grupoClienteBody",
            "promoCompartidaBody",
            "recentRequestsWrap"
          ];
        
          sections.forEach(id => {{
            const body = document.getElementById(id);
            const head = body?.previousElementSibling;
        
            if (!body || !head) return;
        
            const state = localStorage.getItem(id);
        
            if (state === "closed") {{
              body.classList.remove("open");
              body.classList.add("closed");
              head.classList.add("closed");
            }}
          }});
        }});

        function filterSharedPromoGroups() {{
          const search = (document.getElementById("sharedPromoSearch")?.value || "").trim().toLowerCase();
          const showNormal = document.getElementById("filterNormalGroups")?.checked;
          const showTest = document.getElementById("filterTestGroups")?.checked;
          const showProvider = document.getElementById("filterProviderGroups")?.checked;
        
          const items = document.querySelectorAll(".shared-promo-item");
        
          items.forEach(item => {{
            const name = item.dataset.name || "";
            const kind = item.dataset.kind || "normal";
        
            const matchesSearch = !search || name.includes(search);
        
            let matchesKind = false;
            if (kind === "normal" && showNormal) matchesKind = true;
            if (kind === "test" && showTest) matchesKind = true;
            if (kind === "provider" && showProvider) matchesKind = true;
        
            item.style.display = (matchesSearch && matchesKind) ? "flex" : "none";
          }});
        }}
        
        function clearSharedPromotionSelection() {{
          document.querySelectorAll(".shared-promo-group").forEach(el => {{
            el.checked = false;
          }});
        
          const searchInput = document.getElementById("sharedPromoSearch");
          if (searchInput) searchInput.value = "";
        
          const normal = document.getElementById("filterNormalGroups");
          const test = document.getElementById("filterTestGroups");
          const provider = document.getElementById("filterProviderGroups");
        
          if (normal) normal.checked = true;
          if (test) test.checked = false;
          if (provider) provider.checked = false;
        
          filterSharedPromoGroups();
        }}

        function toggleSection(bodyId, headEl) {{
          const body = document.getElementById(bodyId);
          if (!body) return;
        
          const isClosed = body.classList.contains("closed");
        
          if (isClosed) {{
            body.classList.remove("closed");
            body.classList.add("open");
            headEl.classList.remove("closed");    
            localStorage.setItem(bodyId, "open");
          }} else {{
            body.classList.remove("open");
            body.classList.add("closed");
            headEl.classList.add("closed");
            localStorage.setItem(bodyId, "closed");
          }}
        }}

        async function refreshRecentRequests() {{
          const wrap = document.getElementById("recentRequestsWrap");
          if (!wrap) return;
        
          const params = new URLSearchParams({{
            view: document.querySelector('input[name="view"]')?.value || "day",
            group_jid: document.querySelector('input[name="group_jid"]')?.value || "",
            provider_name: document.querySelector('input[name="provider_name"]')?.value || "",
            status: document.querySelector('input[name="status"]')?.value || "",
            act_type: document.querySelector('input[name="act_type"]')?.value || "",
            date_from: document.querySelector('input[name="date_from"]')?.value || "",
            date_to: document.querySelector('input[name="date_to"]')?.value || "",
          }});
        
          try {{
            const res = await fetch(`/panel/recent-requests?${{params.toString()}}`);
            if (!res.ok) throw new Error("No se pudo actualizar solicitudes recientes");
        
            const html = await res.text();
            wrap.innerHTML = html;
          }} catch (e) {{
            console.error("RECENT_REQUESTS_REFRESH_ERROR =", e);
          }}
        }}
        
        let recentRequestsTimer = null;

        function startRecentRequestsStream() {{
          /*
            Antes: EventSource permanente a /panel/recent-requests/stream.
            Ahora: polling ligero cada 10 segundos.
            Mantiene historial reciente actualizado sin dejar conexiones vivas ocupando el panel.
          */

          if (recentRequestsTimer) {{
            clearInterval(recentRequestsTimer);
          }}

          const runRefresh = async () => {{
            if (document.hidden) return;
            await refreshRecentRequests();
          }};

          recentRequestsTimer = setInterval(runRefresh, 10000);

          document.addEventListener("visibilitychange", () => {{
            if (!document.hidden) {{
              runRefresh();
            }}
          }});
        }}

        {api_clients_panel_js}

      </script>
    </body>
    </html>
    """
        try:
            redis_conn.setex(cache_key, PANEL_HTML_TTL, html)
        except Exception:
            pass
            
        return HTMLResponse(
            content=html,
            headers={
                "Cache-Control": "no-store, max-age=0, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
        
    except Exception as e:
        print("panel_actas error:", repr(e), flush=True)
        return HTMLResponse(
            content=f"<pre>Error en /panel: {str(e)}</pre>",
            status_code=500,
        )


@app.get("/panel/auditoria-proveedores", response_class=HTMLResponse)
def panel_auditoria_proveedores(
    request: Request,
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    act_type: str = "",
    date_from: str = "",
    date_to: str = "",
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return HTMLResponse("No autorizado", status_code=403)

    try:
        time_min, time_max, view = _panel_period_bounds(view, date_from, date_to)
        period_label = _panel_period_label(view, time_min, time_max)
        current_token = (request.query_params.get("token") or "").strip()

        data = _provider_accounting_data(
            db,
            time_min=time_min,
            time_max=time_max,
            group_jid=group_jid,
            provider_name=provider_name,
            act_type=act_type,
        )

        provider_control_rows = data["provider_control_rows"]
        provider_control_totals = data["provider_control_totals"]
        error_detail_rows = data["error_detail_rows"]
        provider_bot_rows = data["provider_bot_rows"]
        pending_rows = data["pending_rows"]

        panel_params = {
            "token": current_token,
            "view": view,
            "group_jid": group_jid or "",
            "provider_name": provider_name or "",
            "act_type": act_type or "",
            "date_from": date_from or "",
            "date_to": date_to or "",
        }

        panel_url = "/panel?" + urlencode(panel_params)

        def _audit_link(new_view: str):
            params = dict(panel_params)
            params["view"] = new_view
            return "/panel/auditoria-proveedores?" + urlencode(params)

        html = f"""
        <html>
        <head>
          <meta charset="utf-8">
          <title>Auditoría de proveedores</title>
          <style>
            body {{
              font-family: Arial, sans-serif;
              background:#f3f4f6;
              margin:0;
              padding:22px;
              color:#0f172a;
            }}
            .wrap {{
              max-width:1500px;
              margin:0 auto;
            }}
            .box {{
              background:white;
              border-radius:16px;
              padding:16px;
              margin-bottom:16px;
              box-shadow:0 8px 22px rgba(15,23,42,.08);
            }}
            .head {{
              display:flex;
              align-items:center;
              justify-content:space-between;
              gap:12px;
              flex-wrap:wrap;
            }}
            h2 {{
              margin:0;
            }}
            .small {{
              font-size:12px;
              color:#64748b;
              line-height:1.35;
            }}
            .btn {{
              display:inline-block;
              padding:8px 12px;
              border-radius:10px;
              text-decoration:none;
              background:#2563eb;
              color:white;
              font-size:13px;
              font-weight:700;
              margin:3px;
              border:0;
              cursor:pointer;
            }}
            .btn-secondary {{
              background:#64748b;
            }}
            .table-wrap {{
              overflow-x:auto;
              margin-top:12px;
            }}
            table {{
              width:100%;
              border-collapse:collapse;
              background:white;
            }}
            th, td {{
              padding:10px 12px;
              border-bottom:1px solid #e5e7eb;
              font-size:13px;
              vertical-align:top;
            }}
            th {{
              text-align:left;
              background:#1f2937;
              color:white;
              white-space:nowrap;
            }}
            .right {{
              text-align:right;
            }}
            .mono {{
              font-family:Consolas, monospace;
              font-size:12px;
            }}
            details summary {{
              cursor:pointer;
              font-weight:800;
              color:#1d4ed8;
            }}
            details table {{
              margin-top:6px;
              background:white;
            }}
            .total-row {{
              background:#f1f5f9;
              font-weight:900;
            }}
            .badge {{
              display:inline-block;
              padding:4px 8px;
              border-radius:999px;
              font-size:12px;
              font-weight:800;
            }}
            .status-d {{ color:#15803d;font-weight:bold; }}
            .status-e {{ color:#b91c1c;font-weight:bold; }}
            .status-q {{ color:#92400e;font-weight:bold; }}
            .status-p {{ color:#1d4ed8;font-weight:bold; }}
            .weekly-row {{
              background:#dbeafe;
              font-weight:800;
            }}
            .weekly-row td {{
              border-top:2px solid #93c5fd;
            }}
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="box">
              <div class="head">
                <div>
                  <h2>Auditoría de proveedores</h2>
                  <div class="small">
                    Periodo: <strong>{_esc(period_label)}</strong>
                    · Vista global de todos los bots
                  </div>
                </div>
                <div>
                  <a class="btn btn-secondary" href="{_esc(panel_url)}">← Volver al panel principal</a>
                </div>
              </div>

              <div style="margin-top:12px;">
                <a class="btn" href="{_esc(_audit_link("day"))}">Hoy</a>
                <a class="btn" href="{_esc(_audit_link("30d"))}">30 días</a>
                <a class="btn" href="{_esc(_audit_link("month"))}">Mes actual</a>
                <a class="btn" href="{_esc(_audit_link("prev_month"))}">Mes anterior</a>
              </div>
            </div>
        """

        html += """
            <div class="box">
              <div class="head">
                <div>
                  <strong>Control contable por proveedor</strong>
                  <div class="small">
                    Fórmula: Total = Éxito + Sin registro + Erróneas/duplicadas + Otros errores + Pendientes.
                  </div>
                </div>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Proveedor</th>
                      <th class="right">Total con éxito</th>
                      <th class="right">Sin registro</th>
                      <th class="right">Erróneas / duplicadas</th>
                      <th class="right">Otros errores</th>
                      <th class="right">Pendientes</th>
                      <th class="right">Total solicitudes</th>
                    </tr>
                  </thead>
                  <tbody>
        """

        if provider_control_rows:
            for r in provider_control_rows:
                html += f"""
                    <tr>
                      <td><strong>{_esc(_provider_label(r["provider_name"]))}</strong></td>
                      <td class="right">{r["total_exito"]}</td>
                      <td class="right">{r["sin_registro"]}</td>
                      <td class="right">{r["actas_erroneas"]}</td>
                      <td class="right">{r["otros_errores"]}</td>
                      <td class="right">{r["pendientes"]}</td>
                      <td class="right"><strong>{r["total_solicitudes"]}</strong></td>
                    </tr>
                """

            html += f"""
                    <tr class="total-row">
                      <td>TOTAL GENERAL</td>
                      <td class="right">{provider_control_totals["total_exito"]}</td>
                      <td class="right">{provider_control_totals["sin_registro"]}</td>
                      <td class="right">{provider_control_totals["actas_erroneas"]}</td>
                      <td class="right">{provider_control_totals["otros_errores"]}</td>
                      <td class="right">{provider_control_totals["pendientes"]}</td>
                      <td class="right">{provider_control_totals["total_solicitudes"]}</td>
                    </tr>
            """
        else:
            html += '<tr><td colspan="7">Sin datos para este periodo.</td></tr>'

        html += """
                  </tbody>
                </table>
              </div>
            </div>
        """

        html += """
            <div class="box">
              <div class="head">
                <div>
                  <strong>Detalle de errores por proveedor</strong>
                  <div class="small">
                    Agrupa los mensajes técnicos de error en categorías entendibles.
                  </div>
                </div>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Proveedor</th>
                      <th>Tipo de error</th>
                      <th class="right">Cantidad</th>
                      <th>Ejemplo técnico</th>
                      <th>Solicitudes / CURP</th>
                    </tr>
                  </thead>
                  <tbody>
        """

        if error_detail_rows:
            for r in error_detail_rows:
                examples_html = "<br>".join(_esc(x) for x in r.get("examples", []))

                item_rows_html = ""

                for item in r.get("items", []):
                    item_rows_html += f"""
                        <tr>
                          <td class="mono">{item["id"]}</td>
                          <td class="mono">{_esc(item["created_at"])}</td>
                          <td class="mono">{_esc(item["curp"])}</td>
                          <td>{_esc(item["act_type"])}</td>
                          <td>{_esc(bot_label(item["instance_name"], db))}<br><span class="small mono">{_esc(item["instance_name"])}</span></td>
                          <td class="mono">{_esc(item["source_group_id"])}</td>
                          <td class="mono">{_esc(item["requester_wa_id"])}</td>
                          <td class="small mono">{_esc(item["error_message"][:260])}</td>
                        </tr>
                    """

                if item_rows_html:
                    items_html = f"""
                        <details>
                          <summary>Ver {len(r.get("items", []))} solicitud(es)</summary>
                          <div style="margin-top:8px; max-height:360px; overflow:auto;">
                            <table>
                              <thead>
                                <tr>
                                  <th>ID</th>
                                  <th>Fecha MTY</th>
                                  <th>CURP / dato</th>
                                  <th>Tipo</th>
                                  <th>Bot</th>
                                  <th>Grupo origen</th>
                                  <th>Solicitante</th>
                                  <th>Error técnico</th>
                                </tr>
                              </thead>
                              <tbody>
                                {item_rows_html}
                              </tbody>
                            </table>
                          </div>
                        </details>
                    """
                else:
                    items_html = '<span class="small">Sin detalle.</span>'

                html += f"""
                    <tr>
                      <td><strong>{_esc(_provider_label(r["provider_name"]))}</strong></td>
                      <td>{_esc(r["error_type"])}</td>
                      <td class="right"><strong>{r["total"]}</strong></td>
                      <td class="small mono">{examples_html}</td>
                      <td>{items_html}</td>
                    </tr>
                """
        else:
            html += '<tr><td colspan="5">Sin errores registrados para este periodo.</td></tr>'
            
        html += """
                  </tbody>
                </table>
              </div>
            </div>
        """

        html += """
            <div class="box">
              <div class="head">
                <div>
                  <strong>Proveedor + bot</strong>
                  <div class="small">
                    Sirve para detectar si el problema viene de un proveedor completo o de una combinación proveedor/bot.
                  </div>
                </div>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Proveedor</th>
                      <th>Bot</th>
                      <th class="right">Éxito</th>
                      <th class="right">Sin registro</th>
                      <th class="right">Erróneas</th>
                      <th class="right">Otros errores</th>
                      <th class="right">Pendientes</th>
                      <th class="right">Total</th>
                    </tr>
                  </thead>
                  <tbody>
        """

        if provider_bot_rows:
            for r in provider_bot_rows:
                html += f"""
                    <tr>
                      <td><strong>{_esc(_provider_label(r["provider_name"]))}</strong></td>
                      <td>{_esc(bot_label(r["instance_name"], db))}<br><span class="small mono">{_esc(r["instance_name"])}</span></td>
                      <td class="right">{r["total_exito"]}</td>
                      <td class="right">{r["sin_registro"]}</td>
                      <td class="right">{r["actas_erroneas"]}</td>
                      <td class="right">{r["otros_errores"]}</td>
                      <td class="right">{r["pendientes"]}</td>
                      <td class="right"><strong>{r["total_solicitudes"]}</strong></td>
                    </tr>
                """
        else:
            html += '<tr><td colspan="8">Sin datos para este periodo.</td></tr>'

        html += """
                  </tbody>
                </table>
              </div>
            </div>
        """

        html += """
            <div class="box">
              <div class="head">
                <div>
                  <strong>Pendientes críticas</strong>
                  <div class="small">
                    Muestra las solicitudes más viejas que siguen en QUEUED o PROCESSING.
                  </div>
                </div>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Dato</th>
                      <th>Tipo</th>
                      <th>Bot</th>
                      <th>Proveedor</th>
                      <th>Estado</th>
                      <th>Creado</th>
                      <th>Tiempo esperando</th>
                      <th>Último error</th>
                    </tr>
                  </thead>
                  <tbody>
        """

        if pending_rows:
            now_utc = _utc_now_naive()

            for r in pending_rows:
                try:
                    waiting = _fmt_duration_seconds((now_utc - r.created_at).total_seconds()) if r.created_at else ""
                except Exception:
                    waiting = ""

                status_class = {
                    "QUEUED": "status-q",
                    "PROCESSING": "status-p",
                }.get(r.status, "")

                html += f"""
                    <tr>
                      <td>{r.id}</td>
                      <td class="mono">{_esc(r.curp)}</td>
                      <td>{_esc(r.act_type)}</td>
                      <td>{_esc(bot_label(r.instance_name or MAIN_PANEL_INSTANCE, db))}<br><span class="small mono">{_esc(r.instance_name or MAIN_PANEL_INSTANCE)}</span></td>
                      <td>{_esc(_provider_label(r.provider_name))}</td>
                      <td class="{status_class}">{_esc(r.status)}</td>
                      <td>{_esc(_fmt_dt(r.created_at))}</td>
                      <td><strong>{_esc(waiting)}</strong></td>
                      <td class="small mono">{_esc((r.error_message or "")[:220])}</td>
                    </tr>
                """
        else:
            html += '<tr><td colspan="9">No hay pendientes en este periodo.</td></tr>'

        html += """
                  </tbody>
                </table>
              </div>
            </div>

          </div>
        </body>
        </html>
        """

        return HTMLResponse(content=html)

    except Exception as e:
        print("panel_auditoria_proveedores error:", repr(e), flush=True)
        return HTMLResponse(
            content=f"<pre>Error en /panel/auditoria-proveedores: {str(e)}</pre>",
            status_code=500,
        )


@app.post("/panel/provider3/update-sid")
def update_provider3_sid(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    sid = (payload.get("phpsessid") or "").strip()

    if not sid:
        return {"ok": False, "error": "SID vacía"}

    _set_app_setting(db, "PROVIDER3_PHPSESSID", sid)

    return {
        "ok": True,
        "message": "SID actualizada",
    }


@app.post("/panel/provider7/update-credentials")
def update_provider7_credentials(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    access_token = (payload.get("access_token") or "").strip()
    jsessionid = (payload.get("jsessionid") or "").strip()
    oficialia = str(payload.get("oficialia") or "").strip()
    rfc_usuario = (payload.get("rfc_usuario") or "").strip().upper()
    estados_dir = (payload.get("estados_dir") or "").strip()

    if not access_token:
        return {"ok": False, "error": "ACCESS_TOKEN vacío"}

    if not jsessionid:
        return {"ok": False, "error": "JSESSIONID vacío"}

    if not oficialia:
        return {"ok": False, "error": "OFICIALIA vacía"}

    if not rfc_usuario:
        return {"ok": False, "error": "RFC_USUARIO vacío"}

    _set_app_setting(db, "PROVIDER7_ACCESS_TOKEN", access_token)
    _set_app_setting(db, "PROVIDER7_JSESSIONID", jsessionid)
    _set_app_setting(db, "PROVIDER7_OFICIALIA", oficialia)
    _set_app_setting(db, "PROVIDER7_RFC_USUARIO", rfc_usuario)

    if estados_dir:
        _set_app_setting(db, "PROVIDER7_ESTADOS_DIR", estados_dir)

    return {
        "ok": True,
        "message": "Credenciales de Provider 7 actualizadas",
    }


@app.post("/panel/provider/{provider_name}/on")
def panel_provider_on(provider_name: str, db: Session = Depends(get_db)):
    row = _get_or_create_provider(db, provider_name.upper(), provider_name.upper() == "PROVIDER1")
    row.is_enabled = True
    row.updated_at = _utc_now_naive()
    db.commit()

    try:
        for key in redis_conn.scan_iter("panel:*"):
            redis_conn.delete(key)
        redis_conn.delete("panel:providers_status_text:v1")
    except Exception:
        pass

    return {"ok": True, "provider": provider_name.upper(), "enabled": True}


@app.post("/panel/provider/{provider_name}/off")
def panel_provider_off(provider_name: str, db: Session = Depends(get_db)):
    row = _get_or_create_provider(db, provider_name.upper(), provider_name.upper() == "PROVIDER1")
    row.is_enabled = False
    row.updated_at = _utc_now_naive()
    db.commit()

    try:
        for key in redis_conn.scan_iter("panel:*"):
            redis_conn.delete(key)
        redis_conn.delete("panel:providers_status_text:v1")
    except Exception:
        pass

    return {"ok": True, "provider": provider_name.upper(), "enabled": False}


def _normalize_wa_actor(value: str) -> str:
    value = (value or "").strip()
    value = value.replace("@s.whatsapp.net", "")
    value = value.replace("@lid", "")
    value = value.replace("@g.us", "")
    value = value.replace("+", "")
    value = value.replace(" ", "")
    return value


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

    db = Session(bind=engine)
    try:
        _get_or_create_provider(db, "PROVIDER1", True)
        _get_or_create_provider(db, "PROVIDER2", False)
        _get_or_create_provider(db, "PROVIDER3", False)
        _get_or_create_provider(db, "PROVIDER4", False)
        _get_or_create_provider(db, "PROVIDER5", False)
        _get_or_create_provider(db, "PROVIDER6", False)
        _get_or_create_provider(db, "PROVIDER7", False)
        _get_or_create_provider(db, "PROVIDER8", False)
        _get_or_create_provider(db, "PROVIDER9", False)
        _get_or_create_provider(db, "PROVIDER10", False)
        _get_or_create_provider(db, "PROVIDER11", False)
        _get_or_create_provider(db, "PROVIDER12", False)
        _get_or_create_provider(db, "PROVIDER13", False)
        _get_or_create_provider(db, "PROVIDER14", False)
        _get_or_create_provider(db, "MAYAPROVIDER", False)
    
        current = _get_app_setting(db, "PROVIDER3_PHPSESSID", "")
        if not current and settings.PROVIDER3_PHPSESSID:
            _set_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)
    finally:
        db.close()


@app.post("/panel/provider4/hid")
def update_provider4_hid(payload: dict, db: Session = Depends(get_db)):
    try:
        new_hid = (payload.get("hid") or "").strip()

        if not new_hid:
            return {"ok": False, "error": "HID vacío"}

        setting = (
            db.query(ProviderSetting)
            .filter(ProviderSetting.provider_name == "PROVIDER4_HID")
            .first()
        )

        if not setting:
            setting = ProviderSetting(
                provider_name="PROVIDER4_HID",
                is_enabled=True,
                value=new_hid,
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
            )
            db.add(setting)
        else:
            setting.value = new_hid
            setting.updated_at = _utc_now_naive()

        db.commit()

        print("PROVIDER4_HID_UPDATED =", new_hid, flush=True)

        return {"ok": True}

    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/panel/provider10/hid")
def update_provider10_hid(payload: dict, db: Session = Depends(get_db)):
    try:
        new_hid = (payload.get("hid") or "").strip()

        if not new_hid:
            return {"ok": False, "error": "HID vacío"}

        setting = (
            db.query(ProviderSetting)
            .filter(ProviderSetting.provider_name == "PROVIDER10_HID")
            .first()
        )

        if not setting:
            setting = ProviderSetting(
                provider_name="PROVIDER10_HID",
                is_enabled=True,
                value=new_hid,
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
            )
            db.add(setting)
        else:
            setting.value = new_hid
            setting.updated_at = _utc_now_naive()

        db.commit()

        print("PROVIDER10_HID_UPDATED =", new_hid, flush=True)

        return {"ok": True}

    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/panel/provider11/hid")
def update_provider11_hid(payload: dict, db: Session = Depends(get_db)):
    try:
        new_hid = (payload.get("hid") or "").strip()

        if not new_hid:
            return {"ok": False, "error": "HID vacío"}

        setting = (
            db.query(ProviderSetting)
            .filter(ProviderSetting.provider_name == "PROVIDER11_HID")
            .first()
        )

        if not setting:
            setting = ProviderSetting(
                provider_name="PROVIDER11_HID",
                is_enabled=True,
                value=new_hid,
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
            )
            db.add(setting)
        else:
            setting.value = new_hid
            setting.updated_at = _utc_now_naive()

        db.commit()

        print("PROVIDER11_HID_UPDATED =", new_hid, flush=True)

        return {"ok": True}

    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.get("/panel/provider3/session")
def get_provider3_session(db: Session = Depends(get_db)):
    current = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)
    masked = ""

    if current:
        if len(current) <= 8:
            masked = "*" * len(current)
        else:
            masked = current[:4] + ("*" * (len(current) - 8)) + current[-4:]

    return {
        "ok": True,
        "phpsessid_masked": masked,
        "has_value": bool(current),
    }


@app.post("/panel/provider3/session")
def update_provider3_session(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    phpsessid = (payload.get("phpsessid") or "").strip()

    if not phpsessid:
        return {"ok": False, "error": "PHPSESSID_EMPTY"}

    _set_app_setting(db, "PROVIDER3_PHPSESSID", phpsessid)

    return {
        "ok": True,
        "message": "PHPSESSID actualizado",
    }


@app.post("/panel/provider3/test")
def test_provider3_session(
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):

    curp = (payload.get("curp") or "").strip().upper()
    tipo_acta = (payload.get("tipo_acta") or "nacimiento").strip().lower()

    if not curp:
        return {"ok": False, "error": "CURP_EMPTY"}

    phpsessid = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)
    client = Provider3Client(phpsessid=phpsessid)

    try:
        result = client.generar_por_curp(
            curp=curp,
            tipo_acta=tipo_acta,
            folio1=False,
            folio2=False,
            reverso=True,
            margen=True,
        )

        has_pdf = bool(result.get("pdf"))
        return {
            "ok": True,
            "has_pdf": has_pdf,
            "remaining": result.get("remaining"),
            "keys": list(result.keys()),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }
        



# =========================
# API EXTERNA ACTAS
# =========================

def _money(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _bearer_token(authorization: str | None) -> str:
    raw = (authorization or "").strip()
    if raw.lower().startswith("bearer "):
        return raw.split(" ", 1)[1].strip()
    return raw


def _get_api_client_or_401(db: Session, authorization: str | None) -> ApiClient:
    token = _bearer_token(authorization)

    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error_code": "MISSING_API_KEY",
                "error_message": "Falta enviar el encabezado Authorization: Bearer TU_API_KEY.",
            },
        )

    client = (
        db.query(ApiClient)
        .filter(
            ApiClient.api_key == token,
            ApiClient.is_active == True,
        )
        .first()
    )

    if not client:
        raise HTTPException(
            status_code=401,
            detail={
                "error_code": "INVALID_API_KEY",
                "error_message": "API key invalida.",
            },
        )

    return client


def _api_pending_reserved_amount(db: Session, client: ApiClient) -> Decimal:
    """
    Reserva el precio congelado de cada solicitud pendiente.

    No usar:
        precio_actual_del_cliente × cantidad_pendiente

    porque el precio del cliente puede cambiar después de crear solicitudes
    que aún siguen QUEUED o PROCESSING.
    """
    current_price = Decimal(str(client.price_per_done or 5))

    reserved = (
        db.query(
            func.coalesce(
                func.sum(
                    func.coalesce(
                        RequestLog.api_price,
                        current_price,
                    )
                ),
                0,
            )
        )
        .filter(
            RequestLog.api_client_id == client.id,
            RequestLog.status.in_(["QUEUED", "PROCESSING"]),
            RequestLog.api_charged == False,
        )
        .scalar()
    )

    return Decimal(str(reserved or 0))


def _ensure_api_panel_group(db: Session, client: ApiClient):
    if not client.count_in_panel:
        return

    if not client.panel_group_jid:
        client.panel_group_jid = f"api_cliente_{client.id}"
        db.commit()
        db.refresh(client)

    row = (
        db.query(AuthorizedGroup)
        .filter(AuthorizedGroup.group_jid == client.panel_group_jid)
        .first()
    )

    if not row:
        row = AuthorizedGroup(
            group_jid=client.panel_group_jid,
            group_name=f"API - {client.name}",
            owner_instance=client.panel_instance_name or "docifybot8",
            is_hidden=False,
            hidden_in_main=False,
        )
        db.add(row)
    else:
        row.owner_instance = client.panel_instance_name or "docifybot8"
        row.is_hidden = False
        row.hidden_in_main = False

    alias = (
        db.query(GroupAlias)
        .filter(GroupAlias.group_jid == client.panel_group_jid)
        .first()
    )

    if not alias:
        alias = GroupAlias(
            group_jid=client.panel_group_jid,
            custom_name=f"API - {client.name}",
            owner_instance=client.panel_instance_name or "docifybot8",
            is_hidden=False,
            hidden_in_main=False,
        )
        db.add(alias)
    else:
        alias.custom_name = f"API - {client.name}"
        alias.owner_instance = client.panel_instance_name or "docifybot8"
        alias.is_hidden = False
        alias.hidden_in_main = False

    db.commit()


def _api_client_panel_rows(db: Session) -> list[dict]:
    """
    Información para panel principal.
    Cada cliente API ya está ligado a docifybot8 mediante panel_group_jid.
    """
    clients = (
        db.query(ApiClient)
        .filter(ApiClient.count_in_panel == True)
        .order_by(ApiClient.created_at.desc(), ApiClient.id.desc())
        .all()
    )

    rows = []

    for client in clients:
        client_id = client.id

        done_count = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.api_client_id == client_id,
                RequestLog.status == "DONE",
            )
            .scalar()
            or 0
        )

        pending_count = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.api_client_id == client_id,
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
            )
            .scalar()
            or 0
        )

        error_count = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.api_client_id == client_id,
                RequestLog.status == "ERROR",
            )
            .scalar()
            or 0
        )

        total_count = (
            db.query(func.count(RequestLog.id))
            .filter(RequestLog.api_client_id == client_id)
            .scalar()
            or 0
        )

        reserved = _api_pending_reserved_amount(db, client)
        balance = Decimal(str(client.credit_balance or 0))
        available = balance - reserved

        last_request = (
            db.query(RequestLog)
            .filter(RequestLog.api_client_id == client_id)
            .order_by(RequestLog.created_at.desc(), RequestLog.id.desc())
            .first()
        )

        last_recharge = (
            db.query(ApiCreditLog)
            .filter(
                ApiCreditLog.api_client_id == client_id,
                ApiCreditLog.type == "RECHARGE",
            )
            .order_by(ApiCreditLog.created_at.desc(), ApiCreditLog.id.desc())
            .first()
        )

        rows.append({
            "id": client.id,
            "name": client.name or f"API #{client.id}",
            "is_active": bool(client.is_active),
            "count_in_panel": bool(client.count_in_panel),
            "panel_instance_name": (
                client.panel_instance_name or "docifybot8"
            ),
            "panel_group_jid": client.panel_group_jid or "",
            "balance": _money(balance),
            "reserved": _money(reserved),
            "available": _money(available),
            "price_per_done": _money(client.price_per_done),
            "done_count": int(done_count),
            "pending_count": int(pending_count),
            "error_count": int(error_count),
            "total_count": int(total_count),
            "created_at": (
                client.created_at.isoformat()
                if getattr(client, "created_at", None)
                else ""
            ),
            "last_request_at": (
                last_request.created_at.isoformat()
                if last_request and last_request.created_at
                else ""
            ),
            "last_request_status": (
                last_request.status
                if last_request
                else ""
            ),
            "last_recharge_at": (
                last_recharge.created_at.isoformat()
                if last_recharge and last_recharge.created_at
                else ""
            ),
        })

    return rows


# ============================================================
# API V1 - CODIGOS PUBLICOS DE ERROR
# ============================================================

API_ALLOWED_ACT_TYPES = {
    "NACIMIENTO",
    "MATRIMONIO",
    "DEFUNCION",
    "DIVORCIO",
    "CADENA",
    "FOLIADA NACIMIENTO",
    "FOLIADA MATRIMONIO",
    "FOLIADA DEFUNCION",
    "FOLIADA DIVORCIO",
}

API_ERROR_MESSAGES = {
    "INVALID_ACT_TYPE": "El tipo de acta no es valido.",
    "MISSING_API_KEY": "Falta enviar el encabezado Authorization: Bearer TU_API_KEY.",
    "INVALID_API_KEY": "API key invalida.",
    "MISSING_TERM": "Falta enviar el dato a consultar.",
    "INVALID_TERM_FORMAT": "El dato enviado no tiene un formato valido.",
    "CURP_INVALID_OR_NOT_FOUND": "La CURP esta mal escrita, no existe o no fue posible encontrar informacion para generar el acta.",
    "ACTA_NOT_FOUND": "No se encontro un acta disponible o registrada para el dato solicitado.",
    "NO_PDF_AVAILABLE": "El proveedor no devolvio un PDF disponible.",
    "WRONG_PDF_DATA": "El PDF recibido no corresponde al dato solicitado.",
    "WRONG_ACT_TYPE": "El PDF recibido corresponde a otro tipo de acta.",
    "PROVIDER_TIMEOUT": "El proveedor tardo demasiado en responder.",
    "PROVIDER_FAILED": "El proveedor no pudo procesar la solicitud.",
    "NO_PROVIDER_AVAILABLE": "No hay proveedor disponible para procesar esta solicitud.",
    "INSUFFICIENT_BALANCE": "Saldo insuficiente para crear una nueva solicitud.",
    "GROUP_LIMIT_REACHED": "El grupo o cliente alcanzo su limite disponible de solicitudes.",
    "DELIVERY_FAILED": "El PDF fue generado, pero no se pudo entregar correctamente.",
    "REQUEST_NOT_FOUND": "La solicitud no existe o no pertenece al cliente autenticado.",
    "REQUEST_NOT_DONE": "La solicitud todavia no esta lista o termino en error.",
    "CREATE_REQUEST_FAILED": "No se pudo crear la solicitud.",
    "UNKNOWN_ERROR": "La solicitud no pudo completarse.",
}


def _api_error_response(error_code: str, **extra):
    error_code = (error_code or "UNKNOWN_ERROR").strip().upper()
    return {
        "ok": False,
        "error_code": error_code,
        "error_message": API_ERROR_MESSAGES.get(error_code, API_ERROR_MESSAGES["UNKNOWN_ERROR"]),
        **extra,
    }


def _api_split_error(raw_error: str | None) -> tuple[str, str]:
    raw = (raw_error or "").strip()
    if not raw:
        return "", ""

    if ":" in raw:
        code, detail = raw.split(":", 1)
        return code.strip(), detail.strip()

    if " | " in raw:
        code, detail = raw.split(" | ", 1)
        return code.strip(), detail.strip()

    return raw.strip(), ""


def _api_clean_internal_error_code(code: str | None) -> str:
    code_up = (code or "").strip().upper()

    # Quita prefijos internos tipo PROVIDER1_WRONG_CURP_IN_PDF
    code_up = re.sub(r"^PROVIDER(?:10|11|[1-9])_", "", code_up)

    # Quita prefijo interno de proveedor privado
    code_up = re.sub(r"^MAYAPROVIDER_", "", code_up)

    return code_up


def _api_public_error_code(raw_error: str | None) -> str:
    original_code, detail = _api_split_error(raw_error)
    code = _api_clean_internal_error_code(original_code)
    raw_up = (raw_error or "").strip().upper()

    # Deteccion por texto crudo de errores comunes que vienen del proveedor
    if (
        "CURP INVALID" in raw_up
        or "CURP INVÁLID" in raw_up
        or "CURP INVALIDA" in raw_up
        or "CURP INVÁLIDA" in raw_up
        or "CURP INEXISTENTE" in raw_up
        or "NO SE ENCONTRO INFORMACION" in raw_up
        or "NO SE ENCONTRÓ INFORMACIÓN" in raw_up
        or "NO SE ENCONTRO INFORMACIÓN" in raw_up
        or "NO SE ENCONTRÓ INFORMACION" in raw_up
        or "NO ENCONTRADA" in raw_up
        or "NO ENCONTRADO" in raw_up
    ):
        return "CURP_INVALID_OR_NOT_FOUND"

    if (
        "ACTA NO REGISTRADA" in raw_up
        or "ACTA NO ENCONTRADA" in raw_up
        or "NO EXISTE ACTA" in raw_up
        or "NO HAY ACTA" in raw_up
    ):
        return "ACTA_NOT_FOUND"

    if not code and not raw_up:
        return ""

    # Formato / captura
    if code in {
        "MISSING_TERM",
        "EMPTY_TERM",
        "TERM_REQUIRED",
    }:
        return "MISSING_TERM"

    if code in {
        "NOT_CURP_OR_CHAIN",
        "INVALID_TERM",
        "INVALID_CURP",
        "BAD_CURP",
        "CURP_EMPTY",
        "CURP_INVALIDA",
    }:
        return "INVALID_TERM_FORMAT"

    # CURP mal escrita / no encontrada
    if code in {
        "NO_DATA",
        "NOT_FOUND",
        "NO_ENCONTRADA",
        "NO_ENCONTRADO",
        "CHECKID_E101_BAD_TERM",
        "CHECKID_E200_NOT_FOUND",
        "CHECKID_NOT_FOUND",
        "SATPI_NOT_FOUND",
        "GOBMX_NOT_FOUND",
    }:
        return "CURP_INVALID_OR_NOT_FOUND"

    # Acta no registrada / no disponible
    if code in {
        "NO_ACTA",
        "ACTA_NOT_FOUND",
        "HISTORY_NOT_CONFIRMED_PDF",
        "HISTORY_NOT_CONFIRMED_FOLIO",
        "NO_PDF_LINK_FOR",
        "NO_FOLIO_LINK_FOR",
        "NOT_REGISTERED",
        "ACTA_NOT_REGISTERED",
    }:
        return "ACTA_NOT_FOUND"

    # PDF no disponible
    if code in {
        "NO_PDF",
        "PROVIDER3_NO_PDF",
        "PDF_NOT_FOUND",
        "DOWNLOAD_FAILED",
        "FOLIO_DOWNLOAD_FAILED",
        "PDF_DOWNLOAD_FAILED",
        "NO_PROVIDER_MEDIA",
    }:
        return "NO_PDF_AVAILABLE"

    # PDF incorrecto
    if code in {
        "WRONG_CURP_IN_PDF",
        "WRONG_ELECTRONIC_ID_OR_CODE_IN_PDF",
        "PDF_TERM_MISMATCH",
    }:
        return "WRONG_PDF_DATA"

    if code in {
        "WRONG_ACT_TYPE",
        "WRONG_ACT_TYPE_PDF_PENDING_RETRY",
    }:
        return "WRONG_ACT_TYPE"

    if code in {
        "SHARED_GROUP_LIMIT_REACHED",
    }:
        return "GROUP_LIMIT_REACHED"
    
    if code in {
        "DELIVERY_FAILED",
        "PDF_SEND_FAILED",
        "PROVIDER3_PDF_SEND_FAILED",
    }:
        return "DELIVERY_FAILED"
    
    if code in {
        "PROVIDER6_ACT_TYPE_NOT_ALLOWED",
    }:
        return "WRONG_ACT_TYPE"

    # Proveedor / configuracion
    if code in {
        "NO_PROVIDER_ENABLED",
        "NO_PROVIDER_FOR_SPECIAL_FORMAT",
        "UNKNOWN_PROVIDER",
        "NO_FOLIADAS_PROVIDER_GROUP_CONFIGURED",
        "NO_BIRTH_PROVIDER_GROUP_CONFIGURED",
        "NO_SPECIAL_PROVIDER_GROUP_CONFIGURED",
        "NO_PROVIDER6_FOLIADAS_GROUP_CONFIGURED",
        "NO_PROVIDER6_ESPECIALES_GROUP_CONFIGURED",
        "NO_PROVIDER6_NACIMIENTO_GROUP_CONFIGURED",
        "PROVIDER2_GROUPS_NOT_CONFIGURED",
        "PROVIDER5_GROUPS_NOT_CONFIGURED",
        "PROVIDER8_GROUPS_NOT_CONFIGURED",
        "PROVIDER9_GROUPS_NOT_CONFIGURED",
        "MAYAPROVIDER_GROUPS_NOT_CONFIGURED",
    }:
        return "NO_PROVIDER_AVAILABLE"

    if (
        "TIMEOUT" in code
        or "TIMED_OUT" in code
        or "READ_TIMED_OUT" in code
        or "TIMEOUT" in raw_up
        or "READ TIMED OUT" in raw_up
    ):
        return "PROVIDER_TIMEOUT"

    if (
        "SEND_FAILED" in code
        or "BACKEND_FAILED" in code
        or "VGET_FAILED" in code
        or "HISTORY_FAILED" in code
        or "FAILED" in code
        or "ERROR" in code
    ):
        return "PROVIDER_FAILED"

    return "UNKNOWN_ERROR"


def _api_public_error_message(error_code: str, raw_error: str | None = None) -> str:
    error_code = (error_code or "UNKNOWN_ERROR").strip().upper()
    return API_ERROR_MESSAGES.get(error_code, API_ERROR_MESSAGES["UNKNOWN_ERROR"])


def _api_request_error_fields(req) -> dict:
    status = (getattr(req, "status", "") or "").upper()

    if status != "ERROR":
        return {
            "error_code": "",
            "error_message": "",
        }

    raw_error = getattr(req, "error_message", None)
    error_code = _api_public_error_code(raw_error)

    if not error_code:
        error_code = "UNKNOWN_ERROR"

    return {
        "error_code": error_code,
        "error_message": _api_public_error_message(error_code, raw_error),
        # Para depuracion. Si no quieres mostrar detalles internos al programador,
        # puedes quitar esta linea.
        "error_raw": raw_error or "",
    }


@app.get("/api/v1/balance")
def api_v1_balance(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    client = _get_api_client_or_401(db, authorization)
    reserved = _api_pending_reserved_amount(db, client)
    available = Decimal(str(client.credit_balance or 0)) - reserved

    return {
        "ok": True,
        "client": client.name,
        "balance": _money(client.credit_balance),
        "reserved": _money(reserved),
        "available": _money(available),
        "price_per_done": _money(client.price_per_done),
        "currency": "MXN",
    }


@app.post("/api/v1/actas")
def api_v1_create_acta(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    # 1) Autentica la API key.
    # Aquí solo obtenemos el id; después bloqueamos la fila real del cliente.
    auth_client = _get_api_client_or_401(db, authorization)

    term = (payload.get("term") or payload.get("curp") or "").strip().upper()
    act_type = (
        payload.get("act_type")
        or payload.get("tipo")
        or "NACIMIENTO"
    ).strip().upper()
    external_id = (payload.get("external_id") or "").strip()

    act_type = re.sub(r"\s+", " ", act_type).strip()

    if act_type not in API_ALLOWED_ACT_TYPES:
        return _api_error_response(
            "INVALID_ACT_TYPE",
            error_raw=f"INVALID_ACT_TYPE:{act_type}",
        )

    if not term:
        return _api_error_response("MISSING_TERM")

    # IMPORTANTE:
    # Primero revisamos duplicado antes de validar saldo.
    # Así un retry legítimo del programador devuelve su solicitud anterior,
    # aunque ya no tenga saldo disponible.
    if external_id:
        existing = (
            db.query(RequestLog)
            .filter(
                RequestLog.api_client_id == auth_client.id,
                RequestLog.api_external_id == external_id,
            )
            .order_by(RequestLog.created_at.desc())
            .first()
        )

        if existing:
            resp = {
                "ok": True,
                "request_id": existing.id,
                "external_id": existing.api_external_id,
                "term": existing.curp,
                "act_type": existing.act_type,
                "status": existing.status,
                "duplicated": True,
                "charged": bool(existing.api_charged),
                "charged_amount": (
                    _money(existing.api_price)
                    if existing.api_charged
                    else 0.0
                ),
            }

            if existing.status == "DONE":
                resp["pdf_url"] = f"/api/v1/actas/{existing.id}/pdf"

            if existing.status == "ERROR":
                resp.update(_api_request_error_fields(existing))

            return resp

    # Esto puede hacer commit porque crea/asegura el grupo virtual del panel.
    # Por eso se hace ANTES de adquirir el bloqueo FOR UPDATE del cliente.
    _ensure_api_panel_group(db, auth_client)

    try:
        # ============================================================
        # BLOQUEO PRINCIPAL DE SALDO
        # ============================================================
        # Solo una creación API por cliente puede pasar por aquí a la vez.
        # Mientras una request calcula reserved/available y crea el RequestLog,
        # cualquier otra request del mismo cliente espera.
        client = (
            db.query(ApiClient)
            .populate_existing()
            .filter(
                ApiClient.id == auth_client.id,
                ApiClient.is_active == True,
            )
            .with_for_update()
            .first()
        )

        if not client:
            return _api_error_response("INVALID_API_KEY")

        # Segunda comprobación de external_id DESPUÉS del lock.
        # Protege contra dos POST idénticos que llegaron casi simultáneamente.
        if external_id:
            existing = (
                db.query(RequestLog)
                .filter(
                    RequestLog.api_client_id == client.id,
                    RequestLog.api_external_id == external_id,
                )
                .order_by(RequestLog.created_at.desc())
                .first()
            )

            if existing:
                resp = {
                    "ok": True,
                    "request_id": existing.id,
                    "external_id": existing.api_external_id,
                    "term": existing.curp,
                    "act_type": existing.act_type,
                    "status": existing.status,
                    "duplicated": True,
                    "charged": bool(existing.api_charged),
                    "charged_amount": (
                        _money(existing.api_price)
                        if existing.api_charged
                        else 0.0
                    ),
                }

                if existing.status == "DONE":
                    resp["pdf_url"] = f"/api/v1/actas/{existing.id}/pdf"

                if existing.status == "ERROR":
                    resp.update(_api_request_error_fields(existing))

                return resp

        # ============================================================
        # BLOQUEO DE DUPLICADO ACTIVO POR CURP + TIPO
        # ============================================================
        # external_id protege reintentos idénticos.
        # Esto protege cuando el cliente manda OTRO external_id para
        # la misma CURP y tipo mientras la primera solicitud sigue viva.
        active_duplicate = (
            db.query(RequestLog)
            .filter(
                RequestLog.api_client_id == client.id,
                RequestLog.curp == term,
                RequestLog.act_type == act_type,
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
            )
            .order_by(RequestLog.created_at.asc())
            .first()
        )

        if active_duplicate:
            return {
                "ok": False,
                "error_code": "ACTIVE_DUPLICATE_REQUEST",
                "error_message": (
                    "Ya existe una solicitud activa para esta CURP y tipo de acta. "
                    "Espera el resultado o consulta la solicitud original."
                ),
                "request_id": active_duplicate.id,
                "existing_external_id": active_duplicate.api_external_id,
                "status": active_duplicate.status,
                "term": active_duplicate.curp,
                "act_type": active_duplicate.act_type,
                "reserved_price": _money(active_duplicate.api_price),
            }

        # Ahora sí: saldo actualizado y protegido por el lock del cliente.
        price = Decimal(str(client.price_per_done or 5))
        reserved = _api_pending_reserved_amount(db, client)
        available = Decimal(str(client.credit_balance or 0)) - reserved

        if available < price:
            return _api_error_response(
                "INSUFFICIENT_BALANCE",
                balance=_money(client.credit_balance),
                reserved=_money(reserved),
                available=_money(available),
                price_per_done=_money(price),
            )

        source_group_id = client.panel_group_jid if client.count_in_panel else None
        source_chat_id = source_group_id or f"api_cliente_{client.id}"

        request_key = (
            f"api:{client.id}:{external_id}"
            if external_id
            else f"api:{client.id}:{uuid.uuid4().hex}"
        )

        now = _utc_now_naive()

        row = RequestLog(
            request_key=request_key,
            curp=term,
            act_type=act_type,
            requester_wa_id=f"api:{client.id}",
            requester_name=client.name,
            source_chat_id=source_chat_id,
            source_group_id=source_group_id,
            instance_name=client.panel_instance_name or "docifybot8",
            evolution_message_id=None,
            status="QUEUED",
            created_at=now,
            updated_at=now,
            expires_at=now + timedelta(days=settings.HISTORY_DAYS),
            api_client_id=client.id,
            api_external_id=external_id or None,
            api_charged=False,
            api_price=price,
            api_count_in_panel=bool(client.count_in_panel),
        )

        db.add(row)
        db.commit()
        db.refresh(row)

    except Exception as e:
        db.rollback()

        print("API_CREATE_REQUEST_ERROR =", {
            "api_client_id": getattr(auth_client, "id", None),
            "external_id": external_id,
            "error": str(e),
        }, flush=True)

        return _api_error_response(
            "CREATE_REQUEST_FAILED",
            error_raw=f"CREATE_REQUEST_FAILED:{str(e)[:200]}",
        )

    try:
        _enqueue_process_request(row, "api_v1_create_acta")
    
    except Exception as enqueue_exc:
        # La solicitud ya existe en PostgreSQL, pero no quedó en Redis/RQ.
        # La marcamos ERROR para que NO reserve saldo eternamente.
        db.rollback()
    
        try:
            failed_row = (
                db.query(RequestLog)
                .filter(
                    RequestLog.id == row.id,
                    RequestLog.api_client_id == auth_client.id,
                )
                .with_for_update()
                .first()
            )
    
            if failed_row and (failed_row.status or "").upper() == "QUEUED":
                failed_row.status = "ERROR"
                failed_row.error_message = (
                    f"QUEUE_ENQUEUE_FAILED:{str(enqueue_exc)[:300]}"
                )
                failed_row.updated_at = _utc_now_naive()
                db.commit()
    
            else:
                db.rollback()
    
        except Exception as mark_error_exc:
            db.rollback()
    
            print("API_QUEUE_FAILURE_MARK_ERROR_FAILED =", {
                "request_id": getattr(row, "id", None),
                "enqueue_error": str(enqueue_exc),
                "mark_error": str(mark_error_exc),
            }, flush=True)
    
        print("API_QUEUE_ENQUEUE_FAILED =", {
            "request_id": getattr(row, "id", None),
            "api_client_id": getattr(auth_client, "id", None),
            "error": str(enqueue_exc),
        }, flush=True)
    
        return _api_error_response(
            "CREATE_REQUEST_FAILED",
            error_raw=f"QUEUE_ENQUEUE_FAILED:{str(enqueue_exc)[:200]}",
        )
    
    return {
        "ok": True,
        "request_id": row.id,
        "external_id": row.api_external_id,
        "status": row.status,
        "price_if_done": _money(price),
        "message": "Solicitud recibida",
    }


@app.get("/api/v1/actas/{request_id}")
def api_v1_get_acta(
    request_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    client = _get_api_client_or_401(db, authorization)

    row = (
        db.query(RequestLog)
        .filter(
            RequestLog.id == request_id,
            RequestLog.api_client_id == client.id,
        )
        .first()
    )

    if not row:
        raise HTTPException(
            status_code=404,
            detail={
                "error_code": "REQUEST_NOT_FOUND",
                "error_message": API_ERROR_MESSAGES["REQUEST_NOT_FOUND"],
            },
        )

    data = {
        "ok": True,
        "request_id": row.id,
        "external_id": row.api_external_id,
        "term": row.curp,
        "act_type": row.act_type,
        "status": row.status,
        "charged": bool(row.api_charged),
        "charged_amount": _money(row.api_price if row.api_charged else 0),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }

    if row.status == "DONE" and row.api_result_base64:
        data["pdf_url"] = str(request.base_url).rstrip("/") + f"/api/v1/actas/{row.id}/pdf"

    if row.status == "ERROR":
        data.update(_api_request_error_fields(row))

    db.refresh(client)
    data["balance"] = _money(client.credit_balance)

    return data


@app.get("/api/v1/actas/{request_id}/pdf")
def api_v1_get_acta_pdf(
    request_id: int,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    client = _get_api_client_or_401(db, authorization)

    row = (
        db.query(RequestLog)
        .filter(
            RequestLog.id == request_id,
            RequestLog.api_client_id == client.id,
        )
        .first()
    )

    if not row:
        raise HTTPException(
            status_code=404,
            detail={
                "error_code": "REQUEST_NOT_FOUND",
                "error_message": API_ERROR_MESSAGES["REQUEST_NOT_FOUND"],
            },
        )

    if row.status != "DONE":
        detail = {
            "error_code": "REQUEST_NOT_DONE",
            "error_message": API_ERROR_MESSAGES["REQUEST_NOT_DONE"],
            "status": row.status,
        }
    
        if row.status == "ERROR":
            detail.update(_api_request_error_fields(row))
    
        raise HTTPException(status_code=400, detail=detail)

    if not row.api_result_base64:
        raise HTTPException(
            status_code=404,
            detail={
                "error_code": "NO_PDF_AVAILABLE",
                "error_message": API_ERROR_MESSAGES["NO_PDF_AVAILABLE"],
                "status": row.status,
            },
        )

    raw = row.api_result_base64.strip()
    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]

    try:
        pdf_bytes = base64.b64decode(raw)
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "NO_PDF_AVAILABLE",
                "error_message": "El PDF existe en la solicitud, pero no se pudo decodificar correctamente.",
                "status": row.status,
            },
        )

    filename = row.api_result_filename or f"{row.curp}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )


@app.get("/api/v1/actas")
def api_v1_list_actas(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    client = _get_api_client_or_401(db, authorization)

    limit = max(1, min(int(limit or 50), 200))
    offset = max(0, int(offset or 0))

    rows = (
        db.query(RequestLog)
        .filter(RequestLog.api_client_id == client.id)
        .order_by(RequestLog.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    items = []

    base_url = str(request.base_url).rstrip("/")
    
    for r in rows:
        item = {
            "request_id": r.id,
            "external_id": r.api_external_id,
            "term": r.curp,
            "act_type": r.act_type,
            "status": r.status,
            "charged": bool(r.api_charged),
            "charged_amount": _money(r.api_price if r.api_charged else 0),
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        }
    
        if r.status == "DONE" and r.api_result_base64:
            item["pdf_url"] = f"{base_url}/api/v1/actas/{r.id}/pdf"
    
        if r.status == "ERROR":
            item.update(_api_request_error_fields(r))
    
        items.append(item)
    
    return {
        "ok": True,
        "items": items,
    }


@app.post("/api/admin/clients")
def api_admin_create_client(
    payload: dict = Body(...),
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    name = (payload.get("name") or "").strip()
    credit = Decimal(str(payload.get("credit_balance") or 0))
    price = Decimal(str(payload.get("price_per_done") or 5))
    panel_instance = (payload.get("panel_instance_name") or "docifybot8").strip()
    panel_group_jid = (payload.get("panel_group_jid") or "").strip()

    if not name:
        return {"ok": False, "error": "MISSING_NAME"}

    if credit < 0:
        return {
            "ok": False,
            "error": "INVALID_CREDIT_BALANCE",
            "message": "El saldo inicial no puede ser negativo.",
        }
    
    if price <= 0:
        return {
            "ok": False,
            "error": "INVALID_PRICE_PER_DONE",
            "message": "El precio por acta debe ser mayor a cero.",
        }

    api_key = "sk_" + secrets.token_urlsafe(32)

    client = ApiClient(
        name=name,
        api_key=api_key,
        credit_balance=credit,
        price_per_done=price,
        panel_instance_name=panel_instance,
        panel_group_jid=panel_group_jid or None,
        count_in_panel=True,
        is_active=True,
    )

    db.add(client)
    db.commit()
    db.refresh(client)

    if not client.panel_group_jid:
        client.panel_group_jid = f"api_cliente_{client.id}"
        db.commit()
        db.refresh(client)

    _ensure_api_panel_group(db, client)

    return {
        "ok": True,
        "client_id": client.id,
        "name": client.name,
        "api_key": client.api_key,
        "credit_balance": _money(client.credit_balance),
        "price_per_done": _money(client.price_per_done),
        "panel_instance_name": client.panel_instance_name,
        "panel_group_jid": client.panel_group_jid,
    }


@app.post("/api/admin/clients/{client_id}/recharge")
def api_admin_recharge_client(
    client_id: int,
    payload: dict = Body(...),
    token: str = "",
    db: Session = Depends(get_db),
):
    if token != PANEL_TOKEN:
        return {"ok": False, "error": "UNAUTHORIZED"}

    amount = Decimal(str(payload.get("amount") or 0))
    note = (payload.get("note") or "").strip()

    if amount <= 0:
        return {"ok": False, "error": "INVALID_AMOUNT"}

    client = (
        db.query(ApiClient)
        .filter(ApiClient.id == client_id)
        .with_for_update()
        .first()
    )

    if not client:
        return {"ok": False, "error": "CLIENT_NOT_FOUND"}

    client.credit_balance = Decimal(str(client.credit_balance or 0)) + amount
    client.updated_at = _utc_now_naive()

    db.add(ApiCreditLog(
        api_client_id=client.id,
        request_log_id=None,
        amount=amount,
        type="RECHARGE",
        note=note or "Recarga manual",
        created_at=_utc_now_naive(),
    ))

    db.commit()

    return {
        "ok": True,
        "client_id": client.id,
        "balance": _money(client.credit_balance),
    }


@app.get("/panel/api/clients")
def panel_api_clients(
    request: Request,
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    return {
        "ok": True,
        "items": _api_client_panel_rows(db),
    }


@app.post("/panel/api/clients")
def panel_api_create_client(
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    name = (payload.get("name") or "").strip()
    credit_raw = str(payload.get("credit_balance") or "0").strip()
    price_raw = str(payload.get("price_per_done") or "5").strip()

    if not name:
        return {
            "ok": False,
            "error": "MISSING_NAME",
            "message": "Escribe el nombre del cliente API.",
        }

    try:
        credit = Decimal(credit_raw)
        price = Decimal(price_raw)
    except Exception:
        return {
            "ok": False,
            "error": "INVALID_NUMERIC_VALUES",
            "message": "Saldo y precio deben ser números válidos.",
        }

    if credit < 0:
        return {
            "ok": False,
            "error": "INVALID_CREDIT_BALANCE",
            "message": "El saldo inicial no puede ser negativo.",
        }

    if price <= 0:
        return {
            "ok": False,
            "error": "INVALID_PRICE_PER_DONE",
            "message": "El precio por acta debe ser mayor a cero.",
        }

    api_key = "sk_" + secrets.token_urlsafe(32)

    client = ApiClient(
        name=name,
        api_key=api_key,
        credit_balance=credit,
        price_per_done=price,
        panel_instance_name="docifybot8",
        panel_group_jid=None,
        count_in_panel=True,
        is_active=True,
    )

    db.add(client)
    db.commit()
    db.refresh(client)

    client.panel_group_jid = f"api_cliente_{client.id}"
    db.commit()
    db.refresh(client)

    _ensure_api_panel_group(db, client)
    _clear_panel_cache()
    _clear_group_name_cache()

    return {
        "ok": True,
        "message": "Cliente API creado y vinculado a DOCIFYBOT8.",
        "client_id": client.id,
        "name": client.name,
        "api_key": api_key,
        "credit_balance": _money(client.credit_balance),
        "price_per_done": _money(client.price_per_done),
        "panel_instance_name": client.panel_instance_name,
        "panel_group_jid": client.panel_group_jid,
    }


@app.post("/panel/api/clients/{client_id}/recharge")
def panel_api_recharge_client(
    client_id: int,
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    try:
        amount = Decimal(str(payload.get("amount") or "0"))
    except Exception:
        return {"ok": False, "error": "INVALID_AMOUNT"}

    note = (payload.get("note") or "").strip()

    if amount <= 0:
        return {
            "ok": False,
            "error": "INVALID_AMOUNT",
            "message": "La recarga debe ser mayor a cero.",
        }

    client = (
        db.query(ApiClient)
        .filter(ApiClient.id == client_id)
        .with_for_update()
        .first()
    )

    if not client:
        return {"ok": False, "error": "CLIENT_NOT_FOUND"}

    client.credit_balance = Decimal(str(client.credit_balance or 0)) + amount
    client.updated_at = _utc_now_naive()

    db.add(ApiCreditLog(
        api_client_id=client.id,
        request_log_id=None,
        amount=amount,
        type="RECHARGE",
        note=note or "Recarga desde panel principal",
        created_at=_utc_now_naive(),
    ))

    db.commit()
    _clear_panel_cache()

    return {
        "ok": True,
        "client_id": client.id,
        "balance": _money(client.credit_balance),
    }


@app.post("/panel/api/clients/{client_id}/settings")
def panel_api_update_client(
    client_id: int,
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    client = (
        db.query(ApiClient)
        .filter(ApiClient.id == client_id)
        .with_for_update()
        .first()
    )

    if not client:
        return {"ok": False, "error": "CLIENT_NOT_FOUND"}

    new_name = (payload.get("name") or client.name or "").strip()
    price_raw = payload.get("price_per_done", client.price_per_done)
    is_active = bool(payload.get("is_active", client.is_active))

    if not new_name:
        return {
            "ok": False,
            "error": "MISSING_NAME",
            "message": "El cliente debe conservar un nombre.",
        }

    try:
        new_price = Decimal(str(price_raw))
    except Exception:
        return {"ok": False, "error": "INVALID_PRICE_PER_DONE"}

    if new_price <= 0:
        return {
            "ok": False,
            "error": "INVALID_PRICE_PER_DONE",
            "message": "El precio debe ser mayor a cero.",
        }

    client.name = new_name
    client.price_per_done = new_price
    client.is_active = is_active

    # Siempre vinculado al bot principal.
    client.panel_instance_name = "docifybot8"
    client.count_in_panel = True
    client.updated_at = _utc_now_naive()

    db.commit()
    db.refresh(client)

    _ensure_api_panel_group(db, client)
    _clear_panel_cache()
    _clear_group_name_cache()

    return {
        "ok": True,
        "client_id": client.id,
        "name": client.name,
        "price_per_done": _money(client.price_per_done),
        "is_active": bool(client.is_active),
        "panel_instance_name": client.panel_instance_name,
        "panel_group_jid": client.panel_group_jid,
    }


@app.post("/panel/api/clients/{client_id}/rotate-key")
def panel_api_rotate_key(
    client_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    client = (
        db.query(ApiClient)
        .filter(ApiClient.id == client_id)
        .with_for_update()
        .first()
    )

    if not client:
        return {"ok": False, "error": "CLIENT_NOT_FOUND"}

    new_key = "sk_" + secrets.token_urlsafe(32)

    client.api_key = new_key
    client.updated_at = _utc_now_naive()

    db.commit()

    return {
        "ok": True,
        "client_id": client.id,
        "api_key": new_key,
        "message": "Nueva API key creada. La anterior dejó de funcionar.",
    }


@app.get("/health")
def health():
    return {"ok": True}


def get_last_done_request(db: Session, curp: str, act_type: str):
    return (
        db.query(RequestLog)
        .filter(
            RequestLog.curp == curp,
            RequestLog.act_type == act_type,
            RequestLog.status == "DONE"
        )
        .order_by(RequestLog.created_at.desc())
        .first()
    )


def build_request_key(curp: str, act_type: str, source_chat_id: str) -> str:
    return f"{curp}:{act_type}:{source_chat_id}"


def is_authorized_user(db: Session, wa_id: str) -> bool:
    return db.query(AuthorizedUser).filter(AuthorizedUser.wa_id == wa_id).first() is not None


def is_authorized_group(db: Session, group_jid: str) -> bool:
    return db.query(AuthorizedGroup).filter(AuthorizedGroup.group_jid == group_jid).first() is not None


def _deliver_text_result(req: RequestLog, text: str, instance_name: str = None):
    # Las solicitudes API se consultan por endpoint.
    # Nunca intentar mandarlas a api:X ni api_cliente_X.
    if getattr(req, "api_client_id", None):
        print("API_SKIP_WHATSAPP_TEXT_DELIVERY =", {
            "request_id": getattr(req, "id", None),
            "api_client_id": getattr(req, "api_client_id", None),
            "text_preview": (text or "")[:160],
        }, flush=True)
        return False

    instance = req.instance_name or instance_name or "docifybot8"

    text_up = (text or "").upper()

    if (
        getattr(req, "id", None)
        and "NO HAY REGISTROS DISPONIBLES" in text_up
    ):
        dedupe_key = f"no_record_notified:{req.id}"

        try:
            first_notify = redis_conn.set(dedupe_key, "1", nx=True, ex=86400)
        except Exception as dedupe_exc:
            print("NO_RECORD_WEB_DEDUPE_REDIS_ERROR =", str(dedupe_exc), flush=True)
            first_notify = True

        if not first_notify:
            print("NO_RECORD_WEB_DUPLICATE_IGNORED =", dedupe_key, flush=True)
            return False

        print("NO_RECORD_WEB_NOTIFIED_ONCE =", dedupe_key, flush=True)

    if req.source_group_id:
        send_group_text(req.source_group_id, text, instance)
    else:
        send_text(req.requester_wa_id, text, instance)

    return True


def _deliver_pdf_result(req: RequestLog, pdf_data: str, filename: str | None = None, instance_name: str = None):
    instance = req.instance_name or instance_name or "docifybot8"
    filename = filename or f"{req.curp}.pdf"

    caption_text = ""

    NO_TIME_CAPTION_GROUPS = {
        "120363408668441985@g.us",
        "120363421166637606@g.us",
        "120363427267191472@g.us",
    }

    if req.created_at:
        created_at = req.created_at

        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        now_local = datetime.now(ZoneInfo("America/Monterrey"))
        created_at_local = created_at.astimezone(ZoneInfo("America/Monterrey"))
        delta = now_local - created_at_local
        total_seconds = max(0.0, delta.total_seconds())

        if total_seconds >= 60:
            minutes = int(total_seconds // 60)
            seconds = total_seconds % 60
            tiempo = f"{minutes} min {seconds:.2f} segundos"
        else:
            tiempo = f"{total_seconds:.2f} segundos"

        if req.source_group_id not in NO_TIME_CAPTION_GROUPS:
            caption_text = f"⏱️ Tiempo total: {tiempo}"

    print("PDF_DELIVER_INSTANCE =", instance, flush=True)
    print("PDF_CAPTION =", caption_text, flush=True)

    is_base64 = not pdf_data.startswith("http")

    if getattr(req, "api_client_id", None):
        raw = (pdf_data or "").strip()

        if not is_base64:
            r = requests.get(pdf_data, timeout=60)
            r.raise_for_status()
            raw = base64.b64encode(r.content).decode()

        if raw.startswith("data:"):
            raw = raw.split(",", 1)[1]

        raw = raw.replace("\n", "").replace("\r", "").strip()

        req.api_result_base64 = raw
        req.api_result_filename = filename or f"{req.curp}.pdf"
        req.provider_media_url = "BASE64_API_FROM_PROVIDER_WEBHOOK"
        req.pdf_url = None
        req.updated_at = _utc_now_naive()

        print("API_MAIN_PDF_STORED_NO_WHATSAPP =", {
            "req_id": req.id,
            "filename": req.api_result_filename,
            "b64_len": len(raw),
        }, flush=True)

        return

    if req.source_group_id:
        if is_base64:
            send_group_document_base64(
                req.source_group_id,
                pdf_data,
                filename=filename,
                caption=caption_text,
                instance_name=instance,
            )
        else:
            send_group_document(
                req.source_group_id,
                pdf_data,
                filename=filename,
                caption=caption_text,
                instance_name=instance,
            )
    else:
        if is_base64:
            send_document_base64(
                req.requester_wa_id,
                pdf_data,
                filename=filename,
                caption=caption_text,
                instance_name=instance,
            )
        else:
            send_document(
                req.requester_wa_id,
                pdf_data,
                filename=filename,
                caption=caption_text,
                instance_name=instance,
            )


def _provider_no_record_patterns():
    raw = settings.PROVIDER_NO_RECORD_TEXT or ""
    return [normalize_text(x) for x in raw.split("|") if x.strip()]


def _is_no_record_message(text_upper: str) -> bool:
    patterns = _provider_no_record_patterns()
    return any(p in text_upper for p in patterns)


def _extract_provider_identifier_loose(text_body: str) -> str | None:
    return extract_identifier_loose(text_body)


def _extract_provider_no_record_identifiers(text_body: str | None) -> list[str]:
    """
    Extrae TODOS los identificadores de mensajes individuales tipo PROVIDER_NO_RECORD_TEXT.

    Usa settings.PROVIDER_NO_RECORD_TEXT como base de frases negativas.

    Soporta:
    GACG640211HOCLSD00    No hay registros disponibles
    MALA530523HGRRPL04    No hay registros disponibles
    CAUS580930MGRLRF02    No hay registros disponibles
    09007002520260158490No hay registros disponibles
    """
    raw = (text_body or "").replace("\u00A0", " ").strip()
    if not raw:
        return []

    # Normalizado para comparar contra PROVIDER_NO_RECORD_TEXT.
    norm = normalize_text(raw)

    # CURP y cadena/folio electrónico.
    curp_pat = r"[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
    chain_pat = r"\d{18,25}"

    ids_pat = rf"(?:{curp_pat}|{chain_pat})"

    # Usamos exactamente las frases configuradas en PROVIDER_NO_RECORD_TEXT.
    patterns = _provider_no_record_patterns()

    found = []

    for phrase in patterns:
        phrase = normalize_text(phrase)
        if not phrase:
            continue

        # Convertir la frase normalizada a regex flexible:
        # "NO HAY REGISTROS DISPONIBLES" -> NO\s+HAY\s+REGISTROS\s+DISPONIBLES
        phrase_regex = r"\s+".join(re.escape(part) for part in phrase.split())

        # Caso normal:
        # CURP + texto negativo
        # CADENA + texto negativo
        # También soporta pegado:
        # 09007002520260158490No hay registros disponibles
        pattern_before = re.compile(
            rf"(?P<id>{ids_pat})\s*(?:[-:|,;]*)\s*(?P<neg>{phrase_regex})",
            re.IGNORECASE,
        )

        # Caso inverso por seguridad:
        # texto negativo + CURP/CADENA
        pattern_after = re.compile(
            rf"(?P<neg>{phrase_regex})\s*(?:[-:|,;]*)\s*(?P<id>{ids_pat})",
            re.IGNORECASE,
        )

        for m in pattern_before.finditer(norm):
            ident = re.sub(r"[^A-Z0-9]", "", m.group("id").upper())
            if ident:
                # No eliminar repetidos:
                # cada aparición puede corresponder a una solicitud distinta.
                found.append(ident)
        
        for m in pattern_after.finditer(norm):
            ident = re.sub(r"[^A-Z0-9]", "", m.group("id").upper())
            if ident:
                # No eliminar repetidos:
                # cada aparición puede corresponder a una solicitud distinta.
                found.append(ident)

    return found


def _provider_negative_response_info(text_body: str | None) -> dict:
    """
    Detecta respuestas negativas de proveedores WhatsApp en 2 formas:

    1) Respuesta/reply al mensaje del bot:
       SIN
       -
       No hay registros disponibles

    2) Respuesta mencionando el dato:
       MAXV471219MPLRXC02 No hay registros disponibles
       COLJ740826MSRRPQ04 SIN
       COLJ740826MSRRPQ04 -
    """
    raw = (text_body or "").replace("\u00A0", " ").strip()
    norm = normalize_text(raw)
    up = norm.upper()

    identifiers = _extract_provider_no_record_identifiers(raw)

    loose_id = _extract_provider_identifier_loose(raw)
    if loose_id and loose_id not in identifiers:
        identifiers.append(loose_id)

    long_negative_patterns = [
        r"\bNO\s+HAY\s+REGISTROS?\s+DISPONIBLES?\b",
        r"\bNO\s+HAY\s+REGISTROS?\b",
        r"\bHUBO\s+UN\s+ERROR\s+CON\s+LA\s+SOLICITUD\b",
        r"\bNO\s+ENCONTRAD[OA]\s+POR\s+CURP\b",
        r"\bVERIFICA\s+Y\s+VUELVE\s+A\s+INTENTAR\b",
        r"\bNO\s+SE\s+ENCONTRO\b",
        r"\bNO\s+SE\s+ENCONTR[ÓO]\b",
        r"\bNO\s+SE\s+ENCUENTRA\b",
        r"\bNO\s+LOCALIZAD[OA]\b",
        r"\bNO\s+EXISTE\b",
        r"\bNO\s+EST[ÁA]\b",
        r"\bSIN\s+REGISTROS?\b",
        r"\bSIN\s+RESULTADOS?\b",
        r"\bSIN\s+DATOS\b",
        r"\bACTA\s+NO\s+ENCONTRADA\b",
        r"\bDOCUMENTO\s+NO\s+ENCONTRADO\b",
        r"\bERROR!?\s+CURP\s+INV[ÁA]LIDA\b",
        r"\bNO\s+SE\s+HA\s+ENCONTRADO\b",
        r"\bNO\s+SE\s+ENCONTRARON\b",
    ]

    has_long_negative = any(re.search(p, up) for p in long_negative_patterns)

    short_tokens = {"SIN", "SIN.", "SIN,", "S/N", "SN", "-", "N/A", "NA", "NB", "VERI", "VERIFICAR"}

    clean_short = up.strip()
    is_short_negative_alone = clean_short in short_tokens

    curp_pat = r"[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
    chain_pat = r"\d{18,25}"
    id_pat = rf"(?:{curp_pat}|{chain_pat})"

    is_identifier_plus_short_negative = bool(
        re.search(
            rf"\b(?P<id>{id_pat})\b\s*(?:[-:|,;]*)\s*(?:SIN|SIN\.|SIN,|S/N|SN|-|\.|N/A|NA|NB|VERI|VERIFICAR)\s*$",
            up,
            re.IGNORECASE,
        )
    )

    if is_identifier_plus_short_negative:
        m = re.search(rf"\b(?P<id>{id_pat})\b", up, re.IGNORECASE)
        if m:
            ident = re.sub(r"[^A-Z0-9]", "", m.group("id").upper())
            if ident and ident not in identifiers:
                identifiers.append(ident)

    is_negative = bool(
        has_long_negative
        or is_short_negative_alone
        or is_identifier_plus_short_negative
    )

    return {
        "is_negative": is_negative,
        "identifiers": identifiers,
        "is_short_negative_alone": is_short_negative_alone,
        "has_long_negative": has_long_negative,
        "raw": raw,
        "norm": up,
    }


def _notify_client_no_record(open_req: RequestLog):
    msg = (
        f"❌ No hay registros disponibles.\n"
        f"Dato: {open_req.curp}\n"
        f"Tipo: {open_req.act_type}\n\n"
        f"Verificar que la CURP esté certificada en RENAPO"
    )

    _deliver_text_result(open_req, msg)


def _close_provider_negative_response(
    db: Session,
    *,
    source_chat_id: str,
    quoted_msg_id: str | None,
    text_body: str | None,
) -> dict:
    """
    Cierra solicitudes PROCESSING/QUEUED cuando proveedor WhatsApp responde negativo.

    Soporta:
    - Respuesta individual por reply/cita.
    - Una o varias CURP/cadenas en el mismo mensaje.
    - Mensajes tipo lista:
        CURP1 No hay registros disponibles
        CURP2 No hay registros disponibles

    Para mensajes con varios identificadores, cierra y notifica cada
    solicitud que tenga coincidencia segura.
    """
    info = _provider_negative_response_info(text_body)

    if not info["is_negative"]:
        return {"closed": False, "reason": "not_negative"}

    text_norm = info["norm"]
    negative_act_group = _text_mentions_act_type_group(text_norm)
    
    matched_requests = []
    matched_ids = set()
    match_modes = {}
    
    def _pick_by_type(
        rows,
        *,
        explicit_identifier: bool = False,
    ):
        if not rows:
            return None
    
        # Excluir solicitudes que ya fueron relacionadas con otro renglón
        # del mismo mensaje del proveedor.
        available_rows = [
            row
            for row in rows
            if row.id not in matched_ids
        ]
    
        if not available_rows:
            return None
    
        # Cuando el proveedor menciona explícitamente el tipo de acta,
        # limitar las candidatas a ese tipo.
        if negative_act_group:
            typed = [
                row
                for row in available_rows
                if _expected_act_type_group(row.act_type) == negative_act_group
            ]
    
            print(
                "NEGATIVE_PICK_BY_TYPE =",
                {
                    "negative_act_group": negative_act_group,
                    "candidate_ids": [
                        row.id for row in available_rows
                    ],
                    "candidate_types": [
                        row.act_type for row in available_rows
                    ],
                    "typed_ids": [
                        row.id for row in typed
                    ],
                },
                flush=True,
            )
    
            if not typed:
                return None
    
            # Si hay varias del mismo tipo, tomar primero la más antigua.
            return sorted(
                typed,
                key=lambda row: (
                    row.created_at or datetime.min,
                    row.id,
                ),
            )[0]
    
        # Caso normal: solo hay una candidata.
        if len(available_rows) == 1:
            return available_rows[0]
    
        if explicit_identifier:
            type_groups = {
                _expected_act_type_group(row.act_type)
                for row in available_rows
            }
    
            # Es seguro tomar una por una cuando:
            # 1. El proveedor escribió explícitamente la CURP.
            # 2. Todas las solicitudes abiertas son del mismo tipo.
            #
            # Ejemplo:
            # CUTM... No hay registros disponibles
            # CUTM... No hay registros disponibles
            #
            # Dos solicitudes abiertas de MATRIMONIO.
            if len(type_groups) == 1:
                selected = sorted(
                    available_rows,
                    key=lambda row: (
                        row.created_at or datetime.min,
                        row.id,
                    ),
                )[0]
    
                print(
                    "NEGATIVE_REPEATED_IDENTIFIER_SELECTED =",
                    {
                        "selected_request_id": selected.id,
                        "identifier": selected.curp,
                        "act_type": selected.act_type,
                        "remaining_candidate_ids": [
                            row.id for row in available_rows
                        ],
                    },
                    flush=True,
                )
    
                return selected
    
        print(
            "NEGATIVE_AMBIGUOUS_WITHOUT_TYPE =",
            {
                "candidate_ids": [
                    row.id for row in available_rows
                ],
                "candidate_types": [
                    row.act_type for row in available_rows
                ],
                "explicit_identifier": explicit_identifier,
                "text": info["raw"][:180],
            },
            flush=True,
        )
    
        return None

    def _add_match(req, mode: str):
        if not req:
            return False

        if req.id in matched_ids:
            return False

        matched_ids.add(req.id)
        matched_requests.append(req)
        match_modes[req.id] = mode
        return True

    # 1) Si es respuesta/cita a un mensaje específico, intenta cerrar ese.
    if quoted_msg_id:
        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.provider_group_id == source_chat_id,
                RequestLog.provider_message_id == quoted_msg_id,
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.provider_name.in_(WHATSAPP_TEXT_PROVIDERS),
            )
            .order_by(RequestLog.created_at.desc())
            .all()
        )

        _add_match(
            _pick_by_type(rows),
            "quoted_msg_id",
        )

    # 2) Por cada CURP/cadena del texto, buscar primero dentro del grupo proveedor.
    for ident in info["identifiers"]:
        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.provider_group_id == source_chat_id,
                RequestLog.curp == ident,
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.provider_name.in_(WHATSAPP_TEXT_PROVIDERS),
            )
            # La primera respuesta negativa debe cerrar primero
            # la solicitud más antigua.
            .order_by(
                RequestLog.created_at.asc(),
                RequestLog.id.asc(),
            )
            .all()
        )
    
        req = _pick_by_type(
            rows,
            explicit_identifier=True,
        )

        if req:
            _add_match(req, "provider_group_id_curp")
            continue

        # 3) Respaldo seguro por CURP/cadena si el provider_group_id no coincide.
        # Solo acepta coincidencia única reciente.
        recent_limit = _utc_now_naive() - timedelta(hours=18)

        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.curp == ident,
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.provider_name.in_(WHATSAPP_TEXT_PROVIDERS),
                RequestLog.created_at >= recent_limit,
            )
            .order_by(
                RequestLog.created_at.asc(),
                RequestLog.id.asc(),
            )
            .limit(20)
            .all()
        )
        
        _add_match(
            _pick_by_type(
                rows,
                explicit_identifier=True,
            ),
            "curp_recent_explicit",
        )

    if not matched_requests:
        print("PROVIDER_NEGATIVE_WITHOUT_MATCH =", {
            "source_chat_id": source_chat_id,
            "quoted_msg_id": quoted_msg_id,
            "identifiers": info["identifiers"],
            "text": info["raw"][:250],
        }, flush=True)

        return {
            "closed": False,
            "reason": "negative_without_match",
            "identifiers": info["identifiers"],
        }

    now = _utc_now_naive()

    for req in matched_requests:
        req.status = "ERROR"
        req.error_message = (
            f"SIN REGISTRO | PROVIDER_NEGATIVE_TEXT: {info['raw'][:180]}"
        )
        req.updated_at = now

    db.commit()

    notified_request_ids = []
    notify_errors = []

    for req in matched_requests:
        try:
            _notify_client_no_record(req)
            notified_request_ids.append(req.id)

        except Exception as notify_exc:
            notify_errors.append({
                "request_id": req.id,
                "error": str(notify_exc),
            })

            print("PROVIDER_NEGATIVE_NOTIFY_ERROR =", {
                "request_id": req.id,
                "error": str(notify_exc),
            }, flush=True)

    print("PROVIDER_NEGATIVE_MULTI_CLOSED =", {
        "request_ids": [req.id for req in matched_requests],
        "curps": [req.curp for req in matched_requests],
        "act_types": [req.act_type for req in matched_requests],
        "match_modes": match_modes,
        "notified_request_ids": notified_request_ids,
        "notify_errors": notify_errors,
        "quoted_msg_id": quoted_msg_id,
        "identifiers": info["identifiers"],
        "text": info["raw"][:250],
    }, flush=True)

    return {
        "closed": True,
        "count": len(matched_requests),
        "request_ids": [req.id for req in matched_requests],
        "notified_request_ids": notified_request_ids,
        "match_modes": match_modes,
        "notify_errors": notify_errors,
    }


def _extract_identifier_from_filename_local(filename: str) -> str | None:
    return extract_identifier_from_filename(filename)


def _text_mentions_act_type_group(text: str | None) -> str:
    t = normalize_text(text or "")

    if "MATRIMONIO" in t or "MATRI" in t:
        return "MATRIMONIO"

    if "NACIMIENTO" in t or "NACIM" in t:
        return "NACIMIENTO"

    if "DEFUNCION" in t or "DEFUN" in t:
        return "DEFUNCION"

    if "DIVORCIO" in t or "DIVOR" in t:
        return "DIVORCIO"

    return ""


def _find_same_curp_req_by_act_type(
    db: Session,
    *,
    curp: str,
    provider_group_id: str,
    detected_type: str,
):
    detected_type = (detected_type or "").strip().upper()

    if not curp or not provider_group_id or not detected_type:
        return None

    candidates = (
        db.query(RequestLog)
        .filter(
            RequestLog.curp == curp,
            RequestLog.provider_group_id == provider_group_id,
            RequestLog.status == "PROCESSING",
        )
        .order_by(RequestLog.created_at.asc())
        .all()
    )

    matched = [
        r for r in candidates
        if _expected_act_type_group(r.act_type) == detected_type
    ]

    print("SAME_CURP_ACT_TYPE_CANDIDATES =", {
        "curp": curp,
        "provider_group_id": provider_group_id,
        "detected_type": detected_type,
        "candidate_ids": [r.id for r in candidates],
        "candidate_types": [r.act_type for r in candidates],
        "matched_ids": [r.id for r in matched],
    }, flush=True)

    if len(matched) == 1:
        return matched[0]

    return None


def _pdf_matches_req_type(pdf_bytes: bytes, req: RequestLog) -> bool:
    try:
        if is_chain(req.curp):
            return True
        return _validate_act_type_pdf(pdf_bytes, req.act_type)
    except Exception as e:
        print("PDF_MATCHES_REQ_TYPE_ERROR =", getattr(req, "id", None), str(e), flush=True)
        return False


def _provider_pdf_match_status_filter():
    # Antes estaba en 5 minutos. Era muy poco para PDFs de proveedores WhatsApp.
    # Si el proveedor manda el PDF después del timeout, aún debe poder recuperarse.
    recent_limit = _utc_now_naive() - timedelta(minutes=15)

    return or_(
        RequestLog.status == "PROCESSING",
        and_(
            RequestLog.status == "ERROR",
            RequestLog.updated_at >= recent_limit,
            or_(
                RequestLog.error_message.ilike("%Timeout automático%"),
                RequestLog.error_message.ilike("%DELIVERY_FAILED%"),
                RequestLog.error_message.ilike("%Connection Closed%"),
                RequestLog.error_message.ilike("%SEND_FAILED%"),
                RequestLog.error_message.ilike("%sendMedia%"),
            ),
        ),
    )


def _pick_matching_processing_req_for_pdf(
    db: Session,
    lookup_id: str | None,
    source_chat_id: str,
    quoted_msg_id: str | None,
    pdf_bytes: bytes,
    instance_name: str | None = None,
):
    candidates = []
    status_filter = _provider_pdf_match_status_filter()

    # IMPORTANTE:
    # NO filtrar por RequestLog.instance_name aquí.
    # El PDF del proveedor global llega por docifybot8,
    # pero el request original puede pertenecer a docifybot8moon/max/rywya.
    # El match correcto es por CURP + provider_group_id o provider_message_id.

    if lookup_id:
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.curp == lookup_id,
                status_filter,
                RequestLog.provider_group_id == source_chat_id,
            )
            .order_by(
                case((RequestLog.status == "PROCESSING", 0), else_=1),
                RequestLog.created_at.asc(),
            )
            .all()
        )

    if not candidates and quoted_msg_id:
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.provider_message_id == quoted_msg_id,
                status_filter,
            )
            .order_by(
                case((RequestLog.status == "PROCESSING", 0), else_=1),
                RequestLog.created_at.desc(),
            )
            .all()
        )

    if not candidates and lookup_id:
        today_limit = _utc_now_naive() - timedelta(hours=18)
    
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.curp == lookup_id,
                RequestLog.created_at >= today_limit,
                status_filter,
            )
            .order_by(
                case((RequestLog.status == "PROCESSING", 0), else_=1),
                RequestLog.created_at.desc(),
            )
            .limit(20)
            .all()
        )
    
        print("PROVIDER_PDF_MATCH_STAGE_ANY_GROUP =", [
            {
                "id": r.id,
                "curp": r.curp,
                "act_type": r.act_type,
                "status": r.status,
                "error_message": r.error_message,
                "provider_name": r.provider_name,
                "provider_group_id": r.provider_group_id,
                "source_group_id": r.source_group_id,
                "instance_name": r.instance_name,
                "created_at": str(r.created_at),
                "updated_at": str(r.updated_at),
            }
            for r in candidates
        ], flush=True)

    print("PROVIDER_PDF_MATCH_CANDIDATES =", [
        {
            "id": r.id,
            "curp": r.curp,
            "act_type": r.act_type,
            "status": r.status,
            "error_message": r.error_message,
            "provider_group_id": r.provider_group_id,
            "source_group_id": r.source_group_id,
            "instance_name": r.instance_name,
            "provider_name": r.provider_name,
        }
        for r in candidates
    ], flush=True)

    detected_pdf_type = _detect_pdf_act_type(pdf_bytes)

    print("PROVIDER_PDF_DETECTED_TYPE_FOR_MATCH =", {
        "lookup_id": lookup_id,
        "detected_pdf_type": detected_pdf_type,
        "candidate_ids": [r.id for r in candidates],
        "candidate_types": [r.act_type for r in candidates],
    }, flush=True)
    
    # Si el PDF trae tipo claro y hay candidatos con la misma CURP,
    # escoger SOLO el request cuyo tipo coincida.
    if detected_pdf_type:
        typed_candidates = [
            r for r in candidates
            if is_chain(r.curp) or _expected_act_type_group(r.act_type) == detected_pdf_type
        ]
    
        if len(typed_candidates) == 1:
            r = typed_candidates[0]
            print("PROVIDER_PDF_SMART_DETECTED_TYPE_MATCH =", {
                "matched_req_id": r.id,
                "matched_act_type": r.act_type,
                "detected_pdf_type": detected_pdf_type,
                "matched_status": r.status,
                "matched_instance_name": r.instance_name,
            }, flush=True)
            return r
    
        if len(typed_candidates) > 1:
            same_request_keys = {
                (
                    (r.curp or "").strip().upper(),
                    (r.act_type or "").strip().upper(),
                    (r.provider_name or "").strip().upper(),
                    (r.provider_group_id or "").strip(),
                    (r.source_group_id or "").strip(),
                    (r.instance_name or "").strip(),
                )
                for r in typed_candidates
            }

            # Si todos son realmente el mismo pedido duplicado,
            # elegir el más reciente y cancelar los demás duplicados.
            if len(same_request_keys) == 1:
                picked = sorted(
                    typed_candidates,
                    key=lambda r: (
                        r.created_at or datetime.min,
                        r.id or 0,
                    ),
                    reverse=True,
                )[0]

                duplicate_ids = [r.id for r in typed_candidates if r.id != picked.id]

                print("PROVIDER_PDF_DUPLICATE_SAME_REQUEST_PICKED =", {
                    "lookup_id": lookup_id,
                    "detected_pdf_type": detected_pdf_type,
                    "picked_req_id": picked.id,
                    "duplicate_ids": duplicate_ids,
                    "picked_curp": picked.curp,
                    "picked_act_type": picked.act_type,
                    "picked_provider_group_id": picked.provider_group_id,
                    "picked_source_group_id": picked.source_group_id,
                    "picked_instance_name": picked.instance_name,
                }, flush=True)

                now = _utc_now_naive()

                for dup in typed_candidates:
                    if dup.id == picked.id:
                        continue

                    dup.status = "ERROR"
                    dup.error_message = f"DUPLICADO_IGNORADO: se usará solicitud {picked.id}"
                    dup.updated_at = now

                db.commit()

                return picked

            print("PROVIDER_PDF_MULTIPLE_TYPED_MATCHES_AMBIGUOUS =", {
                "lookup_id": lookup_id,
                "detected_pdf_type": detected_pdf_type,
                "typed_candidate_ids": [r.id for r in typed_candidates],
                "typed_candidate_types": [r.act_type for r in typed_candidates],
                "typed_candidate_source_groups": [r.source_group_id for r in typed_candidates],
                "typed_candidate_instances": [r.instance_name for r in typed_candidates],
            }, flush=True)
            return None
    
    # Si no detectó tipo claro, usa validación vieja SOLO si hay un candidato.
    # Si hay varios con misma CURP, no escoger por antigüedad porque puede cruzar nacimiento/matrimonio.
    if len(candidates) == 1:
        r = candidates[0]
    
        if _pdf_matches_req_type(pdf_bytes, r):
            print("PROVIDER_PDF_SINGLE_CANDIDATE_TYPE_OK =", {
                "matched_req_id": r.id,
                "matched_act_type": r.act_type,
                "matched_status": r.status,
                "matched_instance_name": r.instance_name,
            }, flush=True)
            return r
    
        print("PROVIDER_PDF_SINGLE_CANDIDATE_TYPE_UNCONFIRMED_SOFT_MATCH =", {
            "matched_req_id": r.id,
            "matched_act_type": r.act_type,
            "matched_status": r.status,
            "matched_instance_name": r.instance_name,
        }, flush=True)
        return r
    
    if len(candidates) > 1:
        same_request_keys = {
            (
                (r.curp or "").strip().upper(),
                (r.act_type or "").strip().upper(),
                (r.provider_name or "").strip().upper(),
                (r.provider_group_id or "").strip(),
                (r.source_group_id or "").strip(),
                (r.instance_name or "").strip(),
            )
            for r in candidates
        }

        if len(same_request_keys) == 1:
            picked = sorted(
                candidates,
                key=lambda r: (
                    r.created_at or datetime.min,
                    r.id or 0,
                ),
                reverse=True,
            )[0]

            duplicate_ids = [r.id for r in candidates if r.id != picked.id]

            print("PROVIDER_PDF_DUPLICATE_SAME_REQUEST_PICKED_NO_CLEAR_TYPE =", {
                "lookup_id": lookup_id,
                "picked_req_id": picked.id,
                "duplicate_ids": duplicate_ids,
                "picked_curp": picked.curp,
                "picked_act_type": picked.act_type,
            }, flush=True)

            now = _utc_now_naive()

            for dup in candidates:
                if dup.id == picked.id:
                    continue

                dup.status = "ERROR"
                dup.error_message = f"DUPLICADO_IGNORADO: se usará solicitud {picked.id}"
                dup.updated_at = now

            db.commit()

            return picked

        print("PROVIDER_PDF_MULTIPLE_CANDIDATES_NO_CLEAR_TYPE_IGNORE =", {
            "lookup_id": lookup_id,
            "source_chat_id": source_chat_id,
            "candidate_ids": [r.id for r in candidates],
            "candidate_types": [r.act_type for r in candidates],
        }, flush=True)
        return None

    debug_rows = []

    if lookup_id:
        try:
            today_limit = _utc_now_naive() - timedelta(hours=18)
    
            debug_rows = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == lookup_id,
                    RequestLog.created_at >= today_limit,
                )
                .order_by(RequestLog.created_at.desc())
                .limit(10)
                .all()
            )
        except Exception as dbg_exc:
            print("PROVIDER_PDF_UNMATCHED_DB_DEBUG_ERROR =", str(dbg_exc), flush=True)
    
    print("PROVIDER_PDF_NO_SAFE_TYPE_MATCH =", {
        "lookup_id": lookup_id,
        "source_chat_id": source_chat_id,
        "quoted_msg_id": quoted_msg_id,
        "webhook_instance_name": instance_name,
        "candidates": len(candidates),
    }, flush=True)
    
    print("PROVIDER_PDF_UNMATCHED_DB_DEBUG =", [
        {
            "id": r.id,
            "curp": r.curp,
            "act_type": r.act_type,
            "provider_name": r.provider_name,
            "status": r.status,
            "error_message": r.error_message,
            "provider_group_id": r.provider_group_id,
            "source_group_id": r.source_group_id,
            "instance_name": r.instance_name,
            "created_at": str(r.created_at),
            "updated_at": str(r.updated_at),
        }
        for r in debug_rows
    ], flush=True)
    
    return None


def _extract_quoted_message_id(message: dict, data: dict | None = None) -> str:
    try:
        if data:
            top_ctx = (data.get("contextInfo", {}) or {})
            top_id = top_ctx.get("stanzaId", "") or top_ctx.get("quotedStanzaID", "") or ""
            if top_id:
                return top_id

        msg_unwrapped = _unwrap_message(message) or message

        if "extendedTextMessage" in msg_unwrapped:
            ctx = msg_unwrapped.get("extendedTextMessage", {}).get("contextInfo", {}) or {}
            qid = ctx.get("stanzaId", "") or ctx.get("quotedStanzaID", "") or ""
            if qid:
                return qid

        ctx2 = msg_unwrapped.get("contextInfo", {}) or {}
        qid2 = ctx2.get("stanzaId", "") or ctx2.get("quotedStanzaID", "") or ""
        if qid2:
            return qid2

        if "documentWithCaptionMessage" in msg_unwrapped:
            inner = msg_unwrapped.get("documentWithCaptionMessage", {}).get("message", {}) or {}
            return _extract_quoted_message_id(inner)

        if "ephemeralMessage" in msg_unwrapped:
            inner = msg_unwrapped.get("ephemeralMessage", {}).get("message", {}) or {}
            return _extract_quoted_message_id(inner)

        if "viewOnceMessage" in msg_unwrapped:
            inner = msg_unwrapped.get("viewOnceMessage", {}).get("message", {}) or {}
            return _extract_quoted_message_id(inner)

        if "viewOnceMessageV2" in msg_unwrapped:
            inner = msg_unwrapped.get("viewOnceMessageV2", {}).get("message", {}) or {}
            return _extract_quoted_message_id(inner)

        if "viewOnceMessageV2Extension" in msg_unwrapped:
            inner = msg_unwrapped.get("viewOnceMessageV2Extension", {}).get("message", {}) or {}
            return _extract_quoted_message_id(inner)

    except Exception as e:
        print("EXTRACT_QUOTED_MESSAGE_ID_ERROR =", str(e), flush=True)

    return ""


def _is_admin(requester_wa_id: str, from_me: bool = False) -> bool:
    raw = settings.ADMIN_PHONE or ""

    admins = [
        x.strip().replace("+", "").replace(" ", "")
        for x in raw.split(",")
        if x.strip()
    ]

    requester = (requester_wa_id or "")
    requester = requester.split("@")[0]
    requester = requester.replace("+", "").replace(" ", "").strip()

    return from_me or requester in admins
    

def _reply_to_origin(source_group_id: str | None, requester_wa_id: str, text: str, instance_name: str = None):
    if source_group_id:
        send_group_text(source_group_id, text, instance_name=instance_name)
    else:
        send_text(requester_wa_id, text, instance_name=instance_name)


def _all_provider_groups() -> set[str]:
    vals = {
        settings.PROVIDER_GROUP_NACIMIENTO_1,
        settings.PROVIDER_GROUP_NACIMIENTO_2,
        settings.PROVIDER_GROUP_NACIMIENTO_3,
        settings.PROVIDER_GROUP_NACIMIENTO_4,
        settings.PROVIDER_GROUP_ESPECIALES,
        settings.PROVIDER_GROUP_FOLIADAS,
        settings.PROVIDER_GROUP_CADENA,
        settings.PROVIDER2_GROUP_1,
        settings.PROVIDER2_GROUP_2,
        settings.PROVIDER5_GROUP_NACIMIENTO,
        settings.PROVIDER5_GROUP_ESPECIALES,
        settings.PROVIDER6_GROUP_1_NACIMIENTO,
        settings.PROVIDER6_GROUP_2_NACIMIENTO,
        settings.PROVIDER6_GROUP_ESPECIALES,
        settings.PROVIDER6_GROUP_FOLIADAS,
        settings.PROVIDER8_GROUP_1,
        settings.PROVIDER8_GROUP_2,
        settings.PROVIDER9_GROUP_1,
        settings.PROVIDER9_GROUP_2,
        settings.PROVIDER12_GROUP_NACIMIENTO,
        settings.PROVIDER12_GROUP_ESPECIALES,
        settings.PROVIDER13_GROUP_NACIMIENTO_1,
        settings.PROVIDER13_GROUP_FOLIO,
        settings.PROVIDER13_GROUP_NACIMIENTO_2,
        settings.PROVIDER13_GROUP_CADENA,
        settings.PROVIDER13_GROUP_ESPECIALES,
        settings.PROVIDER14_PRIVATE_JID,
        settings.MAYAPROVIDER_GROUP_1,
        settings.MAYAPROVIDER_GROUP_2,
    }
    return {v.strip() for v in vals if v and v.strip()}


def _get_or_create_provider(db: Session, provider_name: str, default_enabled: bool):
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


def _get_group_promotion(db: Session, group_jid: str) -> GroupPromotion | None:
    if not group_jid:
        return None

    rows = (
        db.query(GroupPromotion)
        .filter(GroupPromotion.group_jid == group_jid)
        .order_by(GroupPromotion.updated_at.desc(), GroupPromotion.id.desc())
        .all()
    )

    for row in rows:
        promo_name = (row.promo_name or "").strip()
        total_actas = int(row.total_actas or 0)
        used_actas = int(row.used_actas or 0)

        if not promo_name and total_actas == 0 and used_actas == 0:
            continue

        return row

    return None


def _promo_client_key(group_jid: str | None, promo_name: str | None = None, client_key: str | None = None) -> str:
    return (client_key or promo_name or group_jid or "").strip().upper()


def _notify_client_groups_main(rows: list, message: str):
    sent = set()
    for row in rows:
        gid = (row.group_jid or "").strip()
        if gid and gid not in sent:
            try:
                send_group_text(gid, message)
                sent.add(gid)
            except Exception as e:
                print("PROMO_NOTIFY_GROUP_ERROR =", gid, str(e), flush=True)


def _unblock_client_groups_main(rows: list):
    for row in rows:
        gid = (row.group_jid or "").strip()
        if gid:
            try:
                unblock_group(gid)
            except Exception as e:
                print("PROMO_AUTO_UNBLOCK_ERROR =", gid, str(e), flush=True)


def _promotion_available(promo: GroupPromotion) -> int:
    return max(0, (promo.total_actas or 0) - (promo.used_actas or 0))


def _real_promo_used_count(db: Session, promo: GroupPromotion) -> int:
    return int(promo.used_actas or 0) if promo else 0


def _sync_promo_used_from_logs(db: Session, promo: GroupPromotion):
    return promo


def _promotion_badge_html(promo: GroupPromotion | None) -> str:
    if not promo:
        return '<span style="color:#6b7280;font-weight:700;">Sin promoción</span>'

    promo_name = (promo.promo_name or "").strip()
    total_actas = int(promo.total_actas or 0)
    used_actas = int(promo.used_actas or 0)

    if not promo_name and total_actas == 0 and used_actas == 0:
        return '<span style="color:#6b7280;font-weight:700;">Sin promoción</span>'

    available = max(0, total_actas - used_actas)

    if available <= 0:
        color = "#991b1b"
        bg = "#fee2e2"
        label = f"Agotada · {available} disponibles"
    elif available <= 10:
        color = "#991b1b"
        bg = "#fee2e2"
        label = f"Crítico · {available} disponibles"
    elif available <= 50:
        color = "#92400e"
        bg = "#fef3c7"
        label = f"Precaución · {available} disponibles"
    elif available <= 100:
        color = "#92400e"
        bg = "#fef3c7"
        label = f"Bajo · {available} disponibles"
    else:
        color = "#166534"
        bg = "#dcfce7"
        label = f"Activa · {available} disponibles"

    return (
        f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;'
        f'font-weight:800;font-size:.82rem;color:{color};background:{bg};">{label}</span>'
    )


def _get_app_setting(db: Session, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if not row or row.value is None:
        return default
    return row.value.strip()


def _set_app_setting(db: Session, key: str, value: str):
    row = db.query(AppSetting).filter(AppSetting.key == key).first()

    if row:
        row.value = value
        row.updated_at = _utc_now_naive()
    else:
        row = AppSetting(
            key=key,
            value=value,
            updated_at=_utc_now_naive(),
        )
        db.add(row)

    db.commit()
    return row


def _cleanup_enabled(db: Session) -> bool:
    value = _get_app_setting(db, CLEANUP_ENABLED_KEY, "1")
    return str(value).strip().lower() in {"1", "true", "yes", "si", "sí", "on", "enabled"}


def _cleanup_max_age_minutes(db: Session) -> int:
    raw = _get_app_setting(
        db,
        CLEANUP_MAX_AGE_MINUTES_KEY,
        str(CLEANUP_DEFAULT_MAX_AGE_MINUTES),
    )

    try:
        value = int(float(str(raw).strip()))
    except Exception:
        value = CLEANUP_DEFAULT_MAX_AGE_MINUTES

    return max(1, min(value, 1440))


def _rq_job_has_request_id(job, request_ids: set[int]) -> bool:
    try:
        args = list(getattr(job, "args", None) or [])
        kwargs = dict(getattr(job, "kwargs", None) or {})

        candidates = []

        for arg in args:
            candidates.append(arg)

        for key in ("request_id", "req_id", "id"):
            if key in kwargs:
                candidates.append(kwargs.get(key))

        for item in candidates:
            try:
                if int(item) in request_ids:
                    return True
            except Exception:
                continue

        return False

    except Exception:
        return False


def _delete_rq_jobs_for_request_ids(request_ids: list[int]) -> dict:
    """
    Elimina jobs pendientes/diferidos/programados/fallidos de RQ que traigan
    como argumento el request_id. Esto limpia Redis/RQ.

    Nota: si un job ya está ejecutándose dentro de un worker, borrar metadata
    de RQ no mata el proceso Python en curso. Para esos casos, al borrar la fila
    de RequestLog el worker ya no debería poder continuar normalmente.
    """
    ids_set = {int(x) for x in request_ids if x}
    result = {
        "checked": 0,
        "deleted": 0,
        "errors": [],
    }

    if not ids_set:
        return result

    queues = [
        request_queue,
        slow_request_queue,
    ]

    registry_classes = [
        StartedJobRegistry,
        DeferredJobRegistry,
        ScheduledJobRegistry,
        FailedJobRegistry,
    ]

    for queue in queues:
        job_ids = set()

        try:
            job_ids.update(queue.job_ids or [])
        except Exception as e:
            result["errors"].append(f"{getattr(queue, 'name', 'queue')}:job_ids:{e}")

        for registry_cls in registry_classes:
            try:
                registry = registry_cls(queue=queue)
                job_ids.update(registry.get_job_ids() or [])
            except Exception as e:
                result["errors"].append(
                    f"{getattr(queue, 'name', 'queue')}:{registry_cls.__name__}:{e}"
                )

        for job_id in job_ids:
            try:
                result["checked"] += 1
                job = queue.fetch_job(job_id)

                if not job:
                    continue

                if not _rq_job_has_request_id(job, ids_set):
                    continue

                job.delete()
                result["deleted"] += 1

            except Exception as e:
                result["errors"].append(f"{getattr(queue, 'name', 'queue')}:{job_id}:{e}")

    return result


def _cleanup_stuck_candidates(db: Session, older_than_minutes: int) -> list[RequestLog]:
    cutoff = _utc_now_naive() - timedelta(minutes=older_than_minutes)

    return (
        db.query(RequestLog)
        .filter(
            RequestLog.status.in_(["QUEUED", "PROCESSING"]),
            RequestLog.created_at < cutoff,
        )
        .order_by(RequestLog.created_at.asc(), RequestLog.id.asc())
        .all()
    )


@app.get("/panel/cleanup/status")
def panel_cleanup_status(
    request: Request,
    older_than_minutes: int | None = None,
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    max_age_minutes = _cleanup_max_age_minutes(db)
    check_minutes = older_than_minutes or max_age_minutes
    check_minutes = max(1, min(int(check_minutes), 1440))

    rows = _cleanup_stuck_candidates(db, check_minutes)

    return {
        "ok": True,
        "enabled": _cleanup_enabled(db),
        "max_age_minutes": max_age_minutes,
        "check_minutes": check_minutes,
        "stuck_count": len(rows),
        "items": [
            {
                "id": r.id,
                "status": r.status,
                "curp": r.curp,
                "act_type": r.act_type,
                "provider_name": r.provider_name,
                "instance_name": r.instance_name,
                "created_at": str(r.created_at),
                "updated_at": str(r.updated_at),
                "error_message": r.error_message,
            }
            for r in rows[:80]
        ],
    }


@app.post("/panel/cleanup/settings")
def panel_cleanup_settings(
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    enabled = bool(payload.get("enabled"))
    max_age_raw = payload.get("max_age_minutes")

    try:
        max_age_minutes = int(float(max_age_raw))
    except Exception:
        return {"ok": False, "error": "MINUTOS_INVALIDOS"}

    if max_age_minutes < 1 or max_age_minutes > 1440:
        return {"ok": False, "error": "El rango permitido es de 1 a 1440 minutos"}

    _set_app_setting(db, CLEANUP_ENABLED_KEY, "1" if enabled else "0")
    _set_app_setting(db, CLEANUP_MAX_AGE_MINUTES_KEY, str(max_age_minutes))

    _clear_panel_cache()

    return {
        "ok": True,
        "enabled": enabled,
        "max_age_minutes": max_age_minutes,
    }


@app.post("/panel/cleanup/purge-stuck")
def panel_cleanup_purge_stuck(
    request: Request,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return {"ok": False, "error": "UNAUTHORIZED"}

    try:
        older_than_minutes = int(float(payload.get("older_than_minutes")))
    except Exception:
        return {"ok": False, "error": "MINUTOS_INVALIDOS"}

    if older_than_minutes < 1 or older_than_minutes > 1440:
        return {"ok": False, "error": "El rango permitido es de 1 a 1440 minutos"}

    rows = _cleanup_stuck_candidates(db, older_than_minutes)
    request_ids = [int(r.id) for r in rows if r.id]

    rq_result = _delete_rq_jobs_for_request_ids(request_ids)

    deleted_db = 0

    if request_ids:
        deleted_db = (
            db.query(RequestLog)
            .filter(RequestLog.id.in_(request_ids))
            .delete(synchronize_session=False)
        )
        db.commit()

    _clear_panel_cache()

    print(
        "PANEL_CLEANUP_PURGE_STUCK =",
        {
            "older_than_minutes": older_than_minutes,
            "request_ids": request_ids,
            "deleted_db": deleted_db,
            "rq_result": rq_result,
        },
        flush=True,
    )

    return {
        "ok": True,
        "older_than_minutes": older_than_minutes,
        "found": len(request_ids),
        "deleted_db": int(deleted_db or 0),
        "rq": rq_result,
        "request_ids": request_ids[:200],
    }


def _bot_manager_name_key(instance_name: str) -> str:
    inst = _norm_instance(instance_name)

    if not inst:
        raise ValueError("Instancia vacía")

    return f"{BOT_MANAGER_NAME_KEY_PREFIX}{inst}"


def _bot_manager_price_key(instance_name: str) -> str:
    inst = _norm_instance(instance_name)

    if not inst:
        raise ValueError("Instancia vacía")

    return f"{BOT_MANAGER_PRICE_KEY_PREFIX}{inst}"


def _get_bot_manager_data(db: Session, instance_name: str) -> dict:
    return {
        "manager_name": _get_app_setting(
            db,
            _bot_manager_name_key(instance_name),
            "",
        ),
        "manager_price": _get_app_setting(
            db,
            _bot_manager_price_key(instance_name),
            "",
        ),
    }


def _providers_status_text(db: Session) -> str:
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

    s1 = "ON" if p1.is_enabled else "OFF"
    s2 = "ON" if p2.is_enabled else "OFF"
    s3 = "ON" if p3.is_enabled else "OFF"
    s4 = "ON" if p4.is_enabled else "OFF"
    s5 = "ON" if p5.is_enabled else "OFF"
    s6 = "ON" if p6.is_enabled else "OFF"
    s7 = "ON" if p7.is_enabled else "OFF"
    s8 = "ON" if p8.is_enabled else "OFF"
    s9 = "ON" if p9.is_enabled else "OFF"
    s10 = "ON" if p10.is_enabled else "OFF"
    s11 = "ON" if p11.is_enabled else "OFF"
    s12 = "ON" if p12.is_enabled else "OFF"
    s13 = "ON" if p13.is_enabled else "OFF"
    s14 = "ON" if p14.is_enabled else "OFF"

    provider1_extra = ""
    provider2_extra = ""
    provider3_extra = ""
    provider4_extra = ""
    provider5_extra = ""
    provider6_extra = ""
    provider7_extra = ""
    provider8_extra = ""
    provider9_extra = ""
    provider10_extra = ""
    provider11_extra = ""
    provider12_extra = ""
    provider13_extra = ""
    provider14_extra = ""

    local_start = _panel_month_start()
    local_end = _panel_month_end()
    utc_start = _panel_to_utc_naive(local_start)
    utc_end = _panel_to_utc_naive(local_end)

    try:
        provider1_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER1",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider1_extra = f" | CURP y CADENA hechas: {provider1_total}"
    except Exception as e:
        provider1_extra = f" | ERROR DB: {str(e)}"

    try:
        provider2_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER2",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider2_extra = f" | CURP hechas: {provider2_total}"
    except Exception as e:
        provider2_extra = f" | ERROR DB: {str(e)}"

    cached = _cache_get_json("panel:providers_status_cached") or {}

    p3_cached = cached.get("provider3", {})
    p4_cached = cached.get("provider4", {})
    p10_cached = cached.get("provider10", {})
    p11_cached = cached.get("provider11", {})

    if p3.is_enabled:
        if p3_cached.get("error"):
            provider3_extra = f" | ERROR: {p3_cached.get('error')}"
        else:
            curp_left = p3_cached.get("curp")
            cadena_left = p3_cached.get("cadena")
            provider3_extra = (
                f" | CURP restantes: {curp_left if curp_left is not None else 'N/D'}"
                f" | CADENA restantes: {cadena_left if cadena_left is not None else 'N/D'}"
            )
    
    if p4.is_enabled:
        if p4_cached.get("error"):
            provider4_extra = f" | ERROR: {p4_cached.get('error')}"
        else:
            total_done = p4_cached.get("total")
            provider4_extra = (
                f" | CURP hechas: {total_done if total_done is not None else 'N/D'}"
            )

    try:
        provider5_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER5",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider5_extra = f" | CURP y CADENA hechas: {provider5_total}"
    except Exception as e:
        provider5_extra = f" | ERROR DB: {str(e)}"

    try:
        provider6_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER6",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider6_extra = f" | CURP hechas: {provider6_total}"
    except Exception as e:
        provider6_extra = f" | ERROR DB: {str(e)}"

    try:
        provider7_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER7",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider7_extra = f" | CURP hechas: {provider7_total}"
    except Exception as e:
        provider7_extra = f" | ERROR DB: {str(e)}"

    try:
        provider8_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER8",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider8_extra = f" | CURP y CADENA hechas: {provider8_total}"
    except Exception as e:
        provider8_extra = f" | ERROR DB: {str(e)}"

    try:
        provider9_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER9",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider9_extra = f" | CURP y CADENA hechas: {provider9_total}"
    except Exception as e:
        provider9_extra = f" | ERROR DB: {str(e)}"

    if p10.is_enabled:
        if p10_cached.get("error"):
            provider10_extra = f" | ERROR: {p10_cached.get('error')}"
        else:
            total_done = p10_cached.get("total")
            provider10_extra = (
                f" | CURP hechas: {total_done if total_done is not None else 'N/D'}"
            )

    if p11.is_enabled:
        if p11_cached.get("error"):
            provider11_extra = f" | ERROR: {p11_cached.get('error')}"
        else:
            total_done = p11_cached.get("total")
            provider11_extra = (
                f" | CURP hechas: {total_done if total_done is not None else 'N/D'}"
            )

    try:
        provider12_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER12",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider12_extra = f" | CURP hechas: {provider12_total}"
    except Exception as e:
        provider12_extra = f" | ERROR DB: {str(e)}"

    try:
        provider13_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER13",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider13_extra = f" | CURP hechas: {provider13_total}"
    except Exception as e:
        provider13_extra = f" | ERROR DB: {str(e)}"

    try:
        provider14_total = (
            db.query(func.count(RequestLog.id))
            .filter(
                RequestLog.provider_name == "PROVIDER14",
                RequestLog.status == "DONE",
                RequestLog.created_at >= utc_start,
                RequestLog.created_at < utc_end,
            )
            .scalar()
        ) or 0
        provider14_extra = f" | CURP hechas: {provider14_total}"
    except Exception as e:
        provider14_extra = f" | ERROR DB: {str(e)}"

    text = (
        f"ESCALANTE:      {s6}{provider6_extra}\n"
        f"ADMIN:          {s1}{provider1_extra}\n"
        f"LUIS:           {s5}{provider5_extra}\n"
        f"VILLAFUERTE:    {s12}{provider12_extra}\n"
        f"RL:             {s13}{provider13_extra}\n"
        f"E-BOT:          {s14}{provider14_extra}\n"
        f"EMILIANO:       {s9}{provider9_extra}\n"
        f"LAZARO 1:       {s4}{provider4_extra}\n"
        f"LAZARO 2:       {s10}{provider10_extra}\n"
        f"LAZARO 3:       {s11}{provider11_extra}"
    )

    return text


def _resolve_requester_wa_id(data: dict, key: dict, is_group: bool) -> str:
    participant = key.get("participant", "") or ""
    remote_jid = key.get("remoteJid", "") or ""

    # Campos alternos que a veces manda Evolution
    participant_alt = data.get("participantAlt", "") or ""
    remote_jid_alt = data.get("remoteJidAlt", "") or ""
    sender = data.get("sender", "") or ""

    # 1) En grupo, intenta primero participantAlt si existe
    if is_group and participant_alt:
        return _normalize_wa_actor(participant_alt)

    # 2) Luego sender
    if sender:
        return _normalize_wa_actor(sender)

    # 3) Luego participant
    if is_group and participant and not participant.endswith("@lid"):
        return _normalize_wa_actor(participant)

    # 4) Luego remote_jid_alt
    if remote_jid_alt:
        return _normalize_wa_actor(remote_jid_alt)

    # 5) Finalmente remote_jid
    return _normalize_wa_actor(remote_jid)


def webhook_msg_seen(msg_id: str, instance_name: str | None = None) -> bool:
    if not msg_id:
        return False

    # Dedupe global por msg_id.
    # Antes usaba instance_name + msg_id, pero Evolution puede mandar
    # el mismo mensaje por más de una instancia y eso duplicaba procesos.
    global_key = f"wa:webhook:msg:{msg_id}"
    created_global = redis_conn.set(global_key, "1", ex=300, nx=True)

    if not created_global:
        return True

    # Llave secundaria solo para diagnóstico/compatibilidad.
    try:
        inst = (instance_name or "default").strip()
        redis_conn.set(f"wa:webhook:msg_instance:{inst}:{msg_id}", "1", ex=300, nx=True)
    except Exception:
        pass

    return False


def release_webhook_msg_seen(
    msg_id: str,
    instance_name: str | None = None,
):
    """
    Libera el dedupe cuando el webhook falló
    por una causa transitoria.
    """

    if not msg_id:
        return

    try:
        redis_conn.delete(
            f"wa:webhook:msg:{msg_id}"
        )

        instance = (
            instance_name
            or "default"
        ).strip()

        redis_conn.delete(
            f"wa:webhook:msg_instance:{instance}:{msg_id}"
        )

        print(
            "WEBHOOK_MSG_DEDUPE_RELEASED =",
            {
                "msg_id": msg_id,
                "instance_name": instance,
            },
            flush=True,
        )

    except Exception as exc:
        print(
            "WEBHOOK_MSG_DEDUPE_RELEASE_ERROR =",
            {
                "msg_id": msg_id,
                "instance_name": instance_name,
                "error": str(exc),
            },
            flush=True,
        )


def block_all_client_groups():
    excluded_words = ("PROV", "PRUEBA", "PRUEBAS", "TEST")

    for gid, name in GROUP_NAME_MAP.items():
        name_up = (name or "").strip().upper()
        if any(word in name_up for word in excluded_words):
            continue
        redis_conn.sadd(BLOCKED_GROUPS_KEY, gid)


def unblock_all_client_groups():
    excluded_words = ("PROV", "PRUEBA", "PRUEBAS", "TEST")

    for gid, name in GROUP_NAME_MAP.items():
        name_up = (name or "").strip().upper()
        if any(word in name_up for word in excluded_words):
            continue
        redis_conn.srem(BLOCKED_GROUPS_KEY, gid)


def are_all_client_groups_blocked() -> bool:
    excluded_words = ("PROV", "PRUEBA", "PRUEBAS", "TEST")
    client_groups = []

    for gid, name in GROUP_NAME_MAP.items():
        name_up = (name or "").strip().upper()
        if any(word in name_up for word in excluded_words):
            continue
        client_groups.append(gid)

    if not client_groups:
        return False

    return all(is_group_blocked(gid) for gid in client_groups)


def list_blocked_groups() -> list[str]:
    values = redis_conn.smembers(BLOCKED_GROUPS_KEY) or set()
    out = []
    for v in values:
        if isinstance(v, bytes):
            out.append(v.decode("utf-8", errors="ignore"))
        else:
            out.append(str(v))
    out.sort()
    return out
    

@app.post("/panel/group/{group_jid}/name")
def panel_set_group_name(
    group_jid: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    custom_name = (payload.get("custom_name") or "").strip()

    if not custom_name:
        return {"ok": False, "error": "NAME_REQUIRED"}

    row = db.query(GroupAlias).filter(GroupAlias.group_jid == group_jid).first()

    if row:
        row.custom_name = custom_name
        row.updated_at = _utc_now_naive()
    else:
        row = GroupAlias(
            group_jid=group_jid,
            custom_name=custom_name
        )
        db.add(row)

    db.commit()
    _clear_panel_cache()
    _clear_group_name_cache()

    return {"ok": True}


@app.post("/panel/group/{group_jid}/promotion")
def panel_set_group_promotion(
    group_jid: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    total_actas = int(payload.get("total_actas") or 0)
    promo_name = (payload.get("promo_name") or "").strip()
    price_per_piece = (payload.get("price_per_piece") or "").strip()

    is_credit = bool(payload.get("is_credit") or False)
    credit_abono = int(payload.get("credit_abono") or 0)
    credit_debe = int(payload.get("credit_debe") or 0)

    if total_actas <= 0:
        return {"ok": False, "error": "TOTAL_ACTAS_INVALID"}

    row = db.query(GroupPromotion).filter(GroupPromotion.group_jid == group_jid).first()

    if row:
        row.promo_name = promo_name or row.promo_name
        row.total_actas = total_actas
        row.price_per_piece = price_per_piece
        row.is_credit = is_credit
        row.credit_abono = credit_abono
        row.credit_debe = credit_debe
        row.is_active = True
        row.updated_at = _utc_now_naive()

        row.used_actas = 0
        row.warning_sent_200 = False
        row.warning_sent_100 = False
        row.warning_sent_50 = False
        row.warning_sent_10 = False
        row.warning_sent_0 = False

        row.client_key = None
        row.shared_key = None

    else:
        row = GroupPromotion(
            group_jid=group_jid,
            promo_name=promo_name,
            total_actas=total_actas,
            used_actas=0,
            price_per_piece=price_per_piece,
            is_credit=is_credit,
            credit_abono=credit_abono,
            credit_debe=credit_debe,
            warning_sent_200=False,
            warning_sent_100=False,
            warning_sent_50=False,
            warning_sent_10=False,
            warning_sent_0=False,
            is_active=True,
            client_key=None,
            shared_key=None,
            created_at=_utc_now_naive(),
            updated_at=_utc_now_naive(),
        )
        db.add(row)
        db.flush()

    available = max(0, (row.total_actas or 0) - (row.used_actas or 0))

    db.commit()

    try:
        redis_conn.delete(f"promo_notify:{group_jid}:0")
        redis_conn.delete(f"promo_notify:{group_jid}:10")
        redis_conn.delete(f"promo_notify:{group_jid}:50")
        redis_conn.delete(f"promo_notify:{group_jid}:100")
        redis_conn.delete(f"promo_notify:{group_jid}:200")
    except Exception as e:
        print("PROMO_NOTIFY_KEYS_CLEAR_ERROR =", str(e), flush=True)

    try:
        unblock_group(group_jid)
    except Exception as unblock_exc:
        print("PROMOTION_AUTO_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    try:
        promo_label = promo_name or "paquete promocional"
        tipo_label = "crédito" if is_credit else "pagada"

        send_group_text(
            group_jid,
            (
                f"✅ *Promoción activada*\n\n"
                f"Tu *{promo_label}* ya fue activado correctamente.\n"
                f"Tipo: *{tipo_label}*\n"
                f"Cuentas con *{available} actas disponibles*.\n\n"
                f"Gracias por tu preferencia."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_ACTIVATION_NOTIFY_ERROR =", str(notify_exc), flush=True)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Promoción guardada correctamente",
        "group_jid": group_jid,
        "total_actas": row.total_actas,
        "used_actas": row.used_actas,
        "available": available,
        "is_credit": row.is_credit,
        "credit_abono": row.credit_abono,
        "credit_debe": row.credit_debe,
    }


@app.post("/panel/group/{group_jid}/promotion/remove")
def panel_remove_group_promotion(
    group_jid: str,
    db: Session = Depends(get_db),
):
    row = db.query(GroupPromotion).filter(GroupPromotion.group_jid == group_jid).first()

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    row.is_active = False
    row.used_actas = 0
    row.total_actas = 0
    row.promo_name = ""
    row.price_per_piece = ""
    row.client_key = None
    row.shared_key = None
    row.credit_abono = 0
    row.credit_debe = 0
    row.warning_sent_200 = False
    row.warning_sent_100 = False
    row.warning_sent_50 = False
    row.warning_sent_10 = False
    row.warning_sent_0 = False
    row.updated_at = _utc_now_naive()

    db.commit()

    try:
        unblock_group(group_jid)
    except Exception as unblock_exc:
        print("PROMOTION_REMOVE_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    try:
        redis_conn.delete(f"promo_notify:{group_jid}:0")
        redis_conn.delete(f"promo_notify:{group_jid}:10")
        redis_conn.delete(f"promo_notify:{group_jid}:50")
        redis_conn.delete(f"promo_notify:{group_jid}:100")
        redis_conn.delete(f"promo_notify:{group_jid}:200")
    except Exception as redis_exc:
        print("PROMOTION_REMOVE_REDIS_CLEAR_ERROR =", str(redis_exc), flush=True)

    try:
        send_group_text(
            group_jid,
            (
                f"⛔ *Promoción desactivada*\n\n"
                f"Tu paquete promocional ha sido desactivado por administración.\n\n"
                f"Para reactivar el servicio será necesaria una nueva activación o recarga."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_REMOVE_NOTIFY_ERROR =", str(notify_exc), flush=True)

    _clear_panel_cache()
    return {
        "ok": True,
        "message": "Promoción desactivada correctamente",
        "group_jid": group_jid,
    }


@app.post("/panel/group/{group_jid}/promotion/recharge")
def panel_recharge_group_promotion(
    group_jid: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    extra_actas = int(payload.get("extra_actas") or 0)

    if extra_actas <= 0:
        return {"ok": False, "error": "EXTRA_ACTAS_INVALID"}

    row = db.query(GroupPromotion).filter(GroupPromotion.group_jid == group_jid).first()

    if not row:
        return {"ok": False, "error": "PROMOTION_NOT_FOUND"}

    shared_key = (row.shared_key or "").strip()

    # =========================
    # RECARGA COMPARTIDA
    # =========================
    if shared_key:
        rows = (
            db.query(GroupPromotion)
            .filter(GroupPromotion.shared_key == shared_key)
            .all()
        )

        if not rows:
            return {"ok": False, "error": "SHARED_PROMOTION_NOT_FOUND"}

        leader = rows[0]
        current_total = int(leader.total_actas or 0)
        current_used = int(leader.used_actas or 0)

        new_total = current_total + extra_actas
        available = max(0, new_total - current_used)

        for r in rows:
            r.total_actas = new_total
            r.used_actas = current_used
            r.warning_sent_200 = False
            r.warning_sent_100 = False
            r.warning_sent_50 = False
            r.warning_sent_10 = False
            r.warning_sent_0 = False
            r.is_active = True
            r.updated_at = _utc_now_naive()

        db.commit()

        try:
            redis_conn.delete(f"promo_notify:{shared_key}:0")
            redis_conn.delete(f"promo_notify:{shared_key}:10")
            redis_conn.delete(f"promo_notify:{shared_key}:50")
            redis_conn.delete(f"promo_notify:{shared_key}:100")
            redis_conn.delete(f"promo_notify:{shared_key}:200")
        except Exception as e:
            print("PROMO_NOTIFY_KEYS_CLEAR_SHARED_ERROR =", str(e), flush=True)

        try:
            notified = set()
            for r in rows:
                gid = (r.group_jid or "").strip()
                if gid:
                    try:
                        unblock_group(gid)
                    except Exception as unblock_exc:
                        print("PROMOTION_RECHARGE_UNBLOCK_SHARED_ERROR =", gid, str(unblock_exc), flush=True)

                    if gid not in notified:
                        try:
                            send_group_text(
                                gid,
                                (
                                    f"🔄 *Recarga aplicada a bolsa compartida*\n\n"
                                    f"Bolsa: *{shared_key}*\n"
                                    f"Se agregaron *{extra_actas} actas*.\n"
                                    f"Ahora cuentan con *{available} actas disponibles*.\n\n"
                                    f"Gracias por tu preferencia."
                                )
                            )
                            notified.add(gid)
                        except Exception as notify_exc:
                            print("PROMOTION_RECHARGE_SHARED_NOTIFY_ERROR =", gid, str(notify_exc), flush=True)
        except Exception as e:
            print("PROMOTION_RECHARGE_SHARED_GENERAL_ERROR =", str(e), flush=True)

        _clear_panel_cache()
        return {
            "ok": True,
            "message": f"Recarga aplicada a la bolsa compartida. Nuevo saldo disponible: {available}",
            "group_jid": group_jid,
            "shared_key": shared_key,
            "total_actas": new_total,
            "used_actas": current_used,
            "available": available,
        }

    # =========================
    # RECARGA INDIVIDUAL
    # =========================
    row.total_actas = (row.total_actas or 0) + extra_actas
    row.used_actas = row.used_actas or 0
    row.warning_sent_200 = False
    row.warning_sent_100 = False
    row.warning_sent_50 = False
    row.warning_sent_10 = False
    row.warning_sent_0 = False
    row.is_active = True
    row.updated_at = _utc_now_naive()

    available = max(0, (row.total_actas or 0) - (row.used_actas or 0))

    db.commit()

    try:
        redis_conn.delete(f"promo_notify:{group_jid}:0")
        redis_conn.delete(f"promo_notify:{group_jid}:10")
        redis_conn.delete(f"promo_notify:{group_jid}:50")
        redis_conn.delete(f"promo_notify:{group_jid}:100")
        redis_conn.delete(f"promo_notify:{group_jid}:200")
    except Exception as e:
        print("PROMO_NOTIFY_KEYS_CLEAR_ERROR =", str(e), flush=True)

    try:
        unblock_group(group_jid)
    except Exception as unblock_exc:
        print("PROMOTION_RECHARGE_UNBLOCK_ERROR =", str(unblock_exc), flush=True)

    try:
        send_group_text(
            group_jid,
            (
                f"🔄 *Recarga aplicada*\n\n"
                f"Tu paquete promocional fue recargado correctamente.\n"
                f"Ahora cuentas con *{available} actas disponibles*.\n\n"
                f"Gracias por tu preferencia."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_RECHARGE_NOTIFY_ERROR =", str(notify_exc), flush=True)

    return {
        "ok": True,
        "message": f"Recarga aplicada. Nuevo saldo disponible: {available}",
        "group_jid": group_jid,
        "total_actas": row.total_actas,
        "used_actas": row.used_actas,
        "available": available,
    }


@app.post("/panel/groups/toggle-all")
def panel_toggle_all_groups():
    blocked = are_all_client_groups_blocked()

    if blocked:
        unblock_all_client_groups()
        return {"ok": True, "blocked": False, "message": "Todos los grupos fueron desbloqueados"}
    else:
        block_all_client_groups()
        return {"ok": True, "blocked": True, "message": "Todos los grupos fueron bloqueados"}


@app.post("/panel/group/{group_jid}/block")
def panel_block_group(group_jid: str):
    print("PANEL_BLOCK_GROUP =", group_jid, flush=True)
    block_group(group_jid)
    _clear_panel_cache()
    return {"ok": True, "group_jid": group_jid, "blocked": True}


@app.post("/panel/group/{group_jid}/unblock")
def panel_unblock_group(group_jid: str):
    print("PANEL_UNBLOCK_GROUP =", group_jid, flush=True)
    unblock_group(group_jid)
    _clear_panel_cache()
    return {"ok": True, "group_jid": group_jid, "blocked": False}


@app.get("/panel/groups/blocked")
def panel_blocked_groups():
    rows = []
    for gid in list_blocked_groups():
        rows.append({
            "group_jid": gid,
            "group_name": _group_name(gid),
        })
    return {"ok": True, "items": rows}


def _unwrap_message(msg: dict) -> dict:
    current = msg or {}

    while isinstance(current, dict):
        if "documentMessage" in current:
            return current

        if "documentWithCaptionMessage" in current:
            inner = current.get("documentWithCaptionMessage", {})
            current = inner.get("message", {}) or {}
            continue

        if "ephemeralMessage" in current:
            inner = current.get("ephemeralMessage", {})
            current = inner.get("message", {}) or {}
            continue

        if "viewOnceMessage" in current:
            inner = current.get("viewOnceMessage", {})
            current = inner.get("message", {}) or {}
            continue

        if "viewOnceMessageV2" in current:
            inner = current.get("viewOnceMessageV2", {})
            current = inner.get("message", {}) or {}
            continue

        if "viewOnceMessageV2Extension" in current:
            inner = current.get("viewOnceMessageV2Extension", {})
            current = inner.get("message", {}) or {}
            continue

        if "editedMessage" in current:
            inner = current.get("editedMessage", {})
            current = inner.get("message", {}) or {}
            continue

        break

    return current
    

def _get_latest_request(
    db: Session,
    term: str,
    act_type: str,
    source_chat_id: str | None,
):
    day_start, day_end = _bot_day_bounds()

    return (
        db.query(RequestLog)
        .filter(
            RequestLog.curp == term,
            RequestLog.act_type == act_type,
            RequestLog.source_chat_id == source_chat_id,
            RequestLog.created_at >= day_start,
            RequestLog.created_at < day_end,
        )
        .order_by(RequestLog.created_at.desc(), RequestLog.id.desc())
        .first()
    )


def is_legacy_known_group(db: Session, group_jid: str) -> bool:
    try:
        if not group_jid:
            return False

        # 1) Si ya está autorizado formalmente, claro que cuenta
        if is_authorized_group(db, group_jid):
            return True

        # 2) Si está bloqueado, significa que el sistema ya lo conoce
        if is_group_blocked(group_jid):
            return True

        # 3) Si ya existe actividad previa en solicitudes, es grupo viejo conocido
        existing_req = (
            db.query(RequestLog.id)
            .filter(RequestLog.source_group_id == group_jid)
            .first()
        )
        if existing_req:
            return True

        # 4) Si tiene promoción asociada, también ya es conocido
        promo = (
            db.query(GroupPromotion.id)
            .filter(GroupPromotion.group_jid == group_jid)
            .first()
        )
        if promo:
            return True

        return False

    except Exception as e:
        print("is_legacy_known_group error =", str(e), flush=True)
        return False


BOT_AUTO_MESSAGES_PREFIXES = (
    "⚠️ La CURP parece incompleta o incorrecta.",
    "⚠️ No pude identificar una CURP",
    "✅ Esta acta ya fue entregada",
    "⏳ Ya existe una solicitud en proceso",
    "❌ No hay registros disponibles.",
    "❌ No se pudo procesar",
    "🔁 Reintentando solicitud",
    "🚀 DOCU EXPRES",
)

def is_bot_generated_text(text: str | None) -> bool:
    if not text:
        return False

    t = text.strip()
    return any(t.startswith(prefix) for prefix in BOT_AUTO_MESSAGES_PREFIXES)


def _webhook_message_has_processable_content(message) -> bool:
    """
    Evita consumir el dedupe con eventos messages.upsert vacíos
    o con actualizaciones que todavía no contienen el mensaje real.

    Se consideran procesables:
    - texto normal;
    - texto extendido;
    - documentos/PDF;
    - imágenes;
    - video/audio;
    - botones/listas;
    - mensajes encapsulados.
    """
    if not isinstance(message, dict) or not message:
        return False

    processable_keys = {
        "conversation",
        "extendedTextMessage",
        "documentMessage",
        "documentWithCaptionMessage",
        "imageMessage",
        "videoMessage",
        "audioMessage",
        "buttonsResponseMessage",
        "listResponseMessage",
        "templateButtonReplyMessage",
        "interactiveResponseMessage",
        "ephemeralMessage",
        "viewOnceMessage",
        "viewOnceMessageV2",
        "viewOnceMessageV2Extension",
    }

    return any(
        key in message and message.get(key) is not None
        for key in processable_keys
    )


@app.post("/webhook/evolution")
async def evolution_webhook(payload: dict, db: Session = Depends(get_db)):
    try:
        webhook_t0 = time.perf_counter()
        print(
            "WEBHOOK_TIMING_START =",
            {
                "ts": time.time(),
            },
            flush=True,
        )

        #print("WEBHOOK PAYLOAD =", payload, flush=True)
        event = payload.get("event", "")
        data = payload.get("data", {})
        
        instance_name = payload.get("instance", "default")
        print("WEBHOOK_INSTANCE =", instance_name, flush=True)
        print("WEBHOOK_IS_INSTANCE_BLOCKED =", is_instance_blocked(instance_name), flush=True)
        
        event_norm = str(event or "").strip().lower()

        if event_norm not in {"messages.upsert", "messages_upsert"}:
            print("WEBHOOK_EVENT_IGNORED =", repr(event), flush=True)
            return {"status": "ignored", "event": event}

        print("WEBHOOK_EVENT_ACCEPTED =", repr(event), flush=True)

        instance_name = (instance_name or "").strip()

        if is_instance_blocked(instance_name):
            print("IGNORED_REASON = instance_blocked_early", flush=True)
            print("BLOCKED_INSTANCE =", instance_name, flush=True)
            return {
                "ok": True,
                "ignored": "instance_blocked",
                "instance_name": instance_name
            }
                
        key = data.get("key", {})
        message = data.get("message", {})
        push_name = data.get("pushName", "")
        
        remote_jid = key.get("remoteJid", "")
        from_me = key.get("fromMe", False)
        participant = key.get("participant", "")
        msg_id = key.get("id", "")
        
        is_group = remote_jid.endswith("@g.us")
        source_chat_id = remote_jid
        source_group_id = remote_jid if is_group else None
        requester_wa_id = _resolve_requester_wa_id(data, key, is_group)

        print("ADMIN_DEBUG_REMOTE_JID =", remote_jid, flush=True)
        print("ADMIN_DEBUG_PARTICIPANT =", participant, flush=True)
        print("ADMIN_DEBUG_PARTICIPANT_ALT =", data.get("participantAlt", ""), flush=True)
        print("ADMIN_DEBUG_SENDER =", data.get("sender", ""), flush=True)
        print("ADMIN_DEBUG_REQUESTER_WA_ID =", requester_wa_id, flush=True)
        print("ADMIN_DEBUG_ADMIN_PHONES =", settings.ADMIN_PHONE, flush=True)
        
        text_body = ""
        
        if "conversation" in message:
            text_body = message.get("conversation", "")

        elif "extendedTextMessage" in message:
            text_body = (
                message
                .get("extendedTextMessage", {})
                .get("text", "")
            )
        
        text_upper = normalize_text(text_body)

        message_keys = (
            list(message.keys())
            if isinstance(message, dict)
            else []
        )

        print(
            "WEBHOOK_INPUT_AUDIT =",
            {
                "event": event,
                "instance_name": instance_name,
                "msg_id": msg_id,
                "remote_jid": remote_jid,
                "participant": participant,
                "from_me": from_me,
                "push_name": push_name,
                "message_keys": message_keys,
                "text": text_body[:180],
            },
            flush=True,
        )

        # Evolution puede emitir primero una variante vacía/incompleta
        # y posteriormente el mismo msg_id con el contenido real.
        # Una variante vacía NO debe consumir el deduplicador.
        if not _webhook_message_has_processable_content(message):
            print(
                "WEBHOOK_EMPTY_OR_INCOMPLETE_UPSERT_IGNORED =",
                {
                    "instance_name": instance_name,
                    "msg_id": msg_id,
                    "remote_jid": remote_jid,
                    "message_keys": message_keys,
                },
                flush=True,
            )

            return {
                "ok": True,
                "ignored": "empty_or_incomplete_upsert",
                "msg_id": msg_id,
            }

        # El dedupe se reclama solo después de confirmar
        # que existe contenido que el sistema puede procesar.
        if webhook_msg_seen(msg_id, instance_name):
            print(
                "IGNORED_DUPLICATE_PROCESSABLE_MSG =",
                {
                    "instance_name": instance_name,
                    "msg_id": msg_id,
                    "remote_jid": remote_jid,
                    "message_keys": message_keys,
                    "text": text_body[:180],
                },
                flush=True,
            )

            return {
                "ok": True,
                "ignored": "duplicate_msg_id",
                "msg_id": msg_id,
            }

        # =========================
        # BLOQUEO DE BUCLE ENTRE BOTS
        # =========================
        if from_me and not text_upper.startswith("/"):
            print("IGNORED_REASON = from_me_early", flush=True)
            return {"ok": True, "ignored": "from_me_early"}
        
        BOT_WARNING_PHRASES = [
            "LA CADENA, IDENTIFICADOR ELECTRONICO O CODIGO DE VERIFICACION",
            "DEBE TENER EXACTAMENTE 20 DIGITOS",
            "NO SE DETECTO UNA CADENA VALIDA",
            "LA CURP PARECE INCOMPLETA O INCORRECTA",
            "NO SE DETECTO UNA CURP VALIDA",
            "LA CURP DEBE TENER EXACTAMENTE 18 CARACTERES",
        ]
        
        if any(p in text_upper for p in BOT_WARNING_PHRASES):
            print("IGNORED_REASON = bot_warning_text", flush=True)
            print("BOT_WARNING_TEXT =", repr(text_body[:120]), flush=True)
            return {"ok": True, "ignored": "bot_warning_text"}

        if is_bot_generated_text(text_body):
            print("WEBHOOK_IGNORED_BOT_GENERATED_TEXT =", repr(text_body[:100]), flush=True)
            return {"ok": True, "ignored": "bot_generated_text"}
        
        admin_commands = (
            "/GROUPID",
            "/ADDGROUP",
            "/ADDUSER ",
            "/RMUSER ",
            "/STATUS",
            "/PENDING",
            "/QUEUE",
            "/LAST ",
            "/REQUEUE ",
            "/PROVIDERS",
            "/P1 ON",
            "/P1 OFF",
            "/P2 ON",
            "/P2 OFF",
            "/PROVIDER1 ON",
            "/PROVIDER1 OFF",
            "/PROVIDER2 ON",
            "/PROVIDER2 OFF",
        )

        print("EARLY_EVENT =", event, flush=True)
        print("EARLY_MSG_ID =", msg_id, flush=True)
        print("EARLY_FROM_ME =", from_me, flush=True)
        print("EARLY_REMOTE_JID =", remote_jid, flush=True)
        print("EARLY_MESSAGE_KEYS =", list(message.keys()) if isinstance(message, dict) else [], flush=True)
        
        if from_me and not any(text_upper.startswith(cmd) for cmd in admin_commands):
            print("IGNORED_REASON = from_me", flush=True)
            print("IGNORED_FROM_ME_REMOTE_JID =", remote_jid, flush=True)
            print("IGNORED_FROM_ME_MSG_ID =", msg_id, flush=True)
            return {"ok": True, "ignored": "from_me"}

        provider_groups = _all_provider_groups()
        is_provider_message = source_chat_id in provider_groups
        is_admin_command = text_upper.startswith("/")
        
        if is_group and not is_provider_message and not is_admin_command:
            ignore_group, ignore_reason = _should_ignore_group_for_instance(
                db,
                source_group_id,
                instance_name,
            )
        
            if ignore_group:
                print("IGNORED_GROUP_AUTH_EARLY =", {
                    "reason": ignore_reason,
                    "source_group_id": source_group_id,
                    "instance_name": instance_name,
                    "text": text_body[:120],
                }, flush=True)
        
                return {
                    "ok": True,
                    "ignored": ignore_reason,
                    "source_group_id": source_group_id,
                    "instance_name": instance_name,
                }
        
        if is_group and not is_provider_message:
            try:
                _ensure_group_owner(db, source_group_id, instance_name)
            except Exception as e:
                print("ENSURE_GROUP_OWNER_ERROR =", str(e), flush=True)

        if is_instance_blocked(instance_name) and not is_admin_command:
            msg = (
                "⚠️ Este bot alcanzó su límite de solicitudes.\n\n"
                "Por el momento está bloqueado para nuevas entradas."
            )
        
            if source_group_id:
                if should_send_extra_text(source_group_id):
                    send_group_text(source_group_id, msg, instance_name)
            else:
                send_text(requester_wa_id, msg, instance_name)
        
            return {"ok": True, "ignored": "instance_blocked"}

        print("WEBHOOK_SOURCE_GROUP_ID =", source_group_id, flush=True)
        print("WEBHOOK_IS_GROUP_BLOCKED =", is_group_blocked(source_group_id), flush=True)

        if is_group and is_group_blocked(source_group_id) and not (is_admin_command and _is_admin(requester_wa_id, from_me)):
            print("IGNORED_REASON = group_blocked", flush=True)
            print("IGNORED_GROUP =", source_group_id, flush=True)
            print("IGNORED_INSTANCE =", instance_name, flush=True)
        
            if (instance_name or "").strip() == MAIN_PANEL_INSTANCE:
                msg = (
                    "🔒 *Grupo bloqueado*\n\n"
                    "Este grupo tiene un pago pendiente.\n"
                    "Para reactivar el servicio, por favor contacta al administrador."
                )
                
                try:
                    if should_send_extra_text(source_group_id):
                        send_group_text(
                            source_group_id,
                            msg,
                            instance_name=instance_name,
                        )
                except Exception as e:
                    print("BLOCKED_GROUP_PAYMENT_NOTICE_ERROR =", str(e), flush=True)
        
            return {"ok": True, "ignored": "group_blocked_payment_pending"}
    
        terms = extract_request_terms(text_body)
        problem = detect_identifier_problem(text_body)

        print("DEBUG_TEXT_BODY =", repr(text_body), flush=True)
        print("DEBUG_TERMS =", terms, flush=True)
        print("DEBUG_PROBLEM =", repr(problem), flush=True)
        print("DEBUG_MESSAGE_KEYS =", list(message.keys()), flush=True)

        if not bot_is_open() and terms and not is_provider_message and not is_admin_command:
            msg = (
                "🚀 *DOCU EXPRES*\n"
                "El sistema está cerrado.\n\n"
                "Horario de solicitudes:\n"
                "🕗 8:00 AM - 10:00 PM\n"
                "Horario América/Monterrey."
            )

            if source_group_id:
                if should_send_extra_text(source_group_id):
                    send_group_text(source_group_id, msg, instance_name=instance_name)
            else:
                send_text(requester_wa_id, msg, instance_name=instance_name)

            return {"ok": True, "ignored": "outside_hours"}

        # =========================
        # RESPUESTA DEL PROVEEDOR
        # =========================
        if is_provider_message and not is_admin_command:
            provider_id = _extract_provider_identifier_loose(text_body or "")
            print("PROVIDER_GROUP =", source_chat_id, flush=True)
            print("PROVIDER_TEXT =", text_body, flush=True)
            print("PROVIDER_IDENTIFIER_DETECTED =", provider_id, flush=True)

            quoted_msg_id = _extract_quoted_message_id(message, data)
            text_norm = (text_body or "").strip().upper()
            
            print("PROVIDER_QUOTED_MSG_ID =", quoted_msg_id, flush=True)
            print("PROVIDER_TEXT_NORM =", text_norm, flush=True)

            # =========================
            # PROVIDER14: confirmar solicitud reaccionando 🙌
            # =========================
            provider14_jid = (
                getattr(settings, "PROVIDER14_PRIVATE_JID", "") or ""
            ).strip()

            is_provider14_message = (
                provider14_jid
                and source_chat_id == provider14_jid
            )

            if is_provider14_message:
                text_norm_p14 = normalize_text(text_body or "")

                if (
                    "NO SE ENCUENTRA ACTIVO" in text_norm_p14
                    or "SERVICIO NO ESTA ACTIVO" in text_norm_p14
                    or "SERVICIO NO SE ENCUENTRA ACTIVO" in text_norm_p14
                ):
                    try:
                        redis_conn.setex("provider14:service_closed", 60, "1")
                        print("PROVIDER14_SERVICE_CLOSED_SET =", {
                            "source_chat_id": source_chat_id,
                            "text": text_body,
                        }, flush=True)
                    except Exception as e:
                        print("PROVIDER14_SERVICE_CLOSED_SET_ERROR =", {
                            "error": str(e),
                            "text": text_body,
                        }, flush=True)

                    return {
                        "ok": True,
                        "provider_result": "provider14_service_closed",
                    }

                provider14_mode_ack = ""

                if (
                    "SE HAN ACTUALIZADO" in text_norm_p14
                    or "ESTABLECISTE" in text_norm_p14
                    or "PREFERENCIAS" in text_norm_p14
                ):
                    p14_prefix = ""

                    if "MATRIMONIO" in text_norm_p14:
                        p14_prefix = "MAT"
                    elif "DEFUNCION" in text_norm_p14 or "DEFUNCIÓN" in text_norm_p14:
                        p14_prefix = "DEF"
                    elif "DIVORCIO" in text_norm_p14:
                        p14_prefix = "DIV"
                    elif "NACIMIENTO" in text_norm_p14:
                        p14_prefix = "NAC"

                    p14_mode = ""

                    if "FOLIADO" in text_norm_p14 or "FOLIO" in text_norm_p14:
                        p14_mode = "FOLIADO"
                    elif "REVERSADO" in text_norm_p14 or "REVERSO" in text_norm_p14:
                        p14_mode = "REVERSO"

                    waiting_mode = ""

                    if not p14_prefix:
                        try:
                            waiting_mode = redis_conn.get("provider14:mode_waiting")

                            if isinstance(waiting_mode, bytes):
                                waiting_mode = waiting_mode.decode("utf-8", errors="ignore")

                            waiting_mode = (waiting_mode or "").strip().upper()

                            if waiting_mode.startswith("NAC_"):
                                p14_prefix = "NAC"
                            elif waiting_mode.startswith("MAT_"):
                                p14_prefix = "MAT"
                            elif waiting_mode.startswith("DIV_"):
                                p14_prefix = "DIV"
                            elif waiting_mode.startswith("DEF_"):
                                p14_prefix = "DEF"

                            if not p14_mode:
                                if "FOLIADO" in waiting_mode:
                                    p14_mode = "FOLIADO"
                                elif "REVERSO" in waiting_mode:
                                    p14_mode = "REVERSO"

                            print("PROVIDER14_MODE_ACK_INFERRED_FROM_WAITING =", {
                                "waiting_mode": waiting_mode,
                                "p14_prefix": p14_prefix,
                                "p14_mode": p14_mode,
                                "text": text_body,
                            }, flush=True)

                        except Exception as e:
                            print("PROVIDER14_MODE_WAITING_READ_ERROR =", {
                                "error": str(e),
                                "text": text_body,
                            }, flush=True)

                    if p14_prefix and p14_mode:
                        provider14_mode_ack = f"{p14_prefix} {p14_mode}"
                        provider14_mode_ack_key = (
                            "provider14:mode_ack:"
                            + re.sub(
                                r"[^A-Z0-9]+",
                                "_",
                                provider14_mode_ack.strip().upper(),
                            ).strip("_")
                        )

                        try:
                            redis_conn.setex(provider14_mode_ack_key, 60, "1")

                            try:
                                redis_conn.delete("provider14:mode_waiting")
                            except Exception:
                                pass

                            print("PROVIDER14_MODE_ACK_SET =", {
                                "source_chat_id": source_chat_id,
                                "mode_ack": provider14_mode_ack,
                                "key": provider14_mode_ack_key,
                                "waiting_mode": waiting_mode,
                                "text": text_body,
                            }, flush=True)
                        except Exception as e:
                            print("PROVIDER14_MODE_ACK_SET_ERROR =", {
                                "mode_ack": provider14_mode_ack,
                                "key": provider14_mode_ack_key,
                                "error": str(e),
                            }, flush=True)

                        return {
                            "ok": True,
                            "provider_result": "provider14_mode_ack",
                        }

                is_new_request_confirmation = (
                    "NUEVA SOLICITUD" in text_norm_p14
                    and (
                        "REACCIONA" in text_norm_p14
                        or "SI ES CORRECTO" in text_norm_p14
                    )
                )

                if is_new_request_confirmation:
                    p14_term = provider_id or _extract_provider_identifier_loose(text_body or "")

                    open_req = None

                    if p14_term:
                        open_req = (
                            db.query(RequestLog)
                            .filter(
                                RequestLog.provider_group_id == source_chat_id,
                                RequestLog.curp == p14_term,
                                RequestLog.status == "PROCESSING",
                                RequestLog.provider_name == "PROVIDER14",
                            )
                            .order_by(RequestLog.created_at.desc())
                            .first()
                        )

                    if not open_req:
                        open_req = (
                            db.query(RequestLog)
                            .filter(
                                RequestLog.provider_group_id == source_chat_id,
                                RequestLog.status == "PROCESSING",
                                RequestLog.provider_name == "PROVIDER14",
                            )
                            .order_by(RequestLog.created_at.desc())
                            .first()
                        )

                    print("PROVIDER14_CONFIRM_DETECTED =", {
                        "msg_id": msg_id,
                        "source_chat_id": source_chat_id,
                        "p14_term": p14_term,
                        "matched_req_id": getattr(open_req, "id", None),
                    }, flush=True)

                    if open_req:
                        try:
                            send_reaction(
                                source_chat_id,
                                msg_id,
                                "🙌",
                                instance_name=instance_name,
                                from_me=False,
                            )

                            open_req.provider_message_id = msg_id
                            open_req.updated_at = _utc_now_naive()
                            db.commit()

                            try:
                                submit_ack_key = f"provider14:submit_ack:{int(open_req.id)}"
                                redis_conn.setex(submit_ack_key, 60, "1")
                                print("PROVIDER14_SUBMIT_ACK_SET =", {
                                    "req_id": open_req.id,
                                    "key": submit_ack_key,
                                    "msg_id": msg_id,
                                }, flush=True)
                            except Exception as ack_exc:
                                print("PROVIDER14_SUBMIT_ACK_SET_ERROR =", {
                                    "req_id": open_req.id,
                                    "error": str(ack_exc),
                                }, flush=True)

                            print("PROVIDER14_REACTION_SENT =", {
                                "req_id": open_req.id,
                                "msg_id": msg_id,
                                "reaction": "🙌",
                            }, flush=True)

                            return {
                                "ok": True,
                                "provider_result": "provider14_confirm_reacted",
                            }

                        except Exception as react_exc:
                            print("PROVIDER14_REACTION_ERROR =", {
                                "msg_id": msg_id,
                                "error": str(react_exc),
                            }, flush=True)

            # 1) INTENTAR DETECTAR PDF
            doc = None
            doc_mode = "none"
            media_message_id = msg_id
            
            msg_unwrapped = _unwrap_message(message) or message
            
            if "documentMessage" in msg_unwrapped:
                doc_mode = "direct_document"
                doc = msg_unwrapped.get("documentMessage")
                media_message_id = msg_id
            
            elif "documentWithCaptionMessage" in msg_unwrapped:
                doc_mode = "direct_document_with_caption"
                doc_wrap = msg_unwrapped.get("documentWithCaptionMessage", {})
                doc = doc_wrap.get("message", {}).get("documentMessage")
                media_message_id = msg_id
            
            elif "extendedTextMessage" in msg_unwrapped:
                ext = msg_unwrapped.get("extendedTextMessage", {})
                ctx = ext.get("contextInfo", {}) or {}
                quoted = _unwrap_message(ctx.get("quotedMessage", {}) or {})
            
                quoted_doc_msg_id = ctx.get("stanzaId", "") or ctx.get("quotedStanzaID", "") or ""
            
                if "documentMessage" in quoted:
                    doc_mode = "quoted_document"
                    doc = quoted.get("documentMessage")
                    media_message_id = quoted_doc_msg_id or msg_id
            
                elif "documentWithCaptionMessage" in quoted:
                    doc_mode = "quoted_document_with_caption"
                    doc_wrap = quoted.get("documentWithCaptionMessage", {})
                    doc = doc_wrap.get("message", {}).get("documentMessage")
                    media_message_id = quoted_doc_msg_id or msg_id

            print("DOC_MESSAGE_MODE =", doc_mode, flush=True)
            print("MEDIA_MESSAGE_ID_USED =", media_message_id, flush=True)

            # =========================
            # NEGATIVOS DE PROVEEDOR WHATSAPP
            # Cubre:
            # 1) reply/cita al mensaje del bot: "SIN", "-", "No hay registros..."
            # 2) texto mencionando CURP/cadena: "CURP No hay registros disponibles", "CURP SIN"
            # =========================
            if not doc:
                negative_close = _close_provider_negative_response(
                    db,
                    source_chat_id=source_chat_id,
                    quoted_msg_id=quoted_msg_id,
                    text_body=text_body,
                )

                if negative_close.get("closed"):
                    return {
                        "ok": True,
                        "provider_result": "provider_negative_closed",
                        **negative_close,
                    }

            # =========================
            # MATCH ESPECIAL: RESPUESTAS NEGATIVAS
            # 1) reply id
            # 2) fallback por CURP en texto
            # =========================
            if not doc:
                sin_values = {
                    "SIN",
                    "SIN REGISTRO",
                    "SIN REGISTROS",
                    "SIN DISPONIBLE",
                    "SIN RESULTADO",
                    "SIN RESULTADOS",
                    "SIN DATOS",
                    "SIN INFORMACION",
                    "SIN INFORMACIÓN",
                    "NO ESTA",
                    "NO ESTÁ",
                    "NO EXISTE",
                    "NO ENCONTRADO",
                    "NO ENCONTRADA",
                    "NO ENCONTRADOS",
                    "NO ENCONTRADAS",
                    "NO SE ENCONTRO",
                    "NO SE ENCONTRÓ",
                    "NO SE ENCUENTRA",
                    "NO SE LOCALIZA",
                    "NO LOCALIZADO",
                    "NO LOCALIZADA",
                    "NO DISPONIBLE",
                    "NO DISPONIBLES",
                    "NO HAY REGISTRO",
                    "NO HAY REGISTROS",
                    "NO HAY RESULTADO",
                    "NO HAY RESULTADOS",
                    "NO HAY DATOS",
                    "NO HAY INFORMACION",
                    "NO HAY INFORMACIÓN",
                    "REGISTRO NO ENCONTRADO",
                    "REGISTRO NO LOCALIZADO",
                    "NB",
                    "VERI",
                    "VERIFICAR",
                    "NO SÉ ENCUENTRA EN EL SISTEMA",
                    "NO SE ENCUENTRA EN EL SISTEMA",
                    "NO LOCALIZADO EN LA BASE DE DATOS",
                    "NO LOCALIZADA EN LA BASE DE DATOS",
                    "NO LOCALIZADO EN BASE DE DATOS",
                    "NO LOCALIZADA EN BASE DE DATOS",
                    "INVALIDA",
                    "INVÁLIDA",
                    "INFORMACIÓN! LO SIENTO",
                    "INFORMACION! LO SIENTO",
                    "ACTA NO ENCONTRADA",
                    "DOCUMENTO NO ENCONTRADO",
                    "ERROR! CURP INVALIDA",
                    "ERROR! CURP INVÁLIDA",
                    "NO SE HA ENCONTRADO",
                    "NO SE ENCONTRARON",
                }

                is_negative_text = any(
                    re.search(rf"\b{re.escape(v)}\b", text_norm)
                    for v in sin_values
                )

                # 1) MATCH POR REPLY ID
                if quoted_msg_id and is_negative_text:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.provider_message_id == quoted_msg_id,
                            RequestLog.status == "PROCESSING",
                            RequestLog.provider_name.in_(WHATSAPP_TEXT_PROVIDERS),
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )

                    if open_req:
                        print("PROVIDER_NEGATIVE_MATCHED_REQ_ID =", open_req.id, flush=True)
                        negative_act_group = _text_mentions_act_type_group(text_norm)

                        if negative_act_group and _expected_act_type_group(open_req.act_type) != negative_act_group:
                            print("PROVIDER_NEGATIVE_REPLY_TYPE_MISMATCH_IGNORE =", {
                                "req_id": open_req.id,
                                "req_act_type": open_req.act_type,
                                "negative_act_group": negative_act_group,
                                "provider_id": provider_id,
                            }, flush=True)
                    
                            return {"ok": True, "ignored": "negative_reply_type_mismatch"}
                            
                        print("PROVIDER_NEGATIVE_MATCHED_PROVIDER =", open_req.provider_name, flush=True)
                        print("PROVIDER_NEGATIVE_MATCHED_CURP =", open_req.curp, flush=True)

                        open_req.status = "ERROR"
                        open_req.error_message = "SIN REGISTRO"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()

                        msg = (
                            f"❌ No hay registros disponibles.\n"
                            f"Dato: {open_req.curp}\n"
                            f"Tipo: {open_req.act_type}\n\n"
                            f"Verificar que la CURP esté certificada en RENAPO"
                        )

                        try:
                            client_instance = open_req.instance_name or "docifybot8"
                        
                            if not _deliver_text_result(open_req, msg, instance_name=client_instance):
                                return {"ok": True, "ignored": "no_record_duplicate"}
                        
                        except Exception as notify_exc:
                            print("PROVIDER_NEGATIVE_NOTIFY_ERROR =", str(notify_exc), flush=True)

                        return {"ok": True, "provider_result": "provider_negative_matched_by_reply_id"}

                    print("PROVIDER5_SIN_WITHOUT_MATCH =", quoted_msg_id, flush=True)

                # 2) FALLBACK POR CURP EN TEXTO
                if provider_id and is_negative_text:
                    negative_act_group = _text_mentions_act_type_group(text_norm)

                    candidates = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.curp == provider_id,
                            RequestLog.status == "PROCESSING",
                            RequestLog.provider_name.in_(WHATSAPP_TEXT_PROVIDERS),
                        )
                        .order_by(RequestLog.created_at.desc())
                        .all()
                    )
                    
                    if negative_act_group:
                        typed_candidates = [
                            r for r in candidates
                            if _expected_act_type_group(r.act_type) == negative_act_group
                        ]
                    
                        print("PROVIDER_NEGATIVE_TYPED_CANDIDATES =", {
                            "provider_id": provider_id,
                            "negative_act_group": negative_act_group,
                            "candidate_ids": [r.id for r in candidates],
                            "candidate_types": [r.act_type for r in candidates],
                            "typed_ids": [r.id for r in typed_candidates],
                        }, flush=True)
                    
                        open_req = typed_candidates[0] if len(typed_candidates) == 1 else None
                    else:
                        open_req = candidates[0] if len(candidates) == 1 else None
                    
                        if len(candidates) > 1:
                            print("PROVIDER_NEGATIVE_AMBIGUOUS_SAME_CURP_NO_TYPE =", {
                                "provider_id": provider_id,
                                "candidate_ids": [r.id for r in candidates],
                                "candidate_types": [r.act_type for r in candidates],
                            }, flush=True)

                    if open_req:
                        print("PROVIDER5_FALLBACK_MATCHED_REQ_ID =", open_req.id, flush=True)
                        print("PROVIDER5_FALLBACK_MATCHED_CURP =", open_req.curp, flush=True)

                        open_req.status = "ERROR"
                        open_req.error_message = "SIN REGISTRO"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()

                        msg = (
                            f"❌ No hay registros disponibles.\n"
                            f"Dato: {open_req.curp}\n"
                            f"Tipo: {open_req.act_type}\n\n"
                            f"Verificar que la CURP esté certificada en RENAPO"
                        )

                        try:
                            client_instance = open_req.instance_name or "docifybot8"
                        
                            if not _deliver_text_result(open_req, msg, instance_name=client_instance):
                                return {"ok": True, "ignored": "no_record_duplicate"}
                        
                        except Exception as notify_exc:
                            print("PROVIDER_NEGATIVE_NOTIFY_ERROR =", str(notify_exc), flush=True)

                        return {"ok": True, "provider_result": "provider5_fallback_matched_by_curp"}

                    print("PROVIDER5_FALLBACK_WITHOUT_MATCH =", provider_id, flush=True)

                # 3) FALLBACK SOLO PROVIDER8: si manda solo "SIN" y no coincide reply ni CURP
                if is_negative_text:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.status == "PROCESSING",
                            RequestLog.provider_name == "PROVIDER8",
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )
                
                    if open_req:
                        print("PROVIDER8_SIN_FALLBACK_MATCHED_REQ_ID =", open_req.id, flush=True)
                        print("PROVIDER8_SIN_FALLBACK_MATCHED_CURP =", open_req.curp, flush=True)
                
                        open_req.status = "ERROR"
                        open_req.error_message = "SIN REGISTRO"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()
                
                        msg = (
                            f"❌ No hay registros disponibles.\n"
                            f"Dato: {open_req.curp}\n"
                            f"Tipo: {open_req.act_type}\n\n"
                            f"Verificar que la CURP esté certificada en RENAPO"
                        )
                
                        try:
                            client_instance = open_req.instance_name or "docifybot8"
                        
                            if not _deliver_text_result(open_req, msg, instance_name=client_instance):
                                return {"ok": True, "ignored": "no_record_duplicate"}
                        
                        except Exception as notify_exc:
                            print("PROVIDER_NEGATIVE_NOTIFY_ERROR =", str(notify_exc), flush=True)
                                        
                        return {"ok": True, "provider_result": "provider8_sin_fallback_last_processing"}
                
                    print("PROVIDER8_SIN_FALLBACK_WITHOUT_MATCH =", source_chat_id, flush=True)

            if doc:
                filename = doc.get("fileName") or ""
                filename_id = _extract_identifier_from_filename_local(filename)
                
                doc_caption = (
                    doc.get("caption")
                    or doc.get("title")
                    or doc.get("fileName")
                    or ""
                )
                
                caption_id = _extract_provider_identifier_loose(doc_caption or "")
                
                print("PROVIDER_DOC_FILENAME =", filename, flush=True)
                print("PROVIDER_DOC_FILENAME_IDENTIFIER =", filename_id, flush=True)
                print("PROVIDER_DOC_CAPTION =", doc_caption, flush=True)
                print("PROVIDER_DOC_CAPTION_IDENTIFIER =", caption_id, flush=True)
            
                provider_msg_ts = data.get("messageTimestamp")
                webhook_received_ts = time.time()
            
                print("PROVIDER_EVENT_MESSAGE_TIMESTAMP =", provider_msg_ts, flush=True)
                print("WEBHOOK_RECEIVED_TS =", webhook_received_ts, flush=True)

                lag_s = None
                try:
                    if provider_msg_ts:
                        lag_s = webhook_received_ts - float(provider_msg_ts)
                        print("PROVIDER_TO_WEBHOOK_LAG_S =", round(lag_s, 3), flush=True)
                except Exception as ts_exc:
                    print("PROVIDER_TO_WEBHOOK_LAG_ERROR =", str(ts_exc), flush=True)
            
                pdf_received_ts = time.time()
                print("PROVIDER1_PDF_RECEIVED =", media_message_id, pdf_received_ts, flush=True)
            
                open_req = None
                lookup_id = filename_id or provider_id or caption_id
            
                media_b64_start_ts = time.time()
                print("PROVIDER1_MEDIA_B64_START =", media_message_id, media_b64_start_ts, flush=True)
                print("PDF_RECEIVED_TO_MEDIA_B64_START_S =", round(media_b64_start_ts - pdf_received_ts, 3), flush=True)
            
                t_media_b64_start = time.perf_counter()
                t0 = time.perf_counter()
                print("T_DOC_DETECTED =", source_chat_id, media_message_id, flush=True)
            
                t1 = time.perf_counter()

                try:
                    print("GET_MEDIA_BASE64_CALL_START =", {
                        "media_message_id": media_message_id,
                        "instance_name": instance_name,
                        "filename": filename,
                        "lookup_id": lookup_id,
                    }, flush=True)
                
                    media_json = get_media_base64("document", media_message_id, instance_name)
                
                    print("GET_MEDIA_BASE64_CALL_OK =", {
                        "media_message_id": media_message_id,
                        "elapsed_s": round(time.perf_counter() - t1, 3),
                    }, flush=True)
                
                except Exception as media_exc:
                    print(
                        "GET_MEDIA_BASE64_CALL_ERROR =",
                        {
                            "media_message_id": media_message_id,
                            "instance_name": instance_name,
                            "filename": filename,
                            "lookup_id": lookup_id,
                            "error": str(media_exc),
                        },
                        flush=True,
                    )
                
                    # El msg_id se marcó como visto al entrar al webhook.
                    # Como la descarga falló por una causa transitoria,
                    # se libera para permitir que Evolution lo reenvíe.
                    release_webhook_msg_seen(
                        msg_id,
                        instance_name,
                    )
                
                    return {
                        "ok": True,
                        "ignored": (
                            "provider_pdf_media_download_failed_retryable"
                        ),
                        "error": str(media_exc),
                    }
                
                print("T_GET_MEDIA_BASE64 =", round(time.perf_counter() - t1, 3), flush=True)
            
                print(
                    "PROVIDER1_MEDIA_B64_DONE =",
                    media_message_id,
                    time.time(),
                    "elapsed_s=",
                    round(time.perf_counter() - t_media_b64_start, 3),
                    flush=True,
                )

                media_b64 = (
                    media_json.get("base64")
                    or media_json.get("data")
                    or media_json.get("media")
                    or ""
                )
                
                if not media_b64:
                    print(
                        "PROVIDER_PDF_BASE64_EMPTY =",
                        media_json,
                        flush=True,
                    )
                
                    release_webhook_msg_seen(
                        msg_id,
                        instance_name,
                    )
                
                    return {
                        "ok": True,
                        "ignored": "provider_pdf_base64_empty_retryable",
                    }
                
                if media_b64.startswith("data:"):
                    parts = media_b64.split(",", 1)
                    media_b64 = parts[1] if len(parts) > 1 else media_b64
                
                media_b64 = media_b64.replace("\n", "").replace("\r", "").strip()
                
                missing_padding = len(media_b64) % 4
                if missing_padding:
                    media_b64 += "=" * (4 - missing_padding)

                t_decode = time.perf_counter()

                try:
                    pdf_bytes = base64.b64decode(
                        media_b64,
                        validate=False,
                    )
                
                except Exception as decode_exc:
                    print(
                        "PROVIDER_PDF_BASE64_DECODE_ERROR =",
                        {
                            "msg_id": msg_id,
                            "media_message_id": media_message_id,
                            "filename": filename,
                            "lookup_id": lookup_id,
                            "base64_len": len(media_b64 or ""),
                            "error": str(decode_exc),
                        },
                        flush=True,
                    )
                
                    release_webhook_msg_seen(
                        msg_id,
                        instance_name,
                    )
                
                    return {
                        "ok": True,
                        "ignored": "provider_pdf_base64_decode_failed_retryable",
                        "error": str(decode_exc),
                    }
                
                print(
                    "T_BASE64_DECODE =",
                    round(time.perf_counter() - t_decode, 3),
                    flush=True,
                )
                
                print("PDF_HEADER =", pdf_bytes[:8], flush=True)
                print("PDF_BYTES_LEN =", len(pdf_bytes), flush=True)
                
                if b"%PDF" not in pdf_bytes[:20]:
                    print(
                        "PROVIDER_PDF_INVALID_BINARY",
                        flush=True,
                    )
                
                    release_webhook_msg_seen(
                        msg_id,
                        instance_name,
                    )
                
                    return {
                        "ok": True,
                        "ignored": "provider_pdf_invalid_binary_retryable",
                    }

                open_req = _pick_matching_processing_req_for_pdf(
                    db=db,
                    lookup_id=lookup_id,
                    source_chat_id=source_chat_id,
                    quoted_msg_id=quoted_msg_id,
                    pdf_bytes=pdf_bytes,
                    instance_name=instance_name,
                )
                
                print("PROVIDER_PDF_FALLBACK_MATCH =", {
                    "lookup_id": lookup_id,
                    "quoted_msg_id": quoted_msg_id,
                    "matched_req_id": getattr(open_req, "id", None),
                    "matched_provider_group_id": getattr(open_req, "provider_group_id", None),
                    "matched_source_group_id": getattr(open_req, "source_group_id", None),
                    "matched_instance_name": getattr(open_req, "instance_name", None),
                    "matched_act_type": getattr(open_req, "act_type", None),
                }, flush=True)
                
                if not open_req:
                    print(
                        "PROVIDER_PDF_WITHOUT_SAFE_MATCH =",
                        {
                            "filename": filename,
                            "lookup_id": lookup_id,
                            "source_chat_id": source_chat_id,
                            "quoted_msg_id": quoted_msg_id,
                            "msg_id": msg_id,
                            "media_message_id": media_message_id,
                        },
                        flush=True,
                    )
                
                    fallback_req = None
                    fallback_filters = None
                
                    if lookup_id:
                        fallback_filters = [
                            RequestLog.curp == lookup_id,
                            _provider_pdf_match_status_filter(),
                        ]
                
                        fallback_req = (
                            db.query(RequestLog)
                            .filter(*fallback_filters)
                            .order_by(
                                case(
                                    (RequestLog.status == "PROCESSING", 0),
                                    else_=1,
                                ),
                                RequestLog.created_at.desc(),
                            )
                            .first()
                        )
                
                    if fallback_req and fallback_filters:
                        active_count = (
                            db.query(RequestLog)
                            .filter(*fallback_filters)
                            .count()
                        )
                
                        print(
                            "PROVIDER_PDF_LAST_RESORT_ACTIVE_COUNT =",
                            active_count,
                            flush=True,
                        )
                
                        if active_count == 1:
                            print(
                                "PROVIDER_PDF_LAST_RESORT_MATCH =",
                                fallback_req.id,
                                flush=True,
                            )
                            open_req = fallback_req
                
                        else:
                            print(
                                "PROVIDER_PDF_LAST_RESORT_AMBIGUOUS =",
                                {
                                    "lookup_id": lookup_id,
                                    "active_count": active_count,
                                },
                                flush=True,
                            )
                
                            # No liberar en caso ambiguo:
                            # podría entregar el mismo PDF a la solicitud incorrecta.
                            return {
                                "ok": True,
                                "ignored": (
                                    "ambiguous_multiple_processing_"
                                    "or_recent_timeout_requests"
                                ),
                            }
                
                    else:
                        # Puede ser una carrera:
                        # el proveedor respondió antes de que la solicitud quedara
                        # completamente visible o registrada en base de datos.
                        release_webhook_msg_seen(
                            msg_id,
                            instance_name,
                        )
                
                        print(
                            "PROVIDER_PDF_MATCH_RETRY_RELEASED =",
                            {
                                "msg_id": msg_id,
                                "media_message_id": media_message_id,
                                "lookup_id": lookup_id,
                                "source_chat_id": source_chat_id,
                            },
                            flush=True,
                        )
                
                        return {
                            "ok": True,
                            "ignored": "provider_pdf_without_safe_match_retryable",
                        }

                is_chain_req = is_chain(open_req.curp)
                detected_pdf_type = "" if is_chain_req else _detect_pdf_act_type(pdf_bytes)

                if (
                    not is_chain_req
                    and detected_pdf_type
                    and _expected_act_type_group(open_req.act_type) != detected_pdf_type
                ):
                    print("PROVIDER_PDF_WRONG_ACT_TYPE_AFTER_SMART_MATCH =", {
                        "req_id": open_req.id,
                        "curp": open_req.curp,
                        "expected_act_type": open_req.act_type,
                        "detected_pdf_type": detected_pdf_type,
                        "filename": filename,
                        "source_chat_id": source_chat_id,
                    }, flush=True)
                
                    reroute_req = _find_same_curp_req_by_act_type(
                        db,
                        curp=open_req.curp,
                        provider_group_id=source_chat_id,
                        detected_type=detected_pdf_type,
                    )
                
                    if reroute_req and reroute_req.id != open_req.id:
                        print("PROVIDER_PDF_REROUTED_TO_SAME_CURP_CORRECT_ACT_TYPE =", {
                            "old_req_id": open_req.id,
                            "old_act_type": open_req.act_type,
                            "new_req_id": reroute_req.id,
                            "new_act_type": reroute_req.act_type,
                            "detected_pdf_type": detected_pdf_type,
                            "filename": filename,
                        }, flush=True)
                
                        open_req = reroute_req
                        is_chain_req = is_chain(open_req.curp)
                
                    else:
                        open_req.status = "PROCESSING"
                        open_req.error_message = "WRONG_ACT_TYPE_PDF_PENDING_RETRY"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()
                
                        try:
                            support_key = f"support_wrong_type_pending:{open_req.id}"
                            if redis_conn.set(support_key, "1", ex=120, nx=True):
                                _notify_support_error(
                                    open_req,
                                    "WRONG_ACT_TYPE_PDF_PENDING_RETRY",
                                    (
                                        f"filename={filename} | "
                                        f"expected_act_type={open_req.act_type} | "
                                        f"detected_pdf_type={detected_pdf_type} | "
                                        f"NO se notificó al cliente para evitar falso error"
                                    )
                                )
                        except Exception as support_exc:
                            print("NOTIFY_SUPPORT_ERROR_FAILED =", str(support_exc), flush=True)
                
                        return {"ok": True, "ignored": "provider_pdf_wrong_act_type_pending_retry"}
                
                if is_chain_req:
                    print("PROVIDER_CHAIN_SKIP_ACT_TYPE_VALIDATION =", open_req.curp, flush=True)
                
                if not is_chain_req:
                    term_check = _validate_pdf_term_detailed(
                        pdf_bytes,
                        open_req.curp,
                        open_req.act_type,
                    )
                
                    term_status = term_check.get("status")
                    term_reason = term_check.get("reason")
                    found_curps = term_check.get("found_curps") or []
                
                    expected_term_norm = re.sub(r"[^A-Z0-9]", "", (open_req.curp or "").upper())
                    filename_id_norm = re.sub(r"[^A-Z0-9]", "", (filename_id or "").upper())
                    provider_id_norm = re.sub(r"[^A-Z0-9]", "", (provider_id or "").upper())
                
                    filename_matches_expected = bool(
                        expected_term_norm
                        and filename_id_norm
                        and filename_id_norm == expected_term_norm
                    )
                
                    provider_text_matches_expected = bool(
                        expected_term_norm
                        and provider_id_norm
                        and provider_id_norm == expected_term_norm
                    )
                
                    if term_status == "MISMATCH":
                        print("PROVIDER_PDF_WRONG_INTERNAL_CURP =", {
                            "req_id": open_req.id,
                            "expected_curp": open_req.curp,
                            "found_curps": found_curps,
                            "expected_act_type": open_req.act_type,
                            "filename": filename,
                            "filename_id": filename_id,
                            "provider_id": provider_id,
                            "source_chat_id": source_chat_id,
                        }, flush=True)
                
                        open_req.status = "PROCESSING"
                        open_req.error_message = "WRONG_CURP_IN_PDF_PENDING_RETRY"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()
                
                        try:
                            support_key = f"support_wrong_curp_pending:{open_req.id}"
                            if redis_conn.set(support_key, "1", ex=120, nx=True):
                                _notify_support_error(
                                    open_req,
                                    "WRONG_CURP_IN_PDF_PENDING_RETRY",
                                    (
                                        f"filename={filename} | "
                                        f"expected_curp={open_req.curp} | "
                                        f"found_curps={found_curps} | "
                                        f"motivo=CURP interna diferente; NO se entregó al cliente"
                                    )
                                )
                        except Exception as support_exc:
                            print("NOTIFY_SUPPORT_WRONG_CURP_PENDING_FAILED =", str(support_exc), flush=True)
                
                        return {"ok": True, "ignored": "provider_pdf_wrong_internal_curp_pending_retry"}
                
                    if term_status == "UNCERTAIN":
                        # Aquí NO hay CURP interna diferente.
                        # Solo no se pudo leer bien el texto interno.
                        # En este caso sí se permite respaldo por filename/texto del proveedor.
                        if filename_matches_expected or provider_text_matches_expected:
                            print("PROVIDER_PDF_CURP_UNCERTAIN_BUT_FILENAME_MATCHES =", {
                                "req_id": open_req.id,
                                "expected_curp": open_req.curp,
                                "expected_act_type": open_req.act_type,
                                "filename": filename,
                                "filename_id": filename_id,
                                "provider_id": provider_id,
                                "reason": term_reason,
                            }, flush=True)
                        else:
                            print("PROVIDER_PDF_CURP_UNCERTAIN_NO_FILENAME_MATCH =", {
                                "req_id": open_req.id,
                                "expected_curp": open_req.curp,
                                "expected_act_type": open_req.act_type,
                                "filename": filename,
                                "filename_id": filename_id,
                                "provider_id": provider_id,
                                "reason": term_reason,
                                "source_chat_id": source_chat_id,
                            }, flush=True)
                
                            open_req.status = "PROCESSING"
                            open_req.error_message = "WRONG_CURP_IN_PDF_PENDING_RETRY"
                            open_req.updated_at = _utc_now_naive()
                            db.commit()
                
                            try:
                                support_key = f"support_wrong_curp_uncertain:{open_req.id}"
                                if redis_conn.set(support_key, "1", ex=120, nx=True):
                                    _notify_support_error(
                                        open_req,
                                        "WRONG_CURP_IN_PDF_PENDING_RETRY",
                                        (
                                            f"filename={filename} | "
                                            f"filename_id={filename_id} | "
                                            f"provider_id={provider_id} | "
                                            f"expected_curp={open_req.curp} | "
                                            f"motivo=No se pudo confirmar CURP interna y filename/texto no coincide; NO se entregó al cliente"
                                        )
                                    )
                            except Exception as support_exc:
                                print("NOTIFY_SUPPORT_WRONG_CURP_UNCERTAIN_FAILED =", str(support_exc), flush=True)
                
                            return {"ok": True, "ignored": "provider_pdf_curp_uncertain_pending_retry"}

                if open_req.provider_name == "PROVIDER8":
                    try:
                        from app.services.provider7 import procesar_pdf_externo_provider8
                
                        result = procesar_pdf_externo_provider8(
                            pdf_bytes=pdf_bytes,
                            term=open_req.curp,
                            act_type=open_req.act_type,
                            filename=filename or f"{open_req.curp}.pdf",
                        )
                
                        pdf_bytes = result["pdf_bytes"]
                
                        print("PROVIDER8_POSTPROCESS_OK =", {
                            "req_id": open_req.id,
                            "estado": result.get("estado"),
                            "folio": result.get("folio"),
                            "pdf_bytes_len": len(pdf_bytes),
                        }, flush=True)
                
                    except Exception as e:
                        print("PROVIDER8_POSTPROCESS_ERROR =", str(e), flush=True)
                
                        open_req.status = "ERROR"
                        open_req.error_message = f"PROVIDER8_POSTPROCESS_ERROR: {str(e)[:300]}"
                        open_req.updated_at = _utc_now_naive()
                        db.commit()
                
                        try:
                            _notify_support_error(
                                open_req,
                                "PROVIDER8_POSTPROCESS_ERROR",
                                f"filename={filename} | error={str(e)[:500]}"
                            )
                        except Exception as support_exc:
                            print("PROVIDER8_POSTPROCESS_SUPPORT_NOTIFY_ERROR =", str(support_exc), flush=True)
                
                        return {"ok": True, "ignored": "provider8_postprocess_failed"}
                
                t_encode = time.perf_counter()
                safe_media_b64 = base64.b64encode(pdf_bytes).decode()
                print("T_BASE64_REENCODE_FINAL =", round(time.perf_counter() - t_encode, 3), flush=True)
                
                # Dedupe fuerte:
                # 1) provider_pdf_done = solo si ya se entregó correctamente.
                # 2) provider_pdf_sending = candado temporal mientras se está entregando.
                done_key = f"provider_pdf_done:{open_req.id}"
                if redis_conn.get(done_key):
                    print("PROVIDER_PDF_ALREADY_DONE_IGNORED =", done_key, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_already_done"}
                
                sending_key = f"provider_pdf_sending:{open_req.id}"
                already_processing = redis_conn.set(sending_key, "1", ex=300, nx=True)
                
                if not already_processing:
                    print("PROVIDER_PDF_DUPLICATE_SENDING_IGNORED =", sending_key, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_duplicate_sending"}
                
                match_term = filename_id or provider_id or open_req.curp or "NO_TERM"
                pdf_dedupe_key = f"provider_pdf:{open_req.id}:{source_chat_id}:{match_term}:{filename or 'nofile'}"
                
                already_sent = redis_conn.set(pdf_dedupe_key, "1", ex=3600, nx=True)
                if not already_sent:
                    print("PROVIDER_PDF_DUPLICATE_IGNORED =", pdf_dedupe_key, flush=True)

                    try:
                        redis_conn.delete(sending_key)
                    except Exception as redis_del_exc:
                        print("PDF_SENDING_DELETE_AFTER_DUPLICATE_ERROR =", str(redis_del_exc), flush=True)

                    return {"ok": True, "ignored": "provider_pdf_duplicate"}
                
                open_req.pdf_url = None
                open_req.provider_media_url = "BASE64_FROM_MEDIA_MESSAGE"
                open_req.status = "PROCESSING"
                open_req.error_message = None
                open_req.updated_at = _utc_now_naive()
                open_req.provider_to_webhook_lag_s = round(lag_s, 3) if lag_s is not None else None

                t2 = time.perf_counter()
                db.commit()
                print("T_DB_COMMIT_BEFORE_DELIVERY =", round(time.perf_counter() - t2, 3), flush=True)

                print("PROVIDER_PDF_MATCHED_REQ_ID =", open_req.id, flush=True)
                print("PROVIDER_PDF_MATCHED_CURP =", open_req.curp, flush=True)

                print("PROVIDER1_RELAY_CONTEXT =", {
                    "req_id": open_req.id,
                    "curp": open_req.curp,
                    "act_type": open_req.act_type,
                    "provider_group_id": open_req.provider_group_id,
                    "source_group_id": open_req.source_group_id,
                    "doc_mode": doc_mode,
                }, flush=True)
    
                total_relay_s = None
                t4 = time.perf_counter()
                try:
                    try:
                        save_request_pdf_to_r2(
                            open_req,
                            db,
                            pdf_bytes,
                            filename=filename or f"{open_req.curp}.pdf",
                            origin=f"provider_whatsapp_before_delivery:{open_req.provider_name or ''}",
                        )
                        db.commit()
                        print("R2_SAVE_BEFORE_DELIVERY_OK =", {
                            "req_id": getattr(open_req, "id", None),
                            "provider_name": getattr(open_req, "provider_name", None),
                            "filename": filename or f"{open_req.curp}.pdf",
                            "pdf_url": getattr(open_req, "pdf_url", None),
                        }, flush=True)
                
                    except Exception as r2_exc:
                        print("R2_SAVE_BEFORE_DELIVERY_ERROR =", {
                            "req_id": getattr(open_req, "id", None),
                            "provider_name": getattr(open_req, "provider_name", None),
                            "filename": filename,
                            "error": str(r2_exc),
                        }, flush=True)
                
                    _deliver_pdf_result(
                        open_req,
                        safe_media_b64,
                        filename=filename or f"{open_req.curp}.pdf",
                    )
                    print("T_DELIVER_PDF_RESULT =", round(time.perf_counter() - t4, 3), flush=True)
                
                except Exception as delivery_exc:
                    print("DELIVERY_FAILED =", str(delivery_exc), flush=True)
                
                    open_req.status = "ERROR"
                    open_req.error_message = f"DELIVERY_FAILED_PENDING_RETRY: {str(delivery_exc)[:300]}"
                    open_req.updated_at = _utc_now_naive()
                    db.commit()
                
                    try:
                        if getattr(open_req, "pdf_url", None):
                            request_queue.enqueue_in(
                                timedelta(seconds=30),
                                retry_pdf_delivery,
                                open_req.id,
                                1,
                            )
                
                            print("MAIN_DELIVERY_FAILED_RETRY_SCHEDULED =", {
                                "req_id": open_req.id,
                                "pdf_url": open_req.pdf_url,
                                "instance": open_req.instance_name,
                                "source_group_id": open_req.source_group_id,
                            }, flush=True)
                        else:
                            print("MAIN_DELIVERY_FAILED_NO_PDF_URL_FOR_RETRY =", {
                                "req_id": open_req.id,
                                "provider_media_url": open_req.provider_media_url,
                            }, flush=True)
                    except Exception as retry_exc:
                        print("MAIN_DELIVERY_FAILED_RETRY_SCHEDULE_ERROR =", str(retry_exc), flush=True)
                
                    try:
                        redis_conn.delete(pdf_dedupe_key)
                    except Exception as redis_del_exc:
                        print("PDF_DEDUPE_DELETE_AFTER_DELIVERY_FAILED_ERROR =", str(redis_del_exc), flush=True)
                
                    try:
                        redis_conn.delete(sending_key)
                    except Exception as redis_del_exc:
                        print("PDF_SENDING_DELETE_AFTER_DELIVERY_FAILED_ERROR =", str(redis_del_exc), flush=True)
                
                    try:
                        _notify_support_error(
                            open_req,
                            "DELIVERY_FAILED",
                            f"filename={filename} | error={str(delivery_exc)[:500]}"
                        )
                    except Exception as support_exc:
                        print("DELIVERY_FAILED_SUPPORT_NOTIFY_ERROR =", str(support_exc), flush=True)
                
                    return {"ok": True, "ignored": "delivery_failed"}

                open_req.status = "DONE"
                open_req.error_message = None
                open_req.updated_at = _utc_now_naive()
                db.commit()
                
                try:
                    redis_conn.set(done_key, "1", ex=3600)
                    redis_conn.delete(sending_key)
                except Exception as redis_done_exc:
                    print("PDF_DONE_MARK_REDIS_ERROR =", str(redis_done_exc), flush=True)

                if getattr(open_req, "api_client_id", None):
                    try:
                        _handle_api_charge_after_done(open_req, db)
                        print("API_MAIN_SKIP_BOT_LIMIT_AND_PROMOS =", open_req.id, flush=True)
                    except Exception as api_charge_exc:
                        print("API_MAIN_CHARGE_ERROR =", str(api_charge_exc), flush=True)

                    total_relay_s = round(time.perf_counter() - t0, 3)
                    open_req.t_total_provider1_relay = total_relay_s

                    try:
                        if open_req.created_at:
                            created_ts = open_req.created_at.timestamp()
                            provider_ts = pdf_received_ts
                            open_req.provider_processing_time = round(provider_ts - created_ts, 3)
                    except Exception as e:
                        print("API_PROVIDER_PROCESSING_TIME_ERROR =", str(e), flush=True)

                    try:
                        delivered_ts = time.time()
                        created_ts = open_req.created_at.timestamp()
                        open_req.total_delivery_time = round(delivered_ts - created_ts, 3)
                    except Exception as e:
                        print("API_TOTAL_DELIVERY_TIME_ERROR =", str(e), flush=True)

                    db.commit()

                    print("API_PROVIDER1_DONE_NO_WHATSAPP =", open_req.id, flush=True)
                    return {"ok": True, "provider_result": "api_pdf_stored_done"}

                if _request_is_no_accounting_main(db, open_req):
                    print(
                        "PRIVATE_PROVIDER_SKIP_ACCOUNTING_MAIN_RELAY =",
                        {
                            "req_id": open_req.id,
                            "instance_name": open_req.instance_name,
                            "provider_name": open_req.provider_name,
                            "source_group_id": open_req.source_group_id,
                        },
                        flush=True,
                    )
                else:
                    try:
                        if open_req.instance_name:
                            used, limit_value, blocked_now = increment_bot_used_and_maybe_block(
                                db,
                                open_req.instance_name
                            )
                            print("BOT_USED_AFTER_DONE =", used, flush=True)
                            print("BOT_LIMIT =", limit_value, flush=True)
                            print("BOT_BLOCKED_NOW =", blocked_now, flush=True)
                        else:
                            print("BOT_INSTANCE_MISSING_FOR_REQ =", open_req.id, flush=True)
                
                    except Exception as bot_limit_exc:
                        print("BOT_LIMIT_UPDATE_ERROR =", str(bot_limit_exc), flush=True)
                
                    t3 = time.perf_counter()
                    try:
                         from app.worker import _handle_group_promotion_after_done
                         _handle_group_promotion_after_done(open_req, db)
                    except Exception as promo_exc:
                         print("PROMOTION_UPDATE_ERROR =", str(promo_exc), flush=True)
                    finally:
                         print("T_PROMO =", round(time.perf_counter() - t3, 3), flush=True)

                total_relay_s = round(time.perf_counter() - t0, 3)
                open_req.t_total_provider1_relay = total_relay_s

                try:
                    if open_req.created_at:
                        created_ts = open_req.created_at.timestamp()
                        provider_ts = pdf_received_ts
                        open_req.provider_processing_time = round(provider_ts - created_ts, 3)
                        print("PROVIDER_PROCESSING_TIME =", open_req.provider_processing_time, flush=True)
                except Exception as e:
                    print("PROVIDER_PROCESSING_TIME_ERROR =", str(e), flush=True)

                try:
                    delivered_ts = time.time()
                    created_ts = open_req.created_at.timestamp()
                    open_req.total_delivery_time = round(delivered_ts - created_ts, 3)
                    print("TOTAL_DELIVERY_TIME =", open_req.total_delivery_time, flush=True)
                except Exception as e:
                    print("TOTAL_DELIVERY_TIME_ERROR =", str(e), flush=True)
                
                t_save = time.perf_counter()
                db.commit()
                print("T_DB_COMMIT_FINAL_METRICS =", round(time.perf_counter() - t_save, 3), flush=True)
                
                print("T_TOTAL_PROVIDER1_RELAY =", total_relay_s, flush=True)
                print("PROVIDER1_PDF_RELAYED =", open_req.id, time.time(), flush=True)
        
                return {"ok": True, "provider_result": "pdf_delivered"}

            # 2) SI NO HAY PDF, INTENTAR TEXTO
            # Este bloque es SOLO para mensajes individuales tipo PROVIDER_NO_RECORD_TEXT:
            # - GACG640211HOCLSD00    No hay registros disponibles
            # - MALA530523HGRRPL04    No hay registros disponibles
            # - CAUS580930MGRLRF02    No hay registros disponibles
            # - 09007002520260158490No hay registros disponibles
            #
            # NO es para replies tipo: "SIN", "VERIFICAR", "NO ESTÁ".
            # Esos ya se manejan arriba por quoted_msg_id.
            if text_body and _is_no_record_message(text_upper):
                no_record_ids = _extract_provider_no_record_identifiers(text_body)
            
                # Fallback viejo: por si viene un solo dato y extract_identifier_loose sí lo detectó.
                if not no_record_ids and provider_id:
                    no_record_ids = [provider_id]
            
                if not no_record_ids:
                    print("PROVIDER_NO_RECORD_WITHOUT_IDENTIFIER =", {
                        "text_body": text_body,
                        "source_chat_id": source_chat_id,
                    }, flush=True)
            
                    return {"ok": True, "ignored": "provider_no_record_without_identifier"}
            
                matched_req_ids = []
                unmatched_ids = []
            
                for no_record_id in no_record_ids:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.curp == no_record_id,
                            RequestLog.status == "PROCESSING",
                        )
                        .order_by(RequestLog.created_at.asc())
                        .first()
                    )
            
                    if not open_req:
                        open_req = (
                            db.query(RequestLog)
                            .filter(
                                RequestLog.curp == no_record_id,
                                RequestLog.status == "PROCESSING",
                            )
                            .order_by(RequestLog.created_at.desc())
                            .first()
                        )
            
                        print("PROVIDER_NO_RECORD_FALLBACK_MATCH =", {
                            "provider_id": no_record_id,
                            "matched_req_id": getattr(open_req, "id", None),
                            "matched_provider_group_id": getattr(open_req, "provider_group_id", None),
                            "matched_source_group_id": getattr(open_req, "source_group_id", None),
                            "matched_instance_name": getattr(open_req, "instance_name", None),
                        }, flush=True)
            
                    if not open_req:
                        unmatched_ids.append(no_record_id)
                        continue
            
                    print("PROVIDER_NO_RECORD_MATCHED_REQ_ID =", open_req.id, flush=True)
                    print("PROVIDER_NO_RECORD_MATCHED_CURP =", open_req.curp, flush=True)
            
                    open_req.status = "ERROR"
                    open_req.error_message = "SIN REGISTRO"
                    open_req.updated_at = _utc_now_naive()
                    db.commit()
            
                    _deliver_text_result(
                        open_req,
                        (
                            f"❌ No hay registros disponibles.\n"
                            f"Dato: {open_req.curp}\n"
                            f"Tipo: {open_req.act_type}\n\n"
                            f"Verificar que la CURP esté certificada en RENAPO"
                        ),
                    )
            
                    matched_req_ids.append(open_req.id)
            
                print("PROVIDER_NO_RECORD_TEXT_LIST_RESULT =", {
                    "ids_detected": no_record_ids,
                    "matched_req_ids": matched_req_ids,
                    "unmatched_ids": unmatched_ids,
                }, flush=True)
            
                if matched_req_ids:
                    return {
                        "ok": True,
                        "provider_result": "provider_no_record_text_matched",
                        "matched_req_ids": matched_req_ids,
                        "unmatched_ids": unmatched_ids,
                    }
            
                print("PROVIDER_NO_RECORD_WITHOUT_MATCH =", {
                    "text_body": text_body,
                    "ids_detected": no_record_ids,
                    "source_chat_id": source_chat_id,
                }, flush=True)
            
                return {"ok": True, "ignored": "provider_no_record_without_match"}
            
            print("PROVIDER_RAW_MESSAGE_KEYS =", list(message.keys()), flush=True)
            print("PROVIDER_RAW_MESSAGE =", message, flush=True)
            print("PROVIDER_UNHANDLED_MESSAGE =", message, flush=True)
            return {"ok": True, "ignored": "provider_unhandled_message"}

        if not terms and not is_admin_command:
            if problem:
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, problem, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, problem, instance_name=instance_name)
        
                return {"ok": True, "ignored": "invalid_identifier"}
        
            # Conversación natural: no marcar como error
            return {"ok": True, "ignored": "natural_text"}

        # =========================
        # COMANDOS ADMIN
        # =========================
        if text_upper.startswith("/ADDUSER "):
            if not _is_admin(requester_wa_id, from_me):
                print("ADDUSER_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            wa = text_upper.replace("/ADDUSER", "").strip()
            if wa and not db.query(AuthorizedUser).filter_by(wa_id=wa).first():
                db.add(AuthorizedUser(wa_id=wa))
                db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, f"✅ Usuario autorizado: {wa}")
            return {"ok": True}

        if text_upper.startswith("/RMUSER "):
            if not _is_admin(requester_wa_id, from_me):
                print("RMUSER_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            wa = text_upper.replace("/RMUSER", "").strip()
            row = db.query(AuthorizedUser).filter_by(wa_id=wa).first()

            if row:
                db.delete(row)
                db.commit()
                _reply_to_origin(source_group_id, requester_wa_id, f"✅ Usuario eliminado: {wa}")
            else:
                _reply_to_origin(source_group_id, requester_wa_id, f"⚠️ Usuario no encontrado: {wa}")

            return {"ok": True}

        if text_upper.startswith("/GROUPID"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            if is_group:
                send_group_text(source_group_id, f"🆔 Group ID:\n{source_group_id}", instance_name=instance_name)
            else:
                send_text(requester_wa_id, "⚠️ Usa /GROUPID dentro de un grupo.", instance_name=instance_name)

            return {"ok": True}
        
        if text_upper.startswith("/ADDGROUP"):
            if not _is_admin(requester_wa_id, from_me):
                print("ADDGROUP_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            if is_group:
                if not db.query(AuthorizedGroup).filter_by(group_jid=source_group_id).first():
                    db.add(AuthorizedGroup(group_jid=source_group_id, group_name=""))
                    db.commit()

                send_group_text(source_group_id, f"✅ Grupo autorizado: {source_group_id}", instance_name=instance_name)

            return {"ok": True}

        if text_upper.startswith("/STATUS"):
            if not _is_admin(requester_wa_id, from_me):
                print("STATUS_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            total = db.query(RequestLog).count()
            pending = db.query(RequestLog).filter(RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])).count()
            done = db.query(RequestLog).filter(RequestLog.status == "DONE").count()
            errors = db.query(RequestLog).filter(RequestLog.status == "ERROR").count()

            _reply_to_origin(
                source_group_id,
                requester_wa_id,
                f"📊 Total: {total}\n⏳ Pendientes: {pending}\n✅ Entregadas: {done}\n❌ Error/Sin registro: {errors}"
            )
            return {"ok": True}

        if text_upper.startswith("/PENDING"):
            if not _is_admin(requester_wa_id, from_me):
                print("PENDING_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            rows = db.query(RequestLog).filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])
            ).order_by(RequestLog.created_at.desc()).limit(15).all()

            if not rows:
                _reply_to_origin(source_group_id, requester_wa_id, "✅ No hay pendientes.")
            else:
                body = "\n".join([f"{r.id} | {r.curp} | {r.act_type} | {r.status}" for r in rows])
                _reply_to_origin(source_group_id, requester_wa_id, f"⏳ Pendientes:\n{body}")

            return {"ok": True}

        if text_upper.startswith("/QUEUE"):
            if not _is_admin(requester_wa_id, from_me):
                print("QUEUE_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            rows = db.query(RequestLog).filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])
            ).order_by(RequestLog.created_at.desc()).limit(15).all()

            if not rows:
                _reply_to_origin(source_group_id, requester_wa_id, "✅ No hay pendientes.")
            else:
                body = "\n".join([f"{r.id} | {r.curp} | {r.act_type} | {r.status}" for r in rows])
                _reply_to_origin(source_group_id, requester_wa_id, f"⏳ Pendientes:\n{body}")

            return {"ok": True}

        if text_upper.startswith("/LAST "):
            if not _is_admin(requester_wa_id, from_me):
                print("LAST_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            curp = text_upper.replace("/LAST", "").strip()
            last = (
                db.query(RequestLog)
                .filter(RequestLog.curp == curp, RequestLog.status == "DONE")
                .order_by(RequestLog.created_at.desc())
                .first()
            )

            if last and last.pdf_url:
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_document(
                            source_group_id,
                            last.pdf_url,
                            filename=f"{last.curp}.pdf",
                            caption="♻️ Reenviado desde historial",
                            instance_name=instance_name,
                        )
                else:
                    send_document(
                        requester_wa_id,
                        last.pdf_url,
                        filename=f"{last.curp}.pdf",
                        caption="♻️ Reenviado desde historial",
                        instance_name=instance_name,
                    )
            else:
                _reply_to_origin(source_group_id, requester_wa_id, "⚠️ No encontré PDF reciente para ese dato.")

            return {"ok": True}

        if text_upper.startswith("/REQUEUE "):
            if not _is_admin(requester_wa_id, from_me):
                print("REQUEUE_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            curp = text_upper.replace("/REQUEUE", "").strip()
            last = (
                db.query(RequestLog)
                .filter(RequestLog.curp == curp)
                .order_by(RequestLog.created_at.desc())
                .first()
            )

            if not last:
                _reply_to_origin(source_group_id, requester_wa_id, "⚠️ No encontré solicitud previa para ese dato.")
            else:
                last.status = "QUEUED"
                last.updated_at = _utc_now_naive()
                db.commit()
                _enqueue_process_request(last, "requeue_last")
                _reply_to_origin(source_group_id, requester_wa_id, f"🔁 Reintentando folio {last.id}")

            return {"ok": True}

        if text_upper.startswith("/PROVIDERS"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            _reply_to_origin(source_group_id, requester_wa_id, _providers_status_text(db))
            return {"ok": True}

        if text_upper in ("/P1 ON", "/PROVIDER1 ON"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER1", True)
            row.is_enabled = True
            row.updated_at = _utc_now_naive()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER1 activado")
            return {"ok": True}

        if text_upper in ("/P1 OFF", "/PROVIDER1 OFF"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER1", True)
            row.is_enabled = False
            row.updated_at = _utc_now_naive()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER1 desactivado")
            return {"ok": True}

        if text_upper in ("/P2 ON", "/PROVIDER2 ON"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER2", False)
            row.is_enabled = True
            row.updated_at = _utc_now_naive()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER2 activado")
            return {"ok": True}

        if text_upper in ("/P2 OFF", "/PROVIDER2 OFF"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER2", False)
            row.is_enabled = False
            row.updated_at = _utc_now_naive()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER2 desactivado")
            return {"ok": True}
        
        # =========================
        # FLUJO NORMAL DE USUARIO
        # =========================
        ALLOW_LEGACY_KNOWN_GROUPS = True
        if is_group:
            group_allowed = (
                is_authorized_group(db, source_group_id)
                or (ALLOW_LEGACY_KNOWN_GROUPS and is_legacy_known_group(db, source_group_id))
            )
        
            if not group_allowed:
                print("IGNORED_REASON = group_not_authorized", flush=True)
                print("IGNORED_GROUP =", source_group_id, flush=True)
                return {"ok": True, "ignored": "group_not_authorized"}

        ALLOW_PRIVATE_TEMP = False
        if not is_group and (not ALLOW_PRIVATE_TEMP or not is_authorized_user(db, requester_wa_id)):
            print("IGNORED_REASON = user_not_authorized", flush=True)
            print("IGNORED_USER =", requester_wa_id, flush=True)
            return {"ok": True, "ignored": "user_not_authorized"}
        
        if not text_body:
            print("IGNORED_REASON = no_text", flush=True)
            return {"ok": True, "ignored": "no_text"}
        
        print("REQUEST_TEXT =", text_body, flush=True)
        print("REQUEST_TERMS =", terms, flush=True)
        
        if not terms:
            print("IGNORED_REASON = no_identifier", flush=True)
        
            problem_msg = detect_identifier_problem(text_body)
        
            if problem_msg:
                final_msg = problem_msg
        
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, final_msg, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, final_msg, instance_name=instance_name)
        
            return {"ok": True, "ignored": "no_identifier"}
        
        act_type = detect_act_type(text_body)
        print("REQUEST_ACT_TYPE =", act_type, flush=True)

        created_any = False

        for term in terms:
            print("PROCESSING_TERM =", term, flush=True)
        
            #last_done = get_last_done_request(db, term, act_type)
            last_req = _get_latest_request(db, term, act_type, source_chat_id)

            if last_req:
                print(
                    "LAST_REQ_FOUND =",
                    {
                        "id": last_req.id,
                        "status": last_req.status,
                        "term": term,
                        "act_type": act_type,
                    },
                    flush=True,
                )

                if last_req.status == "DONE":
                    if should_notify_done(source_group_id):
                        done_msg = (
                            f"✅ Esta acta ya fue entregada\n"
                            f"Dato: {term}\n"
                            f"Tipo: {act_type}"
                        )
    
                        if source_group_id:
                            if should_send_extra_text(source_group_id):
                                send_group_text(source_group_id, done_msg, instance_name=instance_name)
                        else:
                            send_text(requester_wa_id, done_msg, instance_name=instance_name)
    
                        continue
        
            base_request_key = build_request_key(term, act_type, source_chat_id)
            day_start, day_end = _bot_day_bounds()
        
            # contar intentos previos de ese mismo dato/tipo/grupo
            same_requests_count = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.source_chat_id == source_chat_id,
                    RequestLog.created_at >= day_start,
                    RequestLog.created_at < day_end,
                )
                .count()
            )
        
            # máximo 3 intentos
            if same_requests_count >= 3:
                limit_msg = (
                    f"⚠️ Ya alcanzaste el máximo de intentos para este dato.\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}"
                )
        
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, limit_msg, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, limit_msg, instance_name=instance_name)
        
                continue
        
            # request_key único por intento
            request_key = f"{base_request_key}:{_mx_now().strftime('%Y%m%d')}:try_{same_requests_count + 1}:{uuid.uuid4().hex[:6]}"

            # ============================================================
            # RESULTADO DEFINITIVO PREVIO: SIN REGISTRO
            # Se revisa antes de reutilizar errores o crear otra solicitud.
            # Comparte el resultado entre grupos pertenecientes al mismo bot.
            # ============================================================
            no_record_existing = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.instance_name == instance_name,
                    RequestLog.status == "ERROR",
                    or_(
                        RequestLog.error_message.ilike("%SIN REGISTRO%"),
                        RequestLog.error_message.ilike("%SIN_REGISTRO%"),
                        RequestLog.error_message.ilike("%NO_RECORD%"),
                        RequestLog.error_message.ilike("%NO RECORD%"),
                        RequestLog.error_message.ilike("%NO_REGISTRO%"),
                        RequestLog.error_message.ilike("%NO REGISTRO%"),
                        RequestLog.error_message.ilike("%NO HAY REGISTRO%"),
                        RequestLog.error_message.ilike("%NO HAY REGISTROS%"),
                        RequestLog.error_message.ilike("%NO_LOCALIZADO%"),
                        RequestLog.error_message.ilike("%NO LOCALIZADO%"),
                        RequestLog.error_message.ilike("%ACTA NO LOCALIZADA%"),
                        RequestLog.error_message.ilike("%CURP INEXISTENTE%"),
                    ),
                )
                .order_by(
                    RequestLog.updated_at.desc(),
                    RequestLog.id.desc(),
                )
                .first()
            )
            
            if no_record_existing:
                no_record_msg = (
                    "❌ No hay registros disponibles.\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}\n\n"
                    "Verificar que la CURP esté certificada en RENAPO"
                )
            
                print(
                    "GLOBAL_NO_RECORD_RETURNED_WITHOUT_RETRY =",
                    {
                        "matched_request_id": no_record_existing.id,
                        "term": term,
                        "act_type": act_type,
                        "instance_name": instance_name,
                        "current_source_chat_id": source_chat_id,
                        "original_source_chat_id": no_record_existing.source_chat_id,
                        "error_message": no_record_existing.error_message,
                    },
                    flush=True,
                )
            
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(
                            source_group_id,
                            no_record_msg,
                            instance_name=instance_name,
                        )
                else:
                    send_text(
                        requester_wa_id,
                        no_record_msg,
                        instance_name=instance_name,
                    )
            
                continue
        
            # 2) si existe una anterior en ERROR, reutilizar SOLO la más reciente en error
            error_existing = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.source_chat_id == source_chat_id,
                    RequestLog.status == "ERROR",
                    or_(
                        RequestLog.error_message.is_(None),
                        RequestLog.error_message != "AUTO_TIMEOUT_OPEN_REQUEST_DO_NOT_REUSE",
                    )
                )
                .order_by(RequestLog.created_at.desc())
                .first()
            )
        
            if error_existing:
                existing_error_upper = (
                    error_existing.error_message or ""
                ).strip().upper()
            
                existing_is_no_record = any(
                    marker in existing_error_upper
                    for marker in (
                        "SIN REGISTRO",
                        "SIN_REGISTRO",
                        "NO_RECORD",
                        "NO RECORD",
                        "NO_REGISTRO",
                        "NO REGISTRO",
                        "NO_LOCALIZADO",
                        "NO LOCALIZADO",
                        "NO HAY REGISTRO",
                        "NO HAY REGISTROS",
                        "ACTA NO LOCALIZADA",
                        "CURP INEXISTENTE",
                    )
                )
            
                if existing_is_no_record:
                    no_record_msg = (
                        "❌ No hay registros disponibles.\n"
                        f"Dato: {term}\n"
                        f"Tipo: {act_type}\n\n"
                        "Verificar que la CURP esté certificada en RENAPO"
                    )
            
                    print(
                        "EXISTING_NO_RECORD_RETURNED_WITHOUT_RETRY =",
                        {
                            "request_id": error_existing.id,
                            "term": term,
                            "act_type": act_type,
                            "source_chat_id": source_chat_id,
                            "error_message": error_existing.error_message,
                        },
                        flush=True,
                    )
            
                    if source_group_id:
                        if should_send_extra_text(source_group_id):
                            send_group_text(
                                source_group_id,
                                no_record_msg,
                                instance_name=instance_name,
                            )
                    else:
                        send_text(
                            requester_wa_id,
                            no_record_msg,
                            instance_name=instance_name,
                        )
            
                    # Muy importante:
                    # no cambiar el ERROR, no borrar error_message,
                    # no poner QUEUED y no enviar nuevamente al proveedor.
                    continue
            
                error_existing.request_key = request_key
                error_existing.curp = term
                error_existing.act_type = act_type
                error_existing.status = "QUEUED"
                error_existing.updated_at = _utc_now_naive()
                error_existing.error_message = None
                error_existing.evolution_message_id = msg_id
                error_existing.requester_wa_id = requester_wa_id
                error_existing.requester_name = ""
                error_existing.source_chat_id = source_chat_id
                error_existing.source_group_id = source_group_id
                error_existing.instance_name = instance_name
                #error_existing.provider_name = None
                #error_existing.provider_group_id = None
                error_existing.provider_message = None
                
                now_utc = _utc_now_naive()

                error_existing.provider_media_url = None
                error_existing.pdf_url = None
                
                #error_existing.created_at = now_utc
                error_existing.updated_at = now_utc
                error_existing.expires_at = now_utc + timedelta(days=settings.HISTORY_DAYS)
                
                db.commit()
                
                _enqueue_process_request(error_existing, "requeue_error_existing")
                created_any = True
        
                print("REQUEUED_EXISTING_REQUEST_ID =", error_existing.id, flush=True)
                print("REQUEUED_EXISTING_TERM =", error_existing.curp, flush=True)
                print("REQUEUED_EXISTING_TYPE =", error_existing.act_type, flush=True)
        
                retry_msg = (
                    f"🔁 Reintentando solicitud\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}"
                )
        
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, retry_msg, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, retry_msg, instance_name=instance_name)
        
                continue
        
            # 3) si no existe, crear nueva
            row = RequestLog(
                request_key=request_key,
                curp=term,
                act_type=act_type,
                requester_wa_id=requester_wa_id,
                requester_name="",
                source_chat_id=source_chat_id,
                source_group_id=source_group_id,
                instance_name=instance_name,
                evolution_message_id=msg_id,
                status="QUEUED",
                created_at=_utc_now_naive(),
                updated_at=_utc_now_naive(),
                expires_at=_utc_now_naive() + timedelta(days=settings.HISTORY_DAYS),
            )
        
            db.add(row)
            try:
                db.commit()
                db.refresh(row)
            except IntegrityError:
                db.rollback()

                row = RequestLog(
                    request_key=f"{base_request_key}:{uuid.uuid4().hex}",
                    curp=term,
                    act_type=act_type,
                    requester_wa_id=requester_wa_id,
                    requester_name="",
                    source_chat_id=source_chat_id,
                    source_group_id=source_group_id,
                    instance_name=instance_name,
                    evolution_message_id=msg_id,
                    status="QUEUED",
                    created_at=_utc_now_naive(),
                    updated_at=_utc_now_naive(),
                    expires_at=_utc_now_naive() + timedelta(days=settings.HISTORY_DAYS),
                )
            
                db.add(row)
                db.commit()
                db.refresh(row)
                    
            _enqueue_process_request(row, "manual_requeue")
            created_any = True

            print(
                "WEBHOOK_TIMING_AFTER_ENQUEUE =",
                {
                    "request_id": row.id,
                    "term": row.curp,
                    "act_type": row.act_type,
                    "elapsed": round(time.perf_counter() - webhook_t0, 3),
                },
                flush=True,
            )
        
            print("ENQUEUED_REQUEST_ID =", row.id, flush=True)
            print("ENQUEUED_TERM =", row.curp, flush=True)
            print("ENQUEUED_TYPE =", row.act_type, flush=True)
            print("ENQUEUED_SOURCE_GROUP =", row.source_group_id, flush=True)

        if created_any:
            actor = push_name or requester_wa_id
        
            bot_name = BOT_LABELS.get(instance_name)
        
            if not bot_name:
                bot_row = (
                    db.query(BotControl)
                    .filter(BotControl.instance_name == instance_name)
                    .first()
                )
                bot_name = bot_row.label if bot_row and bot_row.label else "🚀 DOCU EXPRES"
        
            ack_msg = (
                f"{bot_name}\n"
                f"Solicitud recibida de {actor}.\n"
                f"Esto puede tardar unos segundos..."
            )

            ack_t0 = time.perf_counter()

            print(
                "WEBHOOK_TIMING_ACK_START =",
                {
                    "source_group_id": source_group_id,
                    "requester_wa_id": requester_wa_id,
                    "instance_name": instance_name,
                    "elapsed_before_ack": round(ack_t0 - webhook_t0, 3),
                },
                flush=True,
            )
        
            try:
                if source_group_id:
                    ack_queue.enqueue(
                        send_group_text,
                        source_group_id,
                        ack_msg,
                        instance_name=instance_name,
                        job_timeout=180,
                    )
                else:
                    ack_queue.enqueue(
                        send_text,
                        requester_wa_id,
                        ack_msg,
                        instance_name=instance_name,
                        job_timeout=180,
                    )
            
                print(
                    "WEBHOOK_ACK_ENQUEUED =",
                    {
                        "source_group_id": source_group_id,
                        "requester_wa_id": requester_wa_id,
                        "instance_name": instance_name,
                    },
                    flush=True,
                )
            
            except Exception as ack_enqueue_exc:
                print("WEBHOOK_ACK_ENQUEUE_ERROR =", str(ack_enqueue_exc), flush=True)

            print(
                "WEBHOOK_TIMING_ACK_DONE =",
                {
                    "ack_send_seconds": round(time.perf_counter() - ack_t0, 3),
                    "total_webhook_seconds": round(time.perf_counter() - webhook_t0, 3),
                },
                flush=True,
            )
            
        else:
            print("IGNORED_REASON = nothing_created", flush=True)

        print(
            "WEBHOOK_TIMING_RETURN_OK =",
            {
                "total_webhook_seconds": round(time.perf_counter() - webhook_t0, 3),
            },
            flush=True,
        )

        return {"ok": True}

    except Exception as e:
        try:
            release_webhook_msg_seen(
                locals().get("msg_id", ""),
                locals().get("instance_name", ""),
            )
        except Exception as release_exc:
            print(
                "WEBHOOK_GLOBAL_DEDUPE_RELEASE_ERROR =",
                {
                    "error": repr(release_exc),
                },
                flush=True,
            )

        print(
            "WEBHOOK_GLOBAL_ERROR =",
            {
                "error": repr(e),
                "instance_name": locals().get("instance_name", ""),
                "msg_id": locals().get("msg_id", ""),
                "remote_jid": locals().get("remote_jid", ""),
                "message_keys": (
                    list(locals().get("message", {}).keys())
                    if isinstance(locals().get("message"), dict)
                    else []
                ),
            },
            flush=True,
        )

        return {
            "ok": False,
            "error": str(e),
        }
