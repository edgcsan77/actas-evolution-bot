import os
import base64
import time
import random
import asyncio
import re
import uuid
import json
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import Counter

from fastapi import FastAPI, Depends, Body, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import Base, engine, get_db, SessionLocal
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog, ProviderSetting, AppSetting, GroupPromotion, GroupAlias, GroupCategory, BotControl
from app.queue import request_queue, redis_conn, broadcast_queue
from app.worker import process_request, provider3_keepalive_job, _validate_act_type_pdf, _validate_pdf_matches_term, _notify_support_error
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

from sqlalchemy import func, case, or_
from app.broadcast_jobs import botpanel_broadcast_job

app = FastAPI(title=settings.APP_NAME)
PANEL_TZ = "America/Monterrey"
BLOCKED_GROUPS_KEY = "blocked_groups_no_response"
BLOCKED_INSTANCES_KEY = "blocked_instances_no_response"

PANEL_HTML_TTL = 180
PANEL_RECENT_TTL = 60
PANEL_GROUP_DETAIL_TTL = 180
GROUP_NAME_CACHE_TTL = 300
PANEL_STREAM_SLEEP = 5
PANEL_STREAM_ENABLED = True

EVOLUTION_BASE_URL = "http://127.0.0.1:8080"
EVOLUTION_APIKEY = "DOCIFY_EVOLUTION_KEY_2026"

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
    try:
        url = f"{EVOLUTION_BASE_URL}/instance/connectionState/{instance_name}"

        r = requests.get(
            url,
            headers={"apikey": EVOLUTION_APIKEY},
            timeout=8,
        )

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        print("EVOLUTION_STATE_DEBUG =", instance_name, r.status_code, data, flush=True)

        state = "unknown"

        if isinstance(data, dict):
            state = (
                data.get("instance", {}).get("state")
                or data.get("state")
                or data.get("connectionState")
                or data.get("status")
                or "unknown"
            )

        return {
            "ok": r.status_code < 400,
            "state": str(state or "unknown").lower(),
            "raw": data,
        }

    except Exception as e:
        print("EVOLUTION_STATE_ERROR =", instance_name, repr(e), flush=True)
        return {
            "ok": False,
            "state": "unknown",
            "error": str(e),
        }
        

def _evolution_connect_qr(instance_name: str) -> dict:
    try:
        url = f"{EVOLUTION_BASE_URL}/instance/connect/{instance_name}"

        r = requests.get(
            url,
            headers={"apikey": EVOLUTION_APIKEY},
            timeout=15,
        )

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        print("EVOLUTION_QR_DEBUG =", instance_name, r.status_code, data, flush=True)

        return {
            "ok": r.status_code < 400,
            "status_code": r.status_code,
            "data": data,
        }

    except Exception as e:
        print("EVOLUTION_QR_ERROR =", instance_name, repr(e), flush=True)
        return {
            "ok": False,
            "error": str(e),
        }


def _bot_status_rows(db: Session) -> list[dict]:
    bots = sorted(set(BOT_LABELS.keys()) | set(BOT_PANEL_TOKENS.values()))

    out = []
    for inst in bots:
        total = (
            db.query(RequestLog)
            .filter(RequestLog.instance_name == inst)
            .count()
        )

        used = get_bot_used(db, inst)
        limit_value = get_bot_limit(db, inst)
        blocked = is_instance_blocked(inst)
        ev = _evolution_instance_state(inst)

        out.append({
            "instance_name": inst,
            "label": bot_label(inst),
            "state": ev.get("state", "unknown"),
            "evolution_ok": bool(ev.get("ok")),
            "blocked": blocked,
            "used": used,
            "limit": limit_value,
            "available": max(0, limit_value - used) if limit_value > 0 else None,
            "total_requests": total,
        })

    return out


@app.get("/panel/instance/{instance_name}/qr")
def panel_instance_qr(instance_name: str):
    result = _evolution_connect_qr(instance_name)
    return result


def _is_valid_admin_panel_token(request: Request) -> bool:
    token = (request.query_params.get("token") or "").strip()
    expected = (settings.ADMIN_PANEL_TOKEN or "").strip()
    return bool(expected) and token == expected


def _bot_instance_from_token(token: str) -> str | None:
    return BOT_PANEL_TOKENS.get((token or "").strip())


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
    "PROVIDER4": "LAZARO WEB",
    "PROVIDER5": "LUIS SID",
    "PROVIDER6": "ACTAS ESCALANTE",
    "PROVIDER7": "MESINO SID",
    "PROVIDER8": "VILLAFUERTE",
}


BOT_LABELS = {
    "docifybot8": "🚀 DOCU EXPRES",
    "docifybot8max": "☄️ MAX BOT",
    "docifybot8docify": "👽 DOCIFY MX",
    "docifybot8cristina": "🌸 ACTAS MAYOREO",
    "docifybot8maya": "🔱 GESTORIA MAYA",
    "docifybot8leli": "🌼 TRAMITES LELI",
    "docifybot8rywya": "🌹 GESTORIA EXPRESS RYWYA",
    "docifybot8xpress": "⚡ DIGITAL XPRESS",
}


BOT_PANEL_TOKENS = {
    "t777fgh6j5": "docifybot8",
    "4a8c92a1e7": "docifybot8max",
    "asd5a6d7g9": "docifybot8docify",
    "63df2dgdae": "docifybot8cristina",
    "as5613f4se": "docifybot8maya",
    "65as6d8fg9": "docifybot8leli",
    "dg5f5f6g3s": "docifybot8rywya",
    "df48r8dg62": "docifybot8xpress",
}


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


def _bot_title(instance_name: str) -> str:
    return BOT_LABELS.get(instance_name, instance_name)


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


def _get_bot_group_name(db: Session, group_jid: str) -> str:
    alias = db.query(GroupAlias).filter_by(group_jid=group_jid).first()
    if alias and (alias.custom_name or "").strip():
        return alias.custom_name.strip()
    return _group_name(group_jid)


def _bot_groups_for_instance(db: Session, instance_name: str):
    hidden_group_ids = {
        g.group_jid
        for g in (
            db.query(AuthorizedGroup.group_jid)
            .filter(AuthorizedGroup.is_hidden == True)
            .all()
        )
    }

    rows = (
        db.query(RequestLog.source_group_id)
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.source_group_id.isnot(None),
        )
        .distinct()
        .all()
    )

    groups = []
    for (group_jid,) in rows:
        if not group_jid:
            continue
        if group_jid in hidden_group_ids:
            continue
        groups.append(SimpleNamespace(group_jid=group_jid))

    groups.sort(key=lambda x: (_get_bot_group_name(db, x.group_jid) or "").lower())
    return groups


def _bot_day_bounds():
    now_local = _mx_now()
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return _panel_to_utc_naive(start_local), _panel_to_utc_naive(end_local)


def _bot_month_30d_start():
    now_local = _mx_now()
    start_local = now_local - timedelta(days=29)
    start_local = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return _panel_to_utc_naive(start_local)


def _bot_sales_today(db: Session, instance_name: str) -> int:
    start_utc, end_utc = _bot_day_bounds()
    return (
        db.query(RequestLog)
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
            RequestLog.created_at < end_utc,
        )
        .count()
    )


def _bot_sales_month(db: Session, instance_name: str) -> int:
    start_utc = _bot_month_30d_start()
    return (
        db.query(RequestLog)
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
        )
        .count()
    )


def _bot_sales_history_30d(db: Session, instance_name: str):
    start_utc = _bot_month_30d_start()
    rows = (
        db.query(
            func.date(RequestLog.created_at).label("day"),
            func.count(RequestLog.id).label("total"),
        )
        .filter(
            RequestLog.instance_name == instance_name,
            RequestLog.status == "DONE",
            RequestLog.created_at >= start_utc,
        )
        .group_by(func.date(RequestLog.created_at))
        .order_by(func.date(RequestLog.created_at).desc())
        .all()
    )
    return rows


def _bot_group_stats(db: Session, instance_name: str):
    start_day, end_day = _bot_day_bounds()
    start_30d = _bot_month_30d_start()

    groups = _bot_groups_for_instance(db, instance_name)
    out = []

    for g in groups:
        today_done = (
            db.query(RequestLog)
            .filter(
                RequestLog.instance_name == instance_name,
                RequestLog.source_group_id == g.group_jid,
                RequestLog.status == "DONE",
                RequestLog.created_at >= start_day,
                RequestLog.created_at < end_day,
            )
            .count()
        )

        month_done = (
            db.query(RequestLog)
            .filter(
                RequestLog.instance_name == instance_name,
                RequestLog.source_group_id == g.group_jid,
                RequestLog.status == "DONE",
                RequestLog.created_at >= start_30d,
            )
            .count()
        )

        promo = db.query(GroupPromotion).filter_by(group_jid=g.group_jid).first()

        out.append({
            "group_jid": g.group_jid,
            "group_name": _get_bot_group_name(db, g.group_jid),
            "today_done": today_done,
            "month_done": month_done,
            "blocked": is_group_blocked(g.group_jid),
            "promo_total": int(promo.total_actas or 0) if promo else 0,
            "promo_used": int(promo.used_actas or 0) if promo else 0,
            "promo_active": bool(promo.is_active) if promo else False,
        })

    out.sort(key=lambda x: (-x["month_done"], x["group_name"].lower()))
    return out


def bot_label(inst):
    if not inst:
        return ""
    return BOT_LABELS.get(inst.lower(), inst)


def _provider_label(name: str) -> str:
    if not name:
        return ""
    return PROVIDER_LABELS.get(name, name)


def _day_name_es_from_date(day_str: str) -> str:
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    return DAYS_ES[dt.weekday()]


def is_group_blocked(group_jid: str) -> bool:
    if not group_jid:
        return False

    blocked = redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid)
    return bool(blocked)


def is_instance_blocked(instance_name: str) -> bool:
    if not instance_name:
        return False
    blocked = redis_conn.sismember(BLOCKED_INSTANCES_KEY, instance_name)
    return bool(blocked)


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
        instance_name = _bot_instance_from_token(token)
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
        used = get_bot_used(db, name)
        limit_value = get_bot_limit(db, name)
        blocked = is_instance_blocked(name)

        items.append({
            "instance_name": name,
            "total_requests": int(total or 0),
            "used": used,
            "limit": limit_value,
            "available": max(0, limit_value - used) if limit_value > 0 else None,
            "blocked": blocked,
        })

    items.sort(key=lambda x: x["instance_name"])
    return {"ok": True, "items": items}


@app.post("/panel/instance/{instance_name}/block")
def panel_block_instance(instance_name: str):
    block_instance(instance_name)
    _clear_panel_cache()
    return {"ok": True, "instance_name": instance_name, "blocked": True}


@app.post("/panel/instance/{instance_name}/unblock")
def panel_unblock_instance(instance_name: str):
    unblock_instance(instance_name)
    _clear_panel_cache()
    return {"ok": True, "instance_name": instance_name, "blocked": False}


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
    return {
        "ok": True,
        "instance_name": instance_name,
        "used": 0,
        "limit": get_bot_limit(db, instance_name),
    }


@app.post("/panel/instance/{instance_name}/recharge")
async def panel_recharge_instance(instance_name: str, request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    add_value = int(payload.get("amount") or 0)
    current_limit = get_bot_limit(db, instance_name)
    new_limit = current_limit + max(0, add_value)

    set_bot_limit(db, instance_name, new_limit)
    unblock_instance(instance_name)
    _clear_panel_cache()

    return {
        "ok": True,
        "instance_name": instance_name,
        "limit": new_limit,
        "used": get_bot_used(db, instance_name),
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
        keys = redis_conn.keys("panel:*")
        if keys:
            redis_conn.delete(*keys)
    except Exception:
        pass


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


def _panel_period_bounds(view: str):
    view = (view or "day").strip().lower()

    if view == "month":
        local_start = _panel_month_start()
        local_end = _panel_month_end()
        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)
        return utc_start, utc_end, "month"

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
    
            if gid in hidden_main_group_ids:
                continue
    
            row = db.query(AuthorizedGroup).filter_by(group_jid=gid).first()
            owner = (row.owner_instance or "").strip() if row else ""
            
            if gid != "PRIVADO" and not owner and _is_hidden_panel_group(gid, group_name):
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


def _panel_detail_for_group(rows: list[RequestLog], group_jid: str, view: str, db: Session) -> dict:
    days = {}

    now_local = _panel_now()
    view = (view or "day").strip().lower()

    if view == "month":
        local_start = _panel_month_start(now_local)
        local_end = _panel_month_end(now_local)
    else:
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
    db: Session = Depends(get_db),
):
    cache_key = "panel:recent_requests_html:" + "|".join([
        (view or "").strip(),
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

    time_min, time_max, view = _panel_period_bounds(view)
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
            <th>Grupo proveedor</th>
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
              <td>{_esc(bot_label(r.instance_name))}</td>
              <td>{_esc(_provider_label(r.provider_name))}</td>
              <td>{_esc(_group_name_cached(r.provider_group_id, group_cache))}</td>
              <td>{_esc(_fmt_dt(r.created_at))}</td>
              <td>{_esc(_fmt_dt(r.updated_at))}</td>
              <td class="small">{_esc(r.error_message)}</td>
            </tr>
            """
    else:
        html += '<tr><td colspan="11">Sin solicitudes en este periodo.</td></tr>'

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
        available = max(0, total_actas)

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
        row.used_actas = 0
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


def _get_broadcast_target_groups(db: Session, target_category: str, selected_groups: list[str] | None = None) -> list[str]:
    selected_groups = selected_groups or []

    all_groups = set(GROUP_NAME_MAP.keys())
    all_groups.update(gid for (gid,) in db.query(GroupAlias.group_jid).all())

    if target_category == "all":
        return sorted(all_groups)

    if target_category == "manual":
        return [g for g in selected_groups if g in all_groups]

    rows = (
        db.query(GroupCategory)
        .filter(GroupCategory.category == target_category)
        .all()
    )

    category_groups = [r.group_jid for r in rows if r.group_jid in all_groups]
    return sorted(category_groups)


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
    return {"ok": True}


@app.post("/panel/group/{group_jid}/category/remove")
def panel_group_remove_category(group_jid: str, db: Session = Depends(get_db)):
    _remove_group_category(db, group_jid)
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

        try:
            redis_conn.delete(f"panel:group_detail:{group_jid}:month")
            redis_conn.delete(f"panel:group_detail:{group_jid}:day")
        except Exception:
            pass

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
    db: Session = Depends(get_db),
):
    if not group_jid:
        return HTMLResponse("<pre>Falta group_jid</pre>", status_code=400)

    cache_key = f"panel:group_detail:{(group_jid or '').strip()}:{(view or 'month').strip()}"
    cached_html = redis_conn.get(cache_key)
    if cached_html:
        if isinstance(cached_html, bytes):
            cached_html = cached_html.decode("utf-8", errors="ignore")
        return HTMLResponse(content=cached_html)

    group_cache = _build_group_name_cache(db)

    promo = _get_group_promotion(db, group_jid)
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

    time_min, time_max, view = _panel_period_bounds(view)

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
    now_local = _panel_now()

    if view == "month":
        local_start = _panel_month_start(now_local)
        local_end = _panel_month_end(now_local)
    else:
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
    subtitle = (
        f"Historial mensual: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
        if view == "month"
        else f"Historial diario: {detail['date_from']} ({PANEL_TZ})"
    )

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
08:00 AM a 11:00 PM
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
    db: Session = Depends(get_db),
):
    time_min, time_max, view = _panel_period_bounds(view)

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
        "summary": summary,
        "by_group": by_group,
        "by_provider": by_provider,
        "by_type": by_type,
        "latest": latest,
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


def _run_broadcast_job(message_text: str, target_groups: list[str]):
    sent = []
    failed = []

    for gid in target_groups:
        try:
            send_group_text(gid, message_text)
            sent.append({
                "group_jid": gid,
                "group_name": _group_name(gid),
            })
        except Exception as e:
            failed.append({
                "group_jid": gid,
                "group_name": _group_name(gid),
                "error": str(e),
            })

    print(
        "BROADCAST_FINISHED",
        {
            "sent_count": len(sent),
            "failed_count": len(failed),
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

        background_tasks.add_task(_run_broadcast_job, BROADCAST_ACTIVAS_MSG, target_groups)

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

        background_tasks.add_task(_run_broadcast_job, BROADCAST_RESTABLECIDO_MSG, target_groups)

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

        background_tasks.add_task(_run_broadcast_job, BROADCAST_SUSPENDIDO_MSG, target_groups)

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

        background_tasks.add_task(_run_broadcast_job, BROADCAST_CERRADO_MSG, target_groups)

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
    instance_name = _bot_instance_from_token(token)

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
def botpanel_broadcast_progress(token: str, job_id: str):
    instance_name = _bot_instance_from_token(token)

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
        target_category = (payload.get("category") or "all").strip().lower()
        selected_groups = payload.get("selected_groups") or []

        if not message_text:
            return {"ok": False, "error": "Mensaje vacío"}

        target_groups = _get_broadcast_target_groups(db, target_category, selected_groups)

        if not target_groups:
            return {"ok": False, "error": "No hay grupos para esa categoría"}

        background_tasks.add_task(_run_broadcast_job, message_text, target_groups)

        return {
            "ok": True,
            "queued": True,
            "message": f"Envío masivo iniciado para {len(target_groups)} grupos",
        }

    except Exception as e:
        print("panel_broadcast_free error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}
        

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
        used_actas = int(r.used_actas or 0)
        available = max(0, total_actas - used_actas)
        shared_key = (r.shared_key or "").strip()
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
) -> str:
    return "panel:html:" + "|".join([
        (view or "").strip(),
        (group_jid or "").strip(),
        (provider_name or "").strip(),
        (status or "").strip(),
        (act_type or "").strip(),
        (group_mode or "").strip(),
    ])
                                                                                                        

def _panel_delivery_metrics(db, time_min, time_max):
    try:
        rows = (
            db.query(
                RequestLog.provider_processing_time,
                RequestLog.provider_to_webhook_lag_s,
                RequestLog.t_total_provider1_relay,
                RequestLog.total_delivery_time,
            )
            .filter(
                RequestLog.created_at >= time_min,
                RequestLog.created_at < time_max,
                RequestLog.provider_processing_time.isnot(None),
                RequestLog.provider_to_webhook_lag_s.isnot(None),
                RequestLog.t_total_provider1_relay.isnot(None),
                RequestLog.total_delivery_time.isnot(None),
            )
            .all()
        )

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


def _bot_credit_stats(db: Session, instance_name: str):
    try:
        limit_value = get_bot_limit(db, instance_name)
        used_value = get_bot_used(db, instance_name)

        available = max(limit_value - used_value, 0)

        return {
            "limit": limit_value,
            "used": used_value,
            "available": available,
            "recharges": 0
        }

    except Exception:
        return {
            "limit": 0,
            "used": 0,
            "available": 0,
            "recharges": 0
        }


@app.post("/botpanel/{token}/group/{group_jid}/block")
def panel_bot_block_group(token: str, group_jid: str, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(token)
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
        instance_name = _bot_instance_from_token(token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        _assert_group_owned_by_bot(db, group_jid, instance_name)
        unblock_group(group_jid)
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/{group_jid}/rename")
async def panel_bot_rename_group(token: str, group_jid: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(token)
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
        instance_name = _bot_instance_from_token(token)
        if not instance_name:
            return {"ok": False, "error": "Panel no válido"}

        payload = await request.json()
        group_jid = (payload.get("group_jid") or "").strip()
        promo_name = (payload.get("promo_name") or "").strip()
        total_actas = int(payload.get("total_actas") or 0)
        price_per_piece = (payload.get("price_per_piece") or "").strip()

        _assert_group_owned_by_bot(db, group_jid, instance_name)

        if total_actas < MIN_BOT_PROMO_ACTAS:
            return {"ok": False, "error": f"La promoción mínima es de {MIN_BOT_PROMO_ACTAS} actas"}

        row = db.query(GroupPromotion).filter_by(group_jid=group_jid).first()
        if row:
            row.promo_name = promo_name
            row.total_actas = total_actas
            row.used_actas = 0
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
                price_per_piece=price_per_piece,
                is_active=True,
                owner_instance=instance_name,
            )
            db.add(row)

        db.commit()
        _clear_panel_cache()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)}


@app.post("/botpanel/{token}/group/add")
async def panel_bot_add_group(token: str, request: Request, db: Session = Depends(get_db)):
    try:
        instance_name = _bot_instance_from_token(token)
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


@app.get("/botpanel/{token}")
def panel_bot(token: str, db: Session = Depends(get_db)):
    instance_name = _bot_instance_from_token(token)

    if not instance_name:
        return HTMLResponse("<h3>Panel no válido.</h3>", status_code=404)

    if not _is_child_bot(instance_name):
        return HTMLResponse("<h3>Este panel es solo para bots desde docifybot8 en adelante.</h3>", status_code=400)

    title = _bot_title(instance_name)
    today_sales = _bot_sales_today(db, instance_name)
    month_sales = _bot_sales_month(db, instance_name)
    history_rows = _bot_sales_history_30d(db, instance_name)
    groups = _bot_group_stats(db, instance_name)
    credits = _bot_credit_stats(db, instance_name)

    credits = credits or {}
    credits.setdefault("limit", 0)
    credits.setdefault("used", 0)
    credits["available"] = max(0, credits["limit"] - credits["used"])
    credits.setdefault("recharges", 0)

    groups = groups or []
    total_groups = len(groups)
    blocked_groups = sum(1 for g in groups if g["blocked"])
    active_promos = sum(1 for g in groups if g["promo_active"])

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
          <div class="head">
            <strong>Grupos del bot</strong>
            <span class="small">Bloquea, renombra y asigna promociones (mínimo 10 actas).</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Grupo</th>
                  <th>Hoy</th>
                  <th>30 días</th>
                  <th>Promoción</th>
                  <th>Estado</th>
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

            html += f"""
                <tr>
                  <td>
                    <strong>{_esc(g["group_name"])}</strong><br>
                    <span class="small">{_esc(g["group_jid"])}</span>
                  </td>
                  <td>{g["today_done"]}</td>
                  <td>{g["month_done"]}</td>
                  <td>{promo_text}</td>
                  <td>{status_badge}</td>

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
                      <button class="btn btn-success" onclick="assignBotPromo('{_esc(g["group_jid"])}')">Aplicar promo</button>
                    </div>
                  </td>

                  <td>
                    {block_btn}
                  </td>
                </tr>
            """
    else:
        html += '<tr><td colspan="8">Este bot aún no tiene grupos asignados.</td></tr>'

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
          if (data.ok) location.reload();
          else alert(data.error || "No se pudo aplicar la promoción");
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
    excluded_words = (
        "PROV",
        "PRUEBA",
        "PRUEBAS",
        "TEST",
        "AD",
    )
    return any(word in name_up for word in excluded_words)


@app.post("/panel/provider-weight")
def panel_provider_weight(payload: dict, db: Session = Depends(get_db)):
    provider_name = str(payload.get("provider_name") or "").strip().upper()
    weight = int(payload.get("weight") or 0)

    if provider_name not in {
        "PROVIDER1",
        "PROVIDER2",
        "PROVIDER3",
        "PROVIDER4",
        "PROVIDER5",
        "PROVIDER6",
        "PROVIDER7",
        "PROVIDER8",
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
    db: Session = Depends(get_db),
):
    if not _is_valid_admin_panel_token(request):
        return HTMLResponse("No autorizado", status_code=403)

    try:
        cache_key = _panel_cache_key(
            view=view,
            group_jid=group_jid,
            provider_name=provider_name,
            status=status,
            act_type=act_type,
            group_mode=group_mode,
        )

        cached_panel = redis_conn.get(cache_key)
        if cached_panel:
            if isinstance(cached_panel, bytes):
                cached_panel = cached_panel.decode("utf-8", errors="ignore")
            return HTMLResponse(content=cached_panel)

        time_min, time_max, view = _panel_period_bounds(view)

        base_q = _query_requests_for_panel(
            db=db,
            time_min=time_min,
            time_max=time_max,
            group_jid=group_jid or None,
            provider_name=provider_name or None,
            status=status or None,
            act_type=act_type or None,
        )
        
        group_cache = _build_group_name_cache(db)
        delivery_metrics = _panel_delivery_metrics(db, time_min, time_max)
        bot_status_rows = _bot_status_rows(db)

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
            base_q.with_entities(
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
                
                if gid != "PRIVADO" and not owner and _is_hidden_panel_group(gid, group_name):
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
            
            if gid != "PRIVADO" and not owner and _is_hidden_panel_group(gid, group_name):
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
            r.provider_name: int(r.weight or 0)
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
        
        subtitle = (
            f"Vista mensual ({PANEL_TZ})" if view == "month"
            else f"Vista diaria ({_panel_day_str()}, {PANEL_TZ})"
        )
        
        provider_states = _esc(_providers_status_text(db)).replace("\n", "<br>")

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
                  <th>QR</th>
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
        
            bot_status_html += f"""
                <tr>
                  <td>{_esc(b["label"])}</td>
                  <td class="mono">{_esc(b["instance_name"])}</td>
                  <td style="font-weight:800;color:{color};">{status_label}</td>
                  <td>{'Sí' if b["blocked"] else 'No'}</td>
                  <td>{_esc(used_txt)}</td>
                  <td>{b["total_requests"]}</td>
                  <td>{action_html}</td>
                </tr>
            """
        
        bot_status_html += """
              </tbody>
            </table>
          </div>
        
          <div id="botQrBox" style="margin-top:14px;"></div>
        </div>
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
                <a href="/panel/promotions/report" class="tool-link" target="_blank">Promociones</a>
              </div>
        
              <div class="grid-hero">
                <div class="glass">
                  <h3 class="section-title">Proveedores</h3>
            
                  <div class="provider-grid">
            
                    <div class="provider-card">
                      <div class="provider-name">ADMIN DIGITAL</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER1" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER1', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER1')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER1','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER1','off')">Desactivar</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">ACTAS DEL SURESTE</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER2" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER2', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER2')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER2','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER2','off')">Desactivar</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">AUSTRAM WEB</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER3" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER3', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER3')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER3','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER3','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshSID()">Actualizar SID</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">LAZARO WEB</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER4" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER4', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER4')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER4','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER4','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshHID()">Actualizar HID</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">LUIS SID</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER5" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER5', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER5')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER5','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER5','off')">Desactivar</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">ACTAS ESCALANTE</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER6" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER6', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER6')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER6','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER6','off')">Desactivar</button>
                      </div>
                    </div>
            
                    <div class="provider-card">
                      <div class="provider-name">VILLAFUERTE</div>
                      <div style="margin:6px 0;">
                        <div style="font-size:12px;font-weight:700;margin-bottom:5px;opacity:.85;">Tráfico asignado</div>
                        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                          <div style="display:flex;align-items:center;gap:6px;">
                            <input id="weight_PROVIDER8" type="number" min="0" step="1" value="{provider_weight_map.get('PROVIDER8', 0)}" style="width:65px;padding:4px 6px;border-radius:6px;border:1px solid #ccc;text-align:center;">
                            <span style="font-size:12px;opacity:.7;">pts</span>
                          </div>
                          <button class="btn btn-primary" onclick="saveProviderWeight('PROVIDER8')">Aplicar</button>
                        </div>
                        <div style="font-size:11px;opacity:.6;margin-top:4px;">Más puntos = más solicitudes asignadas</div>
                      </div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER8','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER8','off')">Desactivar</button>
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
                
                      <div class="broadcast-actions">
                        <button class="btn btn-success" onclick="sendFreeBroadcast()">Enviar mensaje libre</button>
                        <button class="btn btn-light" onclick="clearBroadcast()">Limpiar</button>
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

        html += """
        <div class="box">
          <div class="head">
            <strong>Resumen por bot</strong>
            <span class="small">Solicitudes por instancia de WhatsApp.</span>
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
                  <td>{_esc(bot_label(r["instance_name"]))}</td>
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

        html += bot_status_html

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
        for r in by_instance:
            inst = r["instance_name"]
            bot_used = get_bot_used(db, inst)
            bot_limit = get_bot_limit(db, inst)
            bot_available = max(0, bot_limit - bot_used) if bot_limit > 0 else "∞"
            bot_blocked = is_instance_blocked(inst)
        
            status_badge = (
                '<span class="badge badge-danger">BLOQUEADO</span>'
                if bot_blocked else
                '<span class="badge badge-success">ACTIVO</span>'
            )
        
            html += f"""
                <tr>
                  <td><strong>{_esc(bot_label(inst))}</strong></td>
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
               href="/panel?token=docifymx2026&view={_esc(view)}&group_mode=all&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}">
              Ver todos los grupos
            </a>
            <a class="group-mode-link {'group-mode-link-active' if group_mode == 'active' else ''}"
               href="/panel?token=docifymx2026&view={_esc(view)}&group_mode=active&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}">
              Solo grupos con compras del día
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
          <div class="head"><strong>Agregar grupo manualmente</strong></div>
        
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
                    <a href="/panel/group-detail?group_jid={r['group_jid']}&view={view}"
                       class="btn btn-success"
                       style="color:white;display:inline-flex;align-items:center;justify-content:center;padding:6px 12px; font-size:13px; border-radius:16px; text-decoration:none;">
                       +Promoción
                    </a>
                    """
        
                html += f"""
                <tr>
                  <td>
                    <a href="/panel/group-detail?group_jid={r['group_jid']}&view={view}">
                      {_esc(r["group_name"])}
                    </a>
                  </td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                  <td>{promo_cell}</td>
                  <td>{_esc(_fmt_dt(r["last_update"]))}</td>
                  <td>{blocked_text}</td>
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
                  <th>Grupo proveedor</th>
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
                  <td>{_esc(bot_label(r.instance_name))}</td>
                  <td>{_esc(_provider_label(r.provider_name))}</td>
                  <td>{_esc(_group_name_cached(r.provider_group_id, group_cache))}</td>
                  <td>{_esc(_fmt_dt(r.created_at))}</td>
                  <td>{_esc(_fmt_dt(r.updated_at))}</td>
                  <td class="small">{_esc(r.error_message)}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="11">Sin solicitudes en este periodo.</td></tr>'
    
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
          const hid = prompt("Pega el nuevo HID");
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
              alert("HID actualizado");
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

        async function getBotQr(instanceName) {{
          const box = document.getElementById("botQrBox");
          if (!box) return;
        
          box.innerHTML = "<strong>Generando QR...</strong>";
        
          try {{
            const res = await fetch(`/panel/instance/${{instanceName}}/qr`);
            const data = await res.json();
        
            if (!data.ok) {{
              box.innerHTML = `<div style="color:red;font-weight:800;">Error: ${{data.error || "No se pudo generar QR"}}</div>`;
              return;
            }}
        
            const payload = data.data || {{}};
            const qr =
              payload.base64 ||
              payload.qrcode?.base64 ||
              payload.qrcode?.code ||
              payload.qr ||
              payload.qrCode ||
              payload.code ||
              payload.pairingCode ||
              payload.instance?.qrcode ||
              payload.instance?.qr ||
              "";
        
            if (qr && String(qr).startsWith("data:image")) {{
              box.innerHTML = `
                <div style="padding:14px;border:1px solid #e5e7eb;border-radius:14px;background:white;">
                  <strong>QR para ${{instanceName}}</strong><br><br>
                  <img src="${{qr}}" style="max-width:280px;width:100%;border-radius:12px;">
                </div>
              `;
            }} else if (qr && String(qr).startsWith("/9j")) {{
              box.innerHTML = `
                <div style="padding:14px;border:1px solid #e5e7eb;border-radius:14px;background:white;">
                  <strong>QR para ${{instanceName}}</strong><br><br>
                  <img src="data:image/png;base64,${{qr}}" style="max-width:280px;width:100%;border-radius:12px;">
                </div>
              `;
            }} else {{
              box.innerHTML = `
                <pre style="white-space:pre-wrap;background:#111827;color:white;padding:14px;border-radius:12px;">${{JSON.stringify(payload, null, 2)}}</pre>
              `;
            }}
        
          }} catch (e) {{
            box.innerHTML = `<div style="color:red;font-weight:800;">Error de conexión</div>`;
          }}
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
          const category = document.getElementById("broadcastCategory")?.value || "all";
        
          if (!message) {{
            alert("Escribe un mensaje");
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
                category: category
              }})
            }});
        
            const data = await res.json();
        
            if (data.ok) {{
              alert(data.message || "Envío iniciado");
              textarea.value = "";
            }} else {{
              alert(data.error || "Error en envío masivo");
            }}
          }} catch (e) {{
            alert("No se pudo conectar con el servidor");
          }}
        
          broadcastRunning = false;
        }}
    
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
        
        let recentRequestsEventSource = null;
        
        function startRecentRequestsStream() {{
          if (!PANEL_STREAM_ENABLED) return;

          if (recentRequestsEventSource) {{
            recentRequestsEventSource.close();
          }}
        
          recentRequestsEventSource = new EventSource("/panel/recent-requests/stream");
        
          recentRequestsEventSource.onmessage = async function(event) {{
            try {{
              const data = JSON.parse(event.data || "{{}}");
              if (data.error) {{
                console.error("RECENT_REQUESTS_STREAM_ERROR =", data.error);
                return;
              }}
        
              if (!document.hidden) {{
                await refreshRecentRequests();
              }}
            }} catch (e) {{
              console.error("RECENT_REQUESTS_STREAM_PARSE_ERROR =", e);
            }}
          }};
        
          recentRequestsEventSource.onerror = function(err) {{
            console.error("RECENT_REQUESTS_STREAM_CONNECTION_ERROR =", err);
        
            try {{
              recentRequestsEventSource.close();
            }} catch (_) {{}}
        
            setTimeout(() => {{
              startRecentRequestsStream();
            }}, 5000);
          }};
        }}

      </script>
    </body>
    </html>
    """
        try:
            redis_conn.setex(cache_key, PANEL_HTML_TTL, html)
        except Exception:
            pass
            
        return HTMLResponse(content=html)
        
    except Exception as e:
        print("panel_actas error:", repr(e), flush=True)
        return HTMLResponse(
            content=f"<pre>Error en /panel: {str(e)}</pre>",
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
            )
            db.add(setting)
        else:
            setting.value = new_hid

        db.commit()

        print("PROVIDER4_HID_UPDATED =", new_hid, flush=True)

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
    instance = req.instance_name or instance_name or "docifybot8"

    if req.source_group_id:
        send_group_text(req.source_group_id, text, instance)
    else:
        send_text(req.requester_wa_id, text, instance)


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
            caption_text = f"⏱️ Tiempo de proceso: {tiempo}"

    print("PDF_DELIVER_INSTANCE =", instance, flush=True)
    print("PDF_CAPTION =", caption_text, flush=True)

    is_base64 = not pdf_data.startswith("http")

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


def _extract_identifier_from_filename_local(filename: str) -> str | None:
    return extract_identifier_from_filename(filename)


def _pdf_matches_req_type(pdf_bytes: bytes, req: RequestLog) -> bool:
    try:
        if is_chain(req.curp):
            return True
        return _validate_act_type_pdf(pdf_bytes, req.act_type)
    except Exception as e:
        print("PDF_MATCHES_REQ_TYPE_ERROR =", getattr(req, "id", None), str(e), flush=True)
        return False


def _pick_matching_processing_req_for_pdf(
    db: Session,
    lookup_id: str | None,
    source_chat_id: str,
    quoted_msg_id: str | None,
    pdf_bytes: bytes,
):
    candidates = []

    if lookup_id:
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.curp == lookup_id,
                RequestLog.status == "PROCESSING",
                RequestLog.provider_group_id == source_chat_id,
            )
            .order_by(RequestLog.created_at.asc())
            .all()
        )

    if not candidates and quoted_msg_id:
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.provider_message_id == quoted_msg_id,
                RequestLog.status == "PROCESSING",
            )
            .order_by(RequestLog.created_at.desc())
            .all()
        )

    if not candidates and lookup_id:
        candidates = (
            db.query(RequestLog)
            .filter(
                RequestLog.curp == lookup_id,
                RequestLog.status == "PROCESSING",
            )
            .order_by(RequestLog.created_at.asc())
            .all()
        )

    print("PROVIDER_PDF_MATCH_CANDIDATES =", [
        {
            "id": r.id,
            "curp": r.curp,
            "act_type": r.act_type,
            "provider_group_id": r.provider_group_id,
            "source_group_id": r.source_group_id,
            "instance_name": r.instance_name,
        }
        for r in candidates
    ], flush=True)

    # Primero intentamos encontrar el request cuyo tipo coincida con el PDF.
    for r in candidates:
        if _pdf_matches_req_type(pdf_bytes, r):
            print("PROVIDER_PDF_SMART_TYPE_MATCH =", {
                "matched_req_id": r.id,
                "matched_act_type": r.act_type,
            }, flush=True)
            return r

    # Si solo hay un candidato, lo regresamos para que las validaciones normales decidan.
    if len(candidates) == 1:
        print("PROVIDER_PDF_SINGLE_CANDIDATE_NO_TYPE_MATCH =", {
            "matched_req_id": candidates[0].id,
            "matched_act_type": candidates[0].act_type,
        }, flush=True)
        return candidates[0]

    print("PROVIDER_PDF_NO_SAFE_TYPE_MATCH =", {
        "lookup_id": lookup_id,
        "source_chat_id": source_chat_id,
        "quoted_msg_id": quoted_msg_id,
        "candidates": len(candidates),
    }, flush=True)

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
        settings.PROVIDER_GROUP_ESPECIALES,
        settings.PROVIDER_GROUP_FOLIADAS,
        settings.PROVIDER2_GROUP_1,
        settings.PROVIDER2_GROUP_2,
        settings.PROVIDER5_GROUP_1,
        settings.PROVIDER5_GROUP_2,
        settings.PROVIDER6_GROUP_1,
        settings.PROVIDER6_GROUP_2,
        settings.PROVIDER8_GROUP_1,
        settings.PROVIDER8_GROUP_2,
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


def _providers_status_text(db: Session) -> str:
    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)
    p3 = _get_or_create_provider(db, "PROVIDER3", False)
    p4 = _get_or_create_provider(db, "PROVIDER4", False)
    p5 = _get_or_create_provider(db, "PROVIDER5", False)
    p6 = _get_or_create_provider(db, "PROVIDER6", False)
    p7 = _get_or_create_provider(db, "PROVIDER7", False)
    p8 = _get_or_create_provider(db, "PROVIDER8", False)

    s1 = "ON" if p1.is_enabled else "OFF"
    s2 = "ON" if p2.is_enabled else "OFF"
    s3 = "ON" if p3.is_enabled else "OFF"
    s4 = "ON" if p4.is_enabled else "OFF"
    s5 = "ON" if p5.is_enabled else "OFF"
    s6 = "ON" if p6.is_enabled else "OFF"
    s7 = "ON" if p7.is_enabled else "OFF"
    s8 = "ON" if p8.is_enabled else "OFF"

    provider1_extra = ""
    provider2_extra = ""
    provider3_extra = ""
    provider4_extra = ""
    provider5_extra = ""
    provider6_extra = ""
    provider7_extra = ""
    provider8_extra = ""

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

    text = (
        f"ADMIN DIGITAL:     {s1}{provider1_extra}\n"
        f"ACTAS DEL SURESTE: {s2}{provider2_extra}\n"
        f"AUSTRAM WEB:       {s3}{provider3_extra}\n"
        f"LAZARO WEB:        {s4}{provider4_extra}\n"
        f"LUIS SID:          {s5}{provider5_extra}\n"
        f"ACTAS ESCALANTE:   {s6}{provider6_extra}\n"
        f"VILLAFUERTE:       {s8}{provider8_extra}"
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


def webhook_msg_seen(msg_id: str) -> bool:
    if not msg_id:
        return False

    key = f"wa:webhook:msg:{msg_id}"
    created = redis_conn.set(key, "1", ex=300, nx=True)
    return not bool(created)


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
    return (
        db.query(RequestLog)
        .filter(
            RequestLog.curp == term,
            RequestLog.act_type == act_type,
            RequestLog.source_chat_id == source_chat_id,
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


@app.post("/webhook/evolution")
async def evolution_webhook(payload: dict, db: Session = Depends(get_db)):
    try:
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
                
        key = data.get("key", {})
        message = data.get("message", {})
        push_name = data.get("pushName", "")
        
        remote_jid = key.get("remoteJid", "")
        from_me = key.get("fromMe", False)
        participant = key.get("participant", "")
        msg_id = key.get("id", "")
        
        if webhook_msg_seen(msg_id):
            print("IGNORED_REASON = duplicate_msg_id", flush=True)
            print("IGNORED_MSG_ID =", msg_id, flush=True)
            return {"ok": True, "ignored": "duplicate_msg_id"}
        
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
            text_body = message.get("extendedTextMessage", {}).get("text", "")
        
        text_upper = normalize_text(text_body)

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
            return {"ok": True, "ignored": "group_blocked"}

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
                            RequestLog.provider_name.in_(["PROVIDER5", "PROVIDER6", "PROVIDER8"]),
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )

                    if open_req:
                        print("PROVIDER5_SIN_MATCHED_REQ_ID =", open_req.id, flush=True)
                        print("PROVIDER5_SIN_MATCHED_CURP =", open_req.curp, flush=True)

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
                            if open_req.source_group_id:
                                send_group_text(open_req.source_group_id, msg, instance_name=client_instance)
                            else:
                                send_text(open_req.requester_wa_id, msg, instance_name=client_instance)
                        except Exception as notify_exc:
                            print("PROVIDER5_SIN_NOTIFY_ERROR =", str(notify_exc), flush=True)

                        return {"ok": True, "provider_result": "provider5_sin_matched_by_reply_id"}

                    print("PROVIDER5_SIN_WITHOUT_MATCH =", quoted_msg_id, flush=True)

                # 2) FALLBACK POR CURP EN TEXTO
                if provider_id and is_negative_text:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.curp == provider_id,
                            RequestLog.status == "PROCESSING",
                            RequestLog.provider_name.in_(["PROVIDER5", "PROVIDER6", "PROVIDER8"]),
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )

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
                            if open_req.source_group_id:
                                send_group_text(open_req.source_group_id, msg, instance_name=client_instance)
                            else:
                                send_text(open_req.requester_wa_id, msg, instance_name=client_instance)
                        except Exception as notify_exc:
                            print("PROVIDER5_FALLBACK_NOTIFY_ERROR =", str(notify_exc), flush=True)

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
                            if open_req.source_group_id:
                                send_group_text(open_req.source_group_id, msg, instance_name=client_instance)
                            else:
                                send_text(open_req.requester_wa_id, msg, instance_name=client_instance)
                        except Exception as notify_exc:
                            print("PROVIDER8_SIN_FALLBACK_NOTIFY_ERROR =", str(notify_exc), flush=True)
                
                        return {"ok": True, "provider_result": "provider8_sin_fallback_last_processing"}
                
                    print("PROVIDER8_SIN_FALLBACK_WITHOUT_MATCH =", source_chat_id, flush=True)

            if doc:
                filename = doc.get("fileName") or ""
                filename_id = _extract_identifier_from_filename_local(filename)
            
                print("PROVIDER_DOC_FILENAME =", filename, flush=True)
                print("PROVIDER_DOC_FILENAME_IDENTIFIER =", filename_id, flush=True)
            
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
                lookup_id = filename_id or provider_id
            
                media_b64_start_ts = time.time()
                print("PROVIDER1_MEDIA_B64_START =", media_message_id, media_b64_start_ts, flush=True)
                print("PDF_RECEIVED_TO_MEDIA_B64_START_S =", round(media_b64_start_ts - pdf_received_ts, 3), flush=True)
            
                t_media_b64_start = time.perf_counter()
                t0 = time.perf_counter()
                print("T_DOC_DETECTED =", source_chat_id, media_message_id, flush=True)
            
                t1 = time.perf_counter()
                media_json = get_media_base64("document", media_message_id, instance_name)
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
                    print("PROVIDER_PDF_BASE64_EMPTY =", media_json, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_base64_empty"}
                
                if media_b64.startswith("data:"):
                    parts = media_b64.split(",", 1)
                    media_b64 = parts[1] if len(parts) > 1 else media_b64
                
                media_b64 = media_b64.replace("\n", "").replace("\r", "").strip()
                
                missing_padding = len(media_b64) % 4
                if missing_padding:
                    media_b64 += "=" * (4 - missing_padding)

                t_decode = time.perf_counter()
                pdf_bytes = base64.b64decode(media_b64, validate=False)
                print("T_BASE64_DECODE =", round(time.perf_counter() - t_decode, 3), flush=True)
                
                print("PDF_HEADER =", pdf_bytes[:8], flush=True)
                print("PDF_BYTES_LEN =", len(pdf_bytes), flush=True)
                
                if b"%PDF" not in pdf_bytes[:20]:
                    print("PROVIDER_PDF_INVALID_BINARY", flush=True)
                    return {"ok": True, "ignored": "provider_pdf_invalid_binary"}
                
                t_encode = time.perf_counter()
                safe_media_b64 = base64.b64encode(pdf_bytes).decode()
                print("T_BASE64_REENCODE =", round(time.perf_counter() - t_encode, 3), flush=True)

                open_req = _pick_matching_processing_req_for_pdf(
                    db=db,
                    lookup_id=lookup_id,
                    source_chat_id=source_chat_id,
                    quoted_msg_id=quoted_msg_id,
                    pdf_bytes=pdf_bytes,
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
                    print("PROVIDER_PDF_WITHOUT_SAFE_MATCH =", {
                        "filename": filename,
                        "lookup_id": lookup_id,
                        "source_chat_id": source_chat_id,
                        "quoted_msg_id": quoted_msg_id,
                    }, flush=True)
                
                    fallback_req = None
                    if lookup_id:
                        fallback_req = (
                            db.query(RequestLog)
                            .filter(
                                RequestLog.curp == lookup_id,
                                RequestLog.status.in_(["PROCESSING"]),
                            )
                            .order_by(RequestLog.created_at.desc())
                            .first()
                        )
                
                    if fallback_req:
                        active_count = (
                            db.query(RequestLog)
                            .filter(
                                RequestLog.curp == lookup_id,
                                RequestLog.status == "PROCESSING",
                            )
                            .count()
                        )
                    
                        print("PROVIDER_PDF_LAST_RESORT_ACTIVE_COUNT =", active_count, flush=True)
                    
                        if active_count == 1:
                            print("PROVIDER_PDF_LAST_RESORT_MATCH =", fallback_req.id, flush=True)
                            open_req = fallback_req
                        else:
                            print("PROVIDER_PDF_LAST_RESORT_AMBIGUOUS =", {
                                "lookup_id": lookup_id,
                                "active_count": active_count,
                            }, flush=True)
                            return {"ok": True, "ignored": "ambiguous_multiple_processing_requests"}
                    else:
                        return {"ok": True, "ignored": "provider_pdf_without_safe_match"}

                is_chain_req = is_chain(open_req.curp)
                if (not is_chain_req) and (not _validate_act_type_pdf(pdf_bytes, open_req.act_type)):
                    print("PROVIDER_PDF_WRONG_ACT_TYPE_AFTER_SMART_MATCH =", {
                        "req_id": open_req.id,
                        "curp": open_req.curp,
                        "expected_act_type": open_req.act_type,
                        "filename": filename,
                        "source_chat_id": source_chat_id,
                    }, flush=True)

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
                                f"filename={filename} | expected_act_type={open_req.act_type} | NO se notificó al cliente para evitar falso error"
                            )
                    except Exception as support_exc:
                        print("NOTIFY_SUPPORT_ERROR_FAILED =", str(support_exc), flush=True)
                    
                    return {"ok": True, "ignored": "provider_pdf_wrong_act_type_pending_retry"}
                
                if is_chain_req:
                    print("PROVIDER_CHAIN_SKIP_ACT_TYPE_VALIDATION =", open_req.curp, flush=True)
                
                if (not is_chain_req) and (not _validate_pdf_matches_term(pdf_bytes, open_req.curp, open_req.act_type)):
                    print("PROVIDER_PDF_WRONG_CURP =", {
                        "req_id": open_req.id,
                        "curp": open_req.curp,
                        "expected_act_type": open_req.act_type,
                        "filename": filename,
                        "source_chat_id": source_chat_id,
                    }, flush=True)
                
                    open_req.status = "ERROR"
                    open_req.error_message = "WRONG_CURP_IN_PDF"
                    open_req.updated_at = _utc_now_naive()
                    db.commit()
                
                    _notify_support_error(
                        open_req,
                        "WRONG_CURP_IN_PDF",
                        f"filename={filename} | expected_curp={open_req.curp}"
                    )
                    return {"ok": True, "ignored": "provider_pdf_wrong_curp"}

                match_term = filename_id or provider_id or open_req.curp or "NO_TERM"
                pdf_dedupe_key = f"provider_pdf:{open_req.id}:{source_chat_id}:{match_term}:{filename or 'nofile'}"
                
                already_sent = redis_conn.set(pdf_dedupe_key, "1", ex=3600, nx=True)
                if not already_sent:
                    print("PROVIDER_PDF_DUPLICATE_IGNORED =", pdf_dedupe_key, flush=True)
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
                    _deliver_pdf_result(
                        open_req,
                        safe_media_b64,
                        filename=filename or f"{open_req.curp}.pdf",
                    )
                    print("T_DELIVER_PDF_RESULT =", round(time.perf_counter() - t4, 3), flush=True)
                
                except Exception as delivery_exc:
                    print("DELIVERY_FAILED =", str(delivery_exc), flush=True)
                
                    open_req.status = "ERROR"
                    open_req.error_message = f"DELIVERY_FAILED: {str(delivery_exc)[:300]}"
                    open_req.updated_at = _utc_now_naive()
                    db.commit()
                
                    try:
                        redis_conn.delete(pdf_dedupe_key)
                    except Exception as redis_del_exc:
                        print("PDF_DEDUPE_DELETE_AFTER_DELIVERY_FAILED_ERROR =", str(redis_del_exc), flush=True)
                
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
            open_req = None
            if provider_id:
                open_req = (
                    db.query(RequestLog)
                    .filter(
                        RequestLog.provider_group_id == source_chat_id,
                        RequestLog.curp == provider_id,
                        RequestLog.status == "PROCESSING",
                    )
                    .order_by(RequestLog.created_at.asc())
                    .first()
                )
            
                if not open_req:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.curp == provider_id,
                            RequestLog.status == "PROCESSING",
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )
            
                    print("PROVIDER_NO_RECORD_FALLBACK_MATCH =", {
                        "provider_id": provider_id,
                        "matched_req_id": getattr(open_req, "id", None),
                        "matched_provider_group_id": getattr(open_req, "provider_group_id", None),
                        "matched_source_group_id": getattr(open_req, "source_group_id", None),
                        "matched_instance_name": getattr(open_req, "instance_name", None),
                    }, flush=True)

            if text_body and _is_no_record_message(text_upper):
                if not open_req:
                    print("PROVIDER_NO_RECORD_WITHOUT_MATCH =", text_body, flush=True)
                    return {"ok": True, "ignored": "provider_no_record_without_match"}

                print("PROVIDER_NO_RECORD_MATCHED_REQ_ID =", open_req.id, flush=True)
                print("PROVIDER_NO_RECORD_MATCHED_CURP =", open_req.curp, flush=True)

                open_req.status = "ERROR"
                open_req.error_message = text_body.strip()
                open_req.updated_at = _utc_now_naive()
                db.commit()

                _deliver_text_result(
                    open_req,
                    f"❌ No hay registros disponibles.\nDato: {open_req.curp}\nTipo: {open_req.act_type}\n\nVerificar que la CURP esté certificada en RENAPO",
                )
                return {"ok": True, "provider_result": "no_record"}

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
                request_queue.enqueue(process_request, last.id)
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
        
            # buscar si hay una abierta para este dato/tipo/grupo
            open_existing = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.source_chat_id == source_chat_id,
                    RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])
                )
                .order_by(RequestLog.created_at.desc())
                .first()
            )
        
            # 1) si ya existe una abierta, no duplicar
            if open_existing:
                age_sec = (_utc_now_naive() - open_existing.created_at).total_seconds()
                age_sec = max(age_sec, 0)
            
                dup_msg = (
                    f"⏳ Ya existe una solicitud en proceso\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}\n\n"
                    f"Espera un momento antes de reenviar."
                )
            
                print("DUPLICATE_OPEN_REQUEST_BLOCKED =", {
                    "term": term,
                    "act_type": act_type,
                    "open_req_id": open_existing.id,
                    "open_status": open_existing.status,
                    "age_sec": round(age_sec, 2),
                }, flush=True)
            
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, dup_msg, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, dup_msg, instance_name=instance_name)
            
                continue
        
            # contar intentos previos de ese mismo dato/tipo/grupo
            same_requests_count = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.source_chat_id == source_chat_id
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
            request_key = f"{base_request_key}:try_{same_requests_count + 1}"
        
            # 2) si existe una anterior en ERROR, reutilizar SOLO la más reciente en error
            error_existing = (
                db.query(RequestLog)
                .filter(
                    RequestLog.curp == term,
                    RequestLog.act_type == act_type,
                    RequestLog.source_chat_id == source_chat_id,
                    RequestLog.status == "ERROR"
                )
                .order_by(RequestLog.created_at.desc())
                .first()
            )
        
            if error_existing:
                error_existing.request_key = request_key
                error_existing.status = "QUEUED"
                error_existing.updated_at = _utc_now_naive()
                error_existing.error_message = None
                error_existing.evolution_message_id = msg_id
                error_existing.requester_wa_id = requester_wa_id
                error_existing.requester_name = ""
                error_existing.source_chat_id = source_chat_id
                error_existing.source_group_id = source_group_id
                error_existing.instance_name = instance_name
                error_existing.provider_name = None
                error_existing.provider_group_id = None
                error_existing.provider_message = None
                error_existing.provider_media_url = None
                error_existing.pdf_url = None
                error_existing.expires_at = _utc_now_naive() + timedelta(days=settings.HISTORY_DAYS)
                db.commit()
        
                request_queue.enqueue(process_request, error_existing.id)
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
                print("DUPLICATE_REQUEST_KEY =", request_key, flush=True)
            
                dup_msg = (
                    f"⏳ Ya existe una solicitud en proceso\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}"
                )
            
                if source_group_id:
                    if should_send_extra_text(source_group_id):
                        send_group_text(source_group_id, dup_msg, instance_name=instance_name)
                else:
                    send_text(requester_wa_id, dup_msg, instance_name=instance_name)
            
                continue
        
            request_queue.enqueue(process_request, row.id)
            created_any = True
        
            print("ENQUEUED_REQUEST_ID =", row.id, flush=True)
            print("ENQUEUED_TERM =", row.curp, flush=True)
            print("ENQUEUED_TYPE =", row.act_type, flush=True)
            print("ENQUEUED_SOURCE_GROUP =", row.source_group_id, flush=True)

        if created_any:
            actor = push_name or requester_wa_id
            bot_name = BOT_LABELS.get(instance_name, "🚀 DOCU EXPRES")
            
            ack_msg = (
                f"{bot_name}\n"
                f"Solicitud recibida de {actor}.\n"
                f"Esto puede tardar unos segundos..."
            )
        
            if source_group_id:
                send_group_text(source_group_id, ack_msg, instance_name=instance_name)
            else:
                send_text(requester_wa_id, ack_msg, instance_name=instance_name)
        else:
            print("IGNORED_REASON = nothing_created", flush=True)
        
        return {"ok": True}

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), payload)
        return {"ok": False, "error": str(e)}
