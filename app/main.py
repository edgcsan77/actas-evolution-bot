import os
import base64
import time
import random
import asyncio
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import Counter

from fastapi import FastAPI, Depends, Body, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import Base, engine, get_db, SessionLocal
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog, ProviderSetting, AppSetting, GroupPromotion, GroupAlias
from app.queue import request_queue, redis_conn
from app.worker import process_request, provider3_keepalive_job
from app.services.provider3 import Provider3Client
from app.services.provider4 import Provider4Client

from app.utils.curp import (
    extract_request_terms,
    detect_act_type,
    normalize_text,
    extract_identifier_loose,
    extract_identifier_from_filename,
    detect_identifier_problem,
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

app = FastAPI(title=settings.APP_NAME)
PANEL_TZ = "America/Monterrey"
BLOCKED_GROUPS_KEY = "blocked_groups_no_response"


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


def _day_name_es_from_date(day_str: str) -> str:
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    return DAYS_ES[dt.weekday()]


def is_group_blocked(group_jid: str) -> bool:
    if not group_jid:
        return False

    blocked = redis_conn.sismember(BLOCKED_GROUPS_KEY, group_jid)
    return bool(blocked)


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
    return 8 <= hour < 23


def _panel_now():
    return datetime.now(ZoneInfo(PANEL_TZ))


def _panel_day_str():
    return _panel_now().strftime("%Y-%m-%d")


def _panel_week_start(dt=None):
    dt = dt or _panel_now()
    start = dt - timedelta(days=dt.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0)


def _panel_week_end(dt=None):
    return _panel_week_start(dt) + timedelta(days=7)


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

    if view == "week":
        local_start = _panel_week_start()
        local_end = _panel_week_end()
        utc_start = _panel_to_utc_naive(local_start)
        utc_end = _panel_to_utc_naive(local_end)
        return utc_start, utc_end, "week"

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
    )

    if group_jid:
        val = group_jid.strip()
        val_low = val.lower()

        alias_rows = db.query(GroupAlias).all()
        alias_matches = [
            row.group_jid
            for row in alias_rows
            if row.group_jid and (
                val_low in row.group_jid.lower()
                or val_low in (row.custom_name or "").lower()
            )
        ]

        map_matches = [
            gid for gid, name in GROUP_NAME_MAP.items()
            if val_low in gid.lower() or val_low in (name or "").lower()
        ]

        matching_group_ids = list(dict.fromkeys(alias_matches + map_matches))

        if matching_group_ids:
            q = q.filter(RequestLog.source_group_id.in_(matching_group_ids))
        else:
            q = q.filter(RequestLog.source_group_id.ilike(f"%{val}%"))

    if provider_name:
        val = provider_name.strip()
        q = q.filter(RequestLog.provider_name.ilike(f"%{val}%"))

    if status:
        val = status.strip()
        q = q.filter(RequestLog.status.ilike(f"%{val}%"))

    if act_type:
        val = act_type.strip()
        q = q.filter(RequestLog.act_type.ilike(f"%{val}%"))

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


def _panel_group_rows(
    rows: list[RequestLog],
    db: Session,
    include_all_groups: bool = False,
    has_active_filters: bool = False,
) -> list[dict]:
    data = {}

    excluded_words = (
        "PROV",
        "PRUEBA",
        "PRUEBAS",
        "TEST",
    )

    def _is_hidden_group(name: str) -> bool:
        name_up = (name or "").strip().upper()
        return any(word in name_up for word in excluded_words)

    if include_all_groups and not has_active_filters:
        alias_rows = db.query(GroupAlias).all()
        alias_map = {
            row.group_jid: (row.custom_name or "").strip()
            for row in alias_rows
            if row.group_jid
        }

        all_group_ids = set(GROUP_NAME_MAP.keys()) | set(alias_map.keys())

        for gid in all_group_ids:
            name = alias_map.get(gid) or GROUP_NAME_MAP.get(gid) or "Grupo sin nombre"

            if _is_hidden_group(name):
                continue

            data[gid] = {
                "group_jid": gid,
                "group_name": name,
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
        group_name = _group_name(gid, db)

        if gid != "PRIVADO" and _is_hidden_group(group_name):
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

        if not item["last_update"] or (r.updated_at and r.updated_at > item["last_update"]):
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
        name = r.provider_name or "SIN_PROVEEDOR"
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

    if (view or "day").strip().lower() == "week":
        local_start = _panel_week_start(now_local)
        local_end = _panel_week_end(now_local)
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


@app.get("/panel/recent-requests/stream")
async def panel_recent_requests_stream():
    async def event_generator():
        last_seen_id = 0
        last_seen_updated = ""

        while True:
            db = SessionLocal()
            try:
                row = (
                    db.query(RequestLog.id, RequestLog.updated_at)
                    .order_by(RequestLog.updated_at.desc(), RequestLog.id.desc())
                    .first()
                )

                current_id = row.id if row else 0
                current_updated = (
                    row.updated_at.isoformat() if row and row.updated_at else ""
                )

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

            await asyncio.sleep(3)

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
    time_min, time_max, view = _panel_period_bounds(view)

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
        .order_by(RequestLog.created_at.desc())
        .limit(10)
        .all()
    )

    latest = rows
    
    html = """
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Dato</th>
          <th>Tipo</th>
          <th>Estado</th>
          <th>Grupo cliente</th>
          <th>Proveedor</th>
          <th>Grupo proveedor</th>
          <th>Mensaje proveedor</th>
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
              <td>{_esc(_group_name(r.source_group_id, db))}</td>
              <td>{_esc(r.provider_name)}</td>
              <td>{_esc(_group_name(r.provider_group_id, db))}</td>
              <td class="small">{_esc(r.provider_message)}</td>
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
    """

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
        row.updated_at = _utc_now_naive()

    db.commit()

    try:
        _notify_client_groups_main(
            rows,
            "⚠️ *Promoción desactivada*\n\nLa promoción compartida de este cliente fue desactivada."
        )
    except Exception as e:
        print("PROMOTION_REMOVE_NOTIFY_ERROR =", str(e), flush=True)

    return {"ok": True, "shared_key": shared_key}


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
            row.price_per_piece = price_per_piece
            row.is_credit = is_credit
            row.credit_abono = credit_abono
            row.credit_debe = credit_debe
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

        _notify_client_groups_main(
            rows,
            (
                f"✅ *Promoción activada*\n\n"
                f"Tu promoción *{promo_label}* ya fue activada correctamente.\n"
                f"Tipo: *{tipo_label}*\n"
                f"Cuentas con *{total_actas} actas disponibles*.\n\n"
                f"Este saldo aplica para todos los grupos asociados a esta promoción compartida.\n"
                f"Bolsa compartida: *{shared_key}*.\n\n"
                f"Gracias por tu preferencia."
            )
        )
    except Exception as notify_exc:
        print("PROMOTION_ACTIVATION_NOTIFY_ERROR =", str(notify_exc), flush=True)

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

    return {
        "ok": True,
        "message": "Abono registrado correctamente",
        "group_jid": group_jid,
        "credit_abono": row.credit_abono,
        "credit_debe": row.credit_debe,
    }


@app.get("/panel/promotions/report", response_class=HTMLResponse)
def panel_promotions_report(db: Session = Depends(get_db)):
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
            "group_name": _group_name(r.group_jid, db),
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


@app.get("/panel/group-detail", response_class=HTMLResponse)
def panel_group_detail(
    group_jid: str = "",
    view: str = "week",
    db: Session = Depends(get_db),
):
    if not group_jid:
        return HTMLResponse("<pre>Falta group_jid</pre>", status_code=400)

    promo = _get_group_promotion(db, group_jid)
    promo_html = _promotion_badge_html(promo)
    group_display_name = _esc(_group_name(group_jid, db))
    promo_name = _esc(promo.promo_name if promo else "")
    promo_total = promo.total_actas if promo else 0
    promo_used = promo.used_actas if promo else 0
    promo_available = _promotion_available(promo) if promo else 0
    promo_price = _esc(promo.price_per_piece if promo else "")

    promo_is_credit = bool(promo.is_credit) if promo else False
    promo_credit_abono = promo.credit_abono if promo else 0
    promo_credit_debe = promo.credit_debe if promo else 0
    promo_type_label = "Crédito" if promo_is_credit else "Pagada"

    time_min, time_max, view = _panel_period_bounds(view)

    rows = _query_requests_for_panel(
        db=db,
        time_min=time_min,
        time_max=time_max,
    ).order_by(RequestLog.created_at.asc()).all()

    detail = _panel_detail_for_group(rows, group_jid, view, db)

    title = detail["group_name"]
    subtitle = (
        f"Historial semanal: {detail['date_from']} a {detail['date_to']} ({PANEL_TZ})"
        if view == "week"
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
          <a href="/panel?view={_esc(view)}">← Volver al historial</a>
          <h1>{_esc(title)}</h1>
          <div class="hero-sub">{_esc(subtitle)}</div>
        </div>
    """

    html += f"""
        <div class="box">
          <div class="head"><strong>Nombre del grupo</strong></div>

          <div class="filters" style="grid-template-columns: minmax(0, 1fr) 240px;">
            <div>
              <div class="small">Nombre personalizado</div>
              <input id="group_custom_name" placeholder="Escribe el nombre del grupo" value="{group_display_name}">
            </div>

            <div style="display:flex;align-items:end;">
              <button type="button" class="btn btn-primary" style="width:100%;" onclick="saveGroupName('{group_jid}')">
                Guardar nombre
              </button>
            </div>
          </div>
        </div>
    """

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
              <div class="small">Precio por pieza o bloque</div>
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
            <button type="button" class="btn btn-primary" onclick="savePromotion('{group_jid}')">Guardar promoción</button>
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
              </tr>
            </thead>
            <tbody>
    """

    for r in detail["rows"]:
        html += f"""
              <tr>
                <td>{_esc(r["day_name"])}</td>
                <td>{_esc(r["date"])}</td>
                <td class="right">{r["total"]}</td>
                <td class="right">{r["done"]}</td>
                <td class="right">{r["error"]}</td>
                <td class="right">{r["queued"]}</td>
                <td class="right">{r["processing"]}</td>
              </tr>
        """

    t = detail["totals"]
    html += f"""
              <tr class="total-row">
                <td colspan="2">TOTAL</td>
                <td class="right">{t["total"]}</td>
                <td class="right">{t["done"]}</td>
                <td class="right">{t["error"]}</td>
                <td class="right">{t["queued"]}</td>
                <td class="right">{t["processing"]}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <script>
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
    "120363408639542108@g.us": "Prov Emergencia 1",
    "120363427054214985@g.us": "Prov Emergencia 2",
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
    "120363407565721999@g.us": "Docify Mx 3 - Eduardo",
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


def _run_broadcast_job(message_text: str):
    sent = []
    failed = []

    for gid in _broadcast_target_groups():
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
def panel_broadcast_activas(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_broadcast_job, BROADCAST_ACTIVAS_MSG)
    return {
        "ok": True,
        "queued": True,
        "message": "Envío masivo en segundo plano iniciado",
    }


@app.post("/panel/broadcast/restablecido")
def panel_broadcast_mantenimiento(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_broadcast_job, BROADCAST_RESTABLECIDO_MSG)
    return {
        "ok": True,
        "queued": True,
        "message": "Envío masivo en segundo plano iniciado",
    }


@app.post("/panel/broadcast/suspendido")
def panel_broadcast_suspendido(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_broadcast_job, BROADCAST_SUSPENDIDO_MSG)
    return {
        "ok": True,
        "queued": True,
        "message": "Envío masivo en segundo plano iniciado",
    }


@app.post("/panel/broadcast/cerrado")
def panel_broadcast_cerrado(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_broadcast_job, BROADCAST_CERRADO_MSG)
    return {
        "ok": True,
        "queued": True,
        "message": "Envío masivo en segundo plano iniciado",
    }


@app.post("/panel/broadcast/free")
async def panel_broadcast_free(request: Request, background_tasks: BackgroundTasks):
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        message_text = (payload.get("message") or "").strip()

        if not message_text:
            return {"ok": False, "error": "Mensaje vacío"}

        background_tasks.add_task(_run_broadcast_job, message_text)

        return {
            "ok": True,
            "queued": True,
            "message": "Envío masivo en segundo plano iniciado",
        }

    except Exception as e:
        print("panel_broadcast_free error:", repr(e), flush=True)
        return {"ok": False, "error": str(e)}
        

def _promotion_summary_map(db: Session) -> dict[str, dict]:
    rows = db.query(GroupPromotion).filter(GroupPromotion.is_active == True).all()

    shared_counts = Counter((r.shared_key or "").strip() for r in rows if (r.shared_key or "").strip())

    out = {}

    for r in rows:
        available = max(0, (r.total_actas or 0) - (r.used_actas or 0))
        shared_key = (r.shared_key or "").strip()

        payload = {
            "promo_name": r.promo_name or "",
            "total_actas": r.total_actas or 0,
            "used_actas": r.used_actas or 0,
            "available": available,
            "client_key": (r.client_key or "").strip(),
            "shared_key": shared_key,
            "shared_count": shared_counts.get(shared_key, 0),
            "html": _promotion_badge_html(r),
        }

        raw_key = (r.group_jid or "").strip()
        clean_key = raw_key.replace("@g.us", "")

        out[raw_key] = payload
        out[clean_key] = payload

    return out
                                                                                                        
                    
@app.get("/panel", response_class=HTMLResponse)
def panel_actas(
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    status: str = "",
    act_type: str = "",
    group_mode: str = "active",
    db: Session = Depends(get_db),
):
    try:
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
        
        include_all_groups = (group_mode == "all")
        has_active_filters = any([
            (group_jid or "").strip(),
            (provider_name or "").strip(),
            (status or "").strip(),
            (act_type or "").strip(),
        ])
        
        by_group = _panel_group_rows(
            rows,
            db=db,
            include_all_groups=include_all_groups,
            has_active_filters=has_active_filters,
        )
        
        by_provider = _panel_provider_rows(rows)
        by_type = _panel_type_rows(rows)
        promo_map = _promotion_summary_map(db)
    
        latest = rows[:100]
    
        subtitle = (
            f"Vista semanal ({PANEL_TZ})" if view == "week"
            else f"Vista diaria ({_panel_day_str()}, {PANEL_TZ})"
        )
    
        provider_states = _esc(_providers_status_text(db)).replace("\n", "<br>")
    
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
            
              table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 820px;
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
                <a href="/panel?view=day&group_mode={_esc(group_mode)}" class="tool-link {'tool-link-active' if view == 'day' else ''}">Hoy</a>
                <a href="/panel?view=week&group_mode={_esc(group_mode)}" class="tool-link {'tool-link-active' if view == 'week' else ''}">Semana actual</a>
                <a href="/panel/promotions/report" class="tool-link" target="_blank">Promociones</a>
              </div>
        
              <div class="grid-hero">
                <div class="glass">
                  <h3 class="section-title">Proveedores</h3>
        
                  <div class="provider-grid">
                    <div class="provider-card">
                      <div class="provider-name">PROVEEDOR WA EMERGENCIA</div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER1','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER1','off')">Desactivar</button>
                      </div>
                    </div>
        
                    <div class="provider-card">
                      <div class="provider-name">AUSTRAM WEB</div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER3','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER3','off')">Desactivar</button>
                        <button class="btn btn-warning" onclick="refreshSID()">Actualizar SID</button>
                      </div>
                    </div>
        
                    <div class="provider-card">
                      <div class="provider-name">LAZARO WEB</div>
                      <div class="provider-actions">
                        <button class="btn btn-success" onclick="toggleProvider('PROVIDER4','on')">Activar</button>
                        <button class="btn btn-danger" onclick="toggleProvider('PROVIDER4','off')">Desactivar</button>
                      </div>
                    </div>
                  </div>
        
                  <div class="status-panel">
                    <strong>Estado actual</strong><br><br>
                    {provider_states}
                  </div>
                </div>
        
                <div class="glass">
                  <h3 class="section-title">Mensajes masivos</h3>
        
                  <div class="broadcast-grid">
                    <div>
                      <div class="helper" style="margin-bottom:10px;">Envía mensajes predefinidos a todos los grupos activos.</div>
                      <div class="broadcast-buttons">
                        <button class="btn btn-success" onclick="sendBroadcast('activas')">Servicio activo</button>
                        <button class="btn btn-warning" onclick="sendBroadcast('restablecido')">Servicio restablecido</button>
                        <button class="btn btn-danger" onclick="sendBroadcast('suspendido')">Servicio suspendido</button>
                        <button class="btn btn-closed" onclick="sendBroadcast('cerrado')">Servicio cerrado</button>
                      </div>
                    </div>
        
                    <div class="broadcast-free">
                      <div>
                        <strong>Mensaje libre</strong>
                        <div class="helper" style="margin-top:6px;">
                          Escribe un mensaje personalizado para enviarlo masivamente.
                        </div>
                      </div>
        
                      <textarea
                        id="broadcastMessage"
                        placeholder="Escribe aquí el mensaje que deseas enviar a todos los grupos..."
                      ></textarea>
        
                      <div class="actions-row">
                        <button class="btn btn-success" onclick="sendFreeBroadcast()">Enviar mensaje libre</button>
                        <button class="btn btn-light" onclick="clearBroadcast()">Limpiar</button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
        
            <form class="box" method="get" action="/panel">
              <div class="head">
                <strong>Filtros</strong>
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
          <div class="head collapsible-head closed" onclick="toggleSection('promoCompartidaBody', this)">
            <div>
              <strong>Promoción compartida</strong>
              <span class="small">
                Permite asignar un paquete de actas a varios grupos para compartir el mismo saldo.
              </span>
            </div>
            <span class="collapse-icon">▼</span>
          </div>
        
          <div id="promoCompartidaBody" class="collapsible-body closed">
        
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
        for gid, name in sorted(GROUP_NAME_MAP.items(), key=lambda x: x[1].lower()):
            group_name = (name or "").strip()
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
        
              <button class="btn btn-light" type="button" onclick="clearSharedPromotionSelection()">
                Limpiar selección
              </button>
            </div>
          </div>
        </div>
        """

        html += f"""
        <div class="cards">
          <div class="card"><div class="label">Total</div><div class="value">{summary["total"]}</div></div>
          <div class="card"><div class="label">En cola</div><div class="value">{summary["queued"]}</div></div>
          <div class="card"><div class="label">Procesando</div><div class="value">{summary["processing"]}</div></div>
          <div class="card"><div class="label">Hecho</div><div class="value">{summary["done"]}</div></div>
          <div class="card"><div class="label">Error</div><div class="value">{summary["error"]}</div></div>
        </div>
    
        <div class="box">
          <div class="head"><strong>Resumen por proveedor</strong></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Proveedor</th>
                  <th class="right">Total</th>
                  <th class="right">EN COLA</th>
                  <th class="right">PROCESANDO</th>
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
                  <td>{_esc(r["provider_name"])}</td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["queued"]}</td>
                  <td class="right">{r["processing"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="6">Sin datos.</td></tr>'
    
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
                  <th class="right">EN COLA</th>
                  <th class="right">PROCESANDO</th>
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
                  <td class="right">{r["queued"]}</td>
                  <td class="right">{r["processing"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="6">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """

        html += f"""
        <div class="box">
          <div class="head"><strong>Vista de grupos</strong></div>
          <div class="group-mode-bar">
            <a class="group-mode-link {'group-mode-link-active' if group_mode == 'all' else ''}"
               href="/panel?view={_esc(view)}&group_mode=all&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}">
              Ver todos los grupos
            </a>
            <a class="group-mode-link {'group-mode-link-active' if group_mode == 'active' else ''}"
               href="/panel?view={_esc(view)}&group_mode=active&group_jid={_esc(group_jid)}&provider_name={_esc(provider_name)}&status={_esc(status)}&act_type={_esc(act_type)}">
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
            <span class="small">Bloquea o desbloquea todos los grupos cliente con un solo clic</span>
          </div>
          <div class="group-mode-bar">
            {toggle_all_btn}
          </div>
        </div>
        """
    
        html += """
        <div class="box">
          <div class="head collapsible-head open" onclick="toggleSection('grupoClienteBody', this)">
            <strong>Resumen por grupo cliente</strong>
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
                  <th>Acción</th>
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
                    if blocked
                    else f'<button class="btn btn-danger" onclick="toggleGroupBlock(\'{r["group_jid"]}\', \'block\')">Bloquear</button>'
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
          <div class="head collapsible-head closed" onclick="toggleSection('recentRequestsWrap', this)">
            <strong>Solicitudes recientes</strong>
            <span class="collapse-icon">▼</span>
          </div>
          <div id="recentRequestsWrap" class="collapsible-body closed">
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Dato</th>
                  <th>Tipo</th>
                  <th>Estado</th>
                  <th>Grupo cliente</th>
                  <th>Proveedor</th>
                  <th>Grupo proveedor</th>
                  <th>Mensaje proveedor</th>
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
                  <td>{_esc(_group_name(r.source_group_id))}</td>
                  <td>{_esc(r.provider_name)}</td>
                  <td>{_esc(_group_name(r.provider_group_id))}</td>
                  <td class="small">{_esc(r.provider_message)}</td>
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
        </div>
      </div>
    
      <script>
        let broadcastRunning = false;
    
        async function toggleProvider(provider, action) {
          const url = `/panel/provider/${provider}/${action}`;
    
          try {
            const res = await fetch(url, { method: "POST" });
            const data = await res.json();
    
            if (data.ok) {
              location.reload();
            } else {
              alert("Error cambiando proveedor");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }
        }
    
        async function refreshSID() {
          const sid = prompt("Pega el nuevo PHPSESSID");
          if (!sid) return;
    
          try {
            const res = await fetch("/panel/provider3/session", {
              method: "POST",
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                phpsessid: sid
              })
            });
    
            const data = await res.json();
    
            if (data.ok) {
              alert("SID actualizada");
              location.reload();
            } else {
              alert(data.error || "Error actualizando SID");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }
        }
    
        async function sendBroadcast(type) {
          const ok = confirm("¿Seguro que deseas enviar este mensaje masivamente?");
          if (!ok) return;

          if (broadcastRunning) return;
          broadcastRunning = true;
    
          try {
            const res = await fetch(`/panel/broadcast/${type}`, {
              method: "POST"
            });
    
            const data = await res.json();
    
            if (data.ok) {
              alert(data.message || "Envío iniciado");
            } else {
              alert(data.error || "Error en envío masivo");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }

          broadcastRunning = false;
        }
    
        async function sendFreeBroadcast() {
          const textarea = document.getElementById("broadcastMessage");
          const message = textarea.value.trim();
    
          if (!message) {
            alert("Escribe un mensaje");
            return;
          }
    
          const ok = confirm("¿Seguro que deseas enviar este mensaje masivamente?");
          if (!ok) return;

          if (broadcastRunning) return;
          broadcastRunning = true;
    
          try {
            const res = await fetch("/panel/broadcast/free", {
              method: "POST",
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                message: message
              })
            });
    
            const data = await res.json();
    
            if (data.ok) {
              alert(data.message || "Envío iniciado");
              textarea.value = "";
            } else {
              alert(data.error || "Error en envío masivo");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }

          broadcastRunning = false;
        }
    
        function clearBroadcast() {
          document.getElementById("broadcastMessage").value = "";
        }

        async function toggleGroupBlock(groupJid, action) {
          const msg = action === "block"
            ? "¿Bloquear este grupo? El bot dejará de responder silenciosamente."
            : "¿Desbloquear este grupo?";
        
          const ok = confirm(msg);
          if (!ok) return;
        
          try {
            const res = await fetch(`/panel/group/${encodeURIComponent(groupJid)}/${action}`, {
              method: "POST"
            });
        
            const data = await res.json();
        
            if (data.ok) {
              location.reload();
            } else {
              alert(data.error || "Error cambiando estado del grupo");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }
        }

        async function toggleAllGroups() {
          const ok = confirm("¿Seguro que deseas cambiar el estado de todos los grupos cliente?");
          if (!ok) return;
        
          try {
            const res = await fetch("/panel/groups/toggle-all", {
              method: "POST"
            });
        
            const data = await res.json();
        
            if (data.ok) {
              alert(data.message || "Estado actualizado");
              location.reload();
            } else {
              alert(data.error || "Error actualizando grupos");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }
        }

        async function applySharedPromotion() {
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
        
          if (!is_credit) {
            credit_abono_raw = "0";
            credit_debe_raw = "0";
          } else {
            if (credit_abono_raw === "") credit_abono_raw = "0";
            if (credit_debe_raw === "") credit_debe_raw = "0";
          }
        
          const credit_abono = Number(credit_abono_raw);
          const credit_debe = Number(credit_debe_raw);
        
          if (!selected.length) {
            alert("Selecciona al menos un grupo");
            return;
          }
        
          if (!total_actas || total_actas <= 0) {
            alert("Ingresa un total de actas válido");
            return;
          }
        
          if (!shared_key) {
            alert("Ingresa una bolsa compartida");
            return;
          }
        
          try {
            const res = await fetch("/panel/promotions/apply", {
              method: "POST",
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                selected_group_jids: selected,
                promo_name,
                client_key,
                shared_key,
                total_actas,
                price_per_piece,
                is_credit,
                credit_abono,
                credit_debe
              })
            });
        
            const data = await res.json();
        
            if (data.ok) {
              alert("Promoción compartida aplicada correctamente");
              location.reload();
            } else {
              alert(data.error || "No se pudo aplicar la promoción");
            }
          } catch (e) {
            alert("No se pudo conectar con el servidor");
          }
        }

        function toggleSharedPromoCreditFields() {
          const promoType = document.getElementById("sharedPromoType");
          const isCredit = promoType && promoType.value === "credit";
        
          const abono = document.getElementById("sharedPromoCreditAbono");
          const debe = document.getElementById("sharedPromoCreditDebe");
        
          if (abono) {
            if (isCredit) {
              abono.disabled = false;
              if (!abono.value) abono.value = 0;
            } else {
              abono.disabled = true;
              abono.value = "";
            }
          }
        
          if (debe) {
            if (isCredit) {
              debe.disabled = false;
              if (!debe.value) debe.value = 0;
            } else {
              debe.disabled = true;
              debe.value = "";
            }
          }
        }
        
        document.addEventListener("DOMContentLoaded", () => {
          const promoType = document.getElementById("sharedPromoType");
          if (promoType) {
            promoType.addEventListener("change", toggleSharedPromoCreditFields);
            toggleSharedPromoCreditFields();
          }
        
          filterSharedPromoGroups();
          startRecentRequestsStream();
        
          const sections = [
            "grupoClienteBody",
            "promoCompartidaBody",
            "recentRequestsWrap"
          ];
        
          sections.forEach(id => {
            const body = document.getElementById(id);
            const head = body?.previousElementSibling;
        
            if (!body || !head) return;
        
            const state = localStorage.getItem(id);
        
            if (state === "closed") {
              body.classList.remove("open");
              body.classList.add("closed");
              head.classList.add("closed");
            }
          });
        });

        function filterSharedPromoGroups() {
          const search = (document.getElementById("sharedPromoSearch")?.value || "").trim().toLowerCase();
          const showNormal = document.getElementById("filterNormalGroups")?.checked;
          const showTest = document.getElementById("filterTestGroups")?.checked;
          const showProvider = document.getElementById("filterProviderGroups")?.checked;
        
          const items = document.querySelectorAll(".shared-promo-item");
        
          items.forEach(item => {
            const name = item.dataset.name || "";
            const kind = item.dataset.kind || "normal";
        
            const matchesSearch = !search || name.includes(search);
        
            let matchesKind = false;
            if (kind === "normal" && showNormal) matchesKind = true;
            if (kind === "test" && showTest) matchesKind = true;
            if (kind === "provider" && showProvider) matchesKind = true;
        
            item.style.display = (matchesSearch && matchesKind) ? "flex" : "none";
          });
        }
        
        function clearSharedPromotionSelection() {
          document.querySelectorAll(".shared-promo-group").forEach(el => {
            el.checked = false;
          });
        
          const searchInput = document.getElementById("sharedPromoSearch");
          if (searchInput) searchInput.value = "";
        
          const normal = document.getElementById("filterNormalGroups");
          const test = document.getElementById("filterTestGroups");
          const provider = document.getElementById("filterProviderGroups");
        
          if (normal) normal.checked = true;
          if (test) test.checked = false;
          if (provider) provider.checked = false;
        
          filterSharedPromoGroups();
        }

        function toggleSection(bodyId, headEl) {
          const body = document.getElementById(bodyId);
          if (!body) return;
        
          const isClosed = body.classList.contains("closed");
        
          if (isClosed) {
            body.classList.remove("closed");
            body.classList.add("open");
            headEl.classList.remove("closed");    
            localStorage.setItem(bodyId, "open");
          } else {
            body.classList.remove("open");
            body.classList.add("closed");
            headEl.classList.add("closed");
            localStorage.setItem(bodyId, "closed");
          }
        }

        async function refreshRecentRequests() {
          const wrap = document.getElementById("recentRequestsWrap");
          if (!wrap) return;
        
          const params = new URLSearchParams({
            view: document.querySelector('input[name="view"]')?.value || "day",
            group_jid: document.querySelector('input[name="group_jid"]')?.value || "",
            provider_name: document.querySelector('input[name="provider_name"]')?.value || "",
            status: document.querySelector('input[name="status"]')?.value || "",
            act_type: document.querySelector('input[name="act_type"]')?.value || "",
          });
        
          try {
            const res = await fetch(`/panel/recent-requests?${params.toString()}`);
            if (!res.ok) throw new Error("No se pudo actualizar solicitudes recientes");
        
            const html = await res.text();
            wrap.innerHTML = html;
          } catch (e) {
            console.error("RECENT_REQUESTS_REFRESH_ERROR =", e);
          }
        }
        
        let recentRequestsEventSource = null;
        
        function startRecentRequestsStream() {
          if (recentRequestsEventSource) {
            recentRequestsEventSource.close();
          }
        
          recentRequestsEventSource = new EventSource("/panel/recent-requests/stream");
        
          recentRequestsEventSource.onmessage = async function(event) {
            try {
              const data = JSON.parse(event.data || "{}");
              if (data.error) {
                console.error("RECENT_REQUESTS_STREAM_ERROR =", data.error);
                return;
              }
        
              if (!document.hidden) {
                await refreshRecentRequests();
              }
            } catch (e) {
              console.error("RECENT_REQUESTS_STREAM_PARSE_ERROR =", e);
            }
          };
        
          recentRequestsEventSource.onerror = function(err) {
            console.error("RECENT_REQUESTS_STREAM_CONNECTION_ERROR =", err);
        
            try {
              recentRequestsEventSource.close();
            } catch (_) {}
        
            setTimeout(() => {
              startRecentRequestsStream();
            }, 5000);
          };
        }

      </script>
    </body>
    </html>
        """
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


@app.post("/panel/provider/{provider_name}/on")
def panel_provider_on(provider_name: str, db: Session = Depends(get_db)):
    row = _get_or_create_provider(db, provider_name.upper(), provider_name.upper() == "PROVIDER1")
    row.is_enabled = True
    row.updated_at = _utc_now_naive()
    db.commit()
    return {"ok": True, "provider": provider_name.upper(), "enabled": True}


@app.post("/panel/provider/{provider_name}/off")
def panel_provider_off(provider_name: str, db: Session = Depends(get_db)):
    row = _get_or_create_provider(db, provider_name.upper(), provider_name.upper() == "PROVIDER1")
    row.is_enabled = False
    row.updated_at = _utc_now_naive()
    db.commit()
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

        current = _get_app_setting(db, "PROVIDER3_PHPSESSID", "")
        if not current and settings.PROVIDER3_PHPSESSID:
            _set_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)
    finally:
        db.close()


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


def _deliver_text_result(req: RequestLog, text: str):
    if req.source_group_id:
        send_group_text(req.source_group_id, text)
    else:
        send_text(req.requester_wa_id, text)


def _deliver_pdf_result(req: RequestLog, pdf_data: str, filename: str | None = None):
    filename = filename or f"{req.curp}.pdf"

    caption_text = ""

    NO_TIME_CAPTION_GROUPS = {
        "120363408668441985@g.us",
        "120363421166637606@g.us",
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

    print("PDF_CAPTION =", caption_text, flush=True)

    is_base64 = not pdf_data.startswith("http")

    if req.source_group_id:
        if is_base64:
            send_group_document_base64(
                req.source_group_id,
                pdf_data,
                filename=filename,
                caption=caption_text
            )
        else:
            send_group_document(
                req.source_group_id,
                pdf_data,
                filename=filename,
                caption=caption_text
            )
    else:
        if is_base64:
            send_document_base64(
                req.requester_wa_id,
                pdf_data,
                filename=filename,
                caption=caption_text
            )
        else:
            send_document(
                req.requester_wa_id,
                pdf_data,
                filename=filename,
                caption=caption_text
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


def _is_admin(requester_wa_id: str, from_me: bool = False) -> bool:
    raw = settings.ADMIN_PHONE or ""

    admins = [
        x.strip().replace("+", "").replace(" ", "")
        for x in raw.split(",")
        if x.strip()
    ]

    requester = (requester_wa_id or "").replace("+", "").replace(" ", "").strip()

    return from_me or requester in admins
    

def _reply_to_origin(source_group_id: str | None, requester_wa_id: str, text: str):
    if source_group_id:
        send_group_text(source_group_id, text)
    else:
        send_text(requester_wa_id, text)


def _all_provider_groups() -> set[str]:
    vals = {
        settings.PROVIDER_GROUP_NACIMIENTO_1,
        settings.PROVIDER_GROUP_NACIMIENTO_2,
        settings.PROVIDER_GROUP_NACIMIENTO_3,
        settings.PROVIDER_GROUP_ESPECIALES,
        settings.PROVIDER2_GROUP_1,
        settings.PROVIDER2_GROUP_2,
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

    return (
        db.query(GroupPromotion)
        .filter(
            GroupPromotion.group_jid == group_jid,
            GroupPromotion.is_active == True
        )
        .first()
    )


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

    available = _promotion_available(promo)

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
    from app.services.provider3 import Provider3Client
    from app.services.provider4 import Provider4Client

    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)
    p3 = _get_or_create_provider(db, "PROVIDER3", False)
    p4 = _get_or_create_provider(db, "PROVIDER4", False)

    s1 = "ON" if p1.is_enabled else "OFF"
    s2 = "ON" if p2.is_enabled else "OFF"
    s3 = "ON" if p3.is_enabled else "OFF"
    s4 = "ON" if p4.is_enabled else "OFF"

    provider3_extra = ""

    try:
        phpsessid = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)
        if phpsessid:
            client = Provider3Client(phpsessid=phpsessid)
            lic = client.get_licenses()

            curp_left = lic.get("acta_curp")
            cadena_left = lic.get("acta_cadena")

            provider3_extra = (
                f" | CURP restantes: {curp_left if curp_left is not None else 'N/D'}"
                f" | CADENA restantes: {cadena_left if cadena_left is not None else 'N/D'}"
            )
        else:
            provider3_extra = " | SIN PHPSESSID"
    except Exception as e:
        provider3_extra = f" | ERROR LICENCIAS: {str(e)}"

    return (
        f"PROVEEDOR WA EMERGENCIA: {s1}\n"
        f"AUSTRAM WEB: {s3}{provider3_extra}\n"
        f"LAZARO WEB: {s4}"
    )


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
    if is_group and participant:
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

        # NO reiniciar consumidas al editar
        row.used_actas = row.used_actas or 0
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
        )
        db.add(row)
        db.flush()

    available = max(0, (row.total_actas or 0) - (row.used_actas or 0))

    row.warning_sent_200 = available <= 200
    row.warning_sent_100 = available <= 100
    row.warning_sent_50 = available <= 50
    row.warning_sent_10 = available <= 10
    row.warning_sent_0 = available <= 0

    db.commit()

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
    row.updated_at = _utc_now_naive()
    db.commit()

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
    return {"ok": True, "group_jid": group_jid, "blocked": True}


@app.post("/panel/group/{group_jid}/unblock")
def panel_unblock_group(group_jid: str):
    print("PANEL_UNBLOCK_GROUP =", group_jid, flush=True)
    unblock_group(group_jid)
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
    

@app.post("/webhook/evolution")
async def evolution_webhook(payload: dict, db: Session = Depends(get_db)):
    try:
        event = payload.get("event", "")
        data = payload.get("data", {})

        if event != "messages.upsert":
            return {"ok": True, "ignored": event}

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
        print("EARLY_MESSAGE_KEYS =", list(message.keys()), flush=True)
        
        if from_me and not any(text_upper.startswith(cmd) for cmd in admin_commands):
            print("IGNORED_REASON = from_me", flush=True)
            print("IGNORED_FROM_ME_REMOTE_JID =", remote_jid, flush=True)
            print("IGNORED_FROM_ME_MSG_ID =", msg_id, flush=True)
            return {"ok": True, "ignored": "from_me"}

        provider_groups = _all_provider_groups()
        is_provider_message = source_chat_id in provider_groups
        is_admin_command = text_upper.startswith("/")

        print("WEBHOOK_SOURCE_GROUP_ID =", source_group_id, flush=True)
        print("WEBHOOK_IS_GROUP_BLOCKED =", is_group_blocked(source_group_id), flush=True)

        if is_group and is_group_blocked(source_group_id) and not (is_admin_command and _is_admin(requester_wa_id, from_me)):
            print("IGNORED_REASON = group_blocked", flush=True)
            print("IGNORED_GROUP =", source_group_id, flush=True)
            return {"ok": True, "ignored": "group_blocked"}

        terms = extract_request_terms(text_body)
        problem = detect_identifier_problem(text_body)

        if not bot_is_open() and terms and not is_provider_message and not is_admin_command:
            msg = (
                "🚀 *DOCU EXPRES*\n"
                "El sistema está cerrado.\n\n"
                "Horario de solicitudes:\n"
                "🕗 8:00 AM - 10:00 PM\n"
                "Horario América/Monterrey."
            )

            if source_group_id:
                send_group_text(source_group_id, msg)
            else:
                send_text(requester_wa_id, msg)

            return {"ok": True, "ignored": "outside_hours"}

        # =========================
        # RESPUESTA DEL PROVEEDOR
        # =========================
        if is_provider_message and not is_admin_command:
            provider_id = _extract_provider_identifier_loose(text_body or "")
            print("PROVIDER_GROUP =", source_chat_id, flush=True)
            print("PROVIDER_TEXT =", text_body, flush=True)
            print("PROVIDER_IDENTIFIER_DETECTED =", provider_id, flush=True)

            # 1) INTENTAR DETECTAR PDF
            doc = None
            media_message_id = msg_id
            
            msg_unwrapped = _unwrap_message(message) or message
            
            if "documentMessage" in msg_unwrapped:
                doc = msg_unwrapped.get("documentMessage")
                media_message_id = msg_id
            
            elif "documentWithCaptionMessage" in msg_unwrapped:
                doc_wrap = msg_unwrapped.get("documentWithCaptionMessage", {})
                doc = doc_wrap.get("message", {}).get("documentMessage")
                media_message_id = msg_id
            
            elif "extendedTextMessage" in msg_unwrapped:
                ext = msg_unwrapped.get("extendedTextMessage", {})
                ctx = ext.get("contextInfo", {}) or {}
                quoted = _unwrap_message(ctx.get("quotedMessage", {}) or {})
            
                quoted_msg_id = ctx.get("stanzaId", "") or ctx.get("quotedStanzaID", "") or ""
            
                if "documentMessage" in quoted:
                    doc = quoted.get("documentMessage")
                    media_message_id = quoted_msg_id or msg_id
            
                elif "documentWithCaptionMessage" in quoted:
                    doc_wrap = quoted.get("documentWithCaptionMessage", {})
                    doc = doc_wrap.get("message", {}).get("documentMessage")
                    media_message_id = quoted_msg_id or msg_id
            
            print("MEDIA_MESSAGE_ID_USED =", media_message_id, flush=True)

            if doc:
                filename = doc.get("fileName") or ""
                pdf_url = doc.get("url") or doc.get("directPath") or ""
                filename_id = _extract_identifier_from_filename_local(filename)

                print("PROVIDER_DOC_FILENAME =", filename, flush=True)
                print("PROVIDER_DOC_FILENAME_IDENTIFIER =", filename_id, flush=True)
                print("PROVIDER_DOC_URL =", pdf_url, flush=True)

                open_req = None

                if filename_id:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.curp == filename_id,
                            RequestLog.status == "PROCESSING"
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )
                
                if not open_req and not filename_id and provider_id:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_group_id == source_chat_id,
                            RequestLog.curp == provider_id,
                            RequestLog.status == "PROCESSING"
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )

                if not open_req:
                    print("PROVIDER_PDF_WITHOUT_MATCH =", filename, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_without_match"}
                
                match_term = filename_id or provider_id or open_req.curp or "NO_TERM"
                pdf_dedupe_key = f"provider_pdf:{source_chat_id}:{match_term}:{filename or 'nofile'}"
                
                already_sent = redis_conn.set(pdf_dedupe_key, "1", ex=3600, nx=True)
                if not already_sent:
                    print("PROVIDER_PDF_DUPLICATE_IGNORED =", pdf_dedupe_key, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_duplicate"}
                
                media_json = get_media_base64("document", media_message_id)
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
                
                pdf_bytes = base64.b64decode(media_b64, validate=False)

                print("PDF_HEADER =", pdf_bytes[:8], flush=True)
                print("PDF_BYTES_LEN =", len(pdf_bytes), flush=True)
                
                if b"%PDF" not in pdf_bytes[:20]:
                    print("PROVIDER_PDF_INVALID_BINARY", flush=True)
                    return {"ok": True, "ignored": "provider_pdf_invalid_binary"}
                
                safe_media_b64 = base64.b64encode(pdf_bytes).decode()
                
                open_req.pdf_url = None
                open_req.provider_media_url = "BASE64_FROM_MEDIA_MESSAGE"
                open_req.status = "DONE"
                open_req.updated_at = _utc_now_naive()
                
                db.commit()
                
                print("PROVIDER_PDF_MATCHED_REQ_ID =", open_req.id, flush=True)
                print("PROVIDER_PDF_MATCHED_CURP =", open_req.curp, flush=True)
                print("PROVIDER_PDF_BASE64_LEN =", len(safe_media_b64), flush=True)
                
                _deliver_pdf_result(open_req, safe_media_b64, filename=filename or f"{open_req.curp}.pdf")
                
                return {"ok": True, "provider_result": "pdf_delivered"}

            # 2) SI NO HAY PDF, INTENTAR TEXTO
            open_req = None
            if provider_id:
                open_req = (
                    db.query(RequestLog)
                    .filter(
                        RequestLog.provider_group_id == source_chat_id,
                        RequestLog.curp == provider_id,
                        RequestLog.status == "PROCESSING"
                    )
                    .order_by(RequestLog.created_at.asc())
                    .first()
                )

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
                    f"❌ No hay registros disponibles.\nDato: {open_req.curp}\nTipo: {open_req.act_type}\n\nVerificar que la CURP esté certificada en RENAPO"
                )
                return {"ok": True, "provider_result": "no_record"}

            print("PROVIDER_RAW_MESSAGE_KEYS =", list(message.keys()), flush=True)
            print("PROVIDER_RAW_MESSAGE =", message, flush=True)
            print("PROVIDER_UNHANDLED_MESSAGE =", message, flush=True)
            return {"ok": True, "ignored": "provider_unhandled_message"}

        if not terms and not is_admin_command:
            if problem:
                if source_group_id:
                    send_group_text(source_group_id, problem)
                else:
                    send_text(requester_wa_id, problem)
        
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
                send_group_text(source_group_id, f"🆔 Group ID:\n{source_group_id}")
            else:
                send_text(requester_wa_id, "⚠️ Usa /GROUPID dentro de un grupo.")

            return {"ok": True}
        
        if text_upper.startswith("/ADDGROUP"):
            if not _is_admin(requester_wa_id, from_me):
                print("ADDGROUP_DENIED_USER =", requester_wa_id, flush=True)
                return {"ok": True, "ignored": "not_admin"}

            if is_group:
                if not db.query(AuthorizedGroup).filter_by(group_jid=source_group_id).first():
                    db.add(AuthorizedGroup(group_jid=source_group_id, group_name=""))
                    db.commit()

                send_group_text(source_group_id, f"✅ Grupo autorizado: {source_group_id}")

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
                    send_group_document(
                        source_group_id,
                        last.pdf_url,
                        filename=f"{last.curp}.pdf",
                        caption="♻️ Reenviado desde historial"
                    )
                else:
                    send_document(
                        requester_wa_id,
                        last.pdf_url,
                        filename=f"{last.curp}.pdf",
                        caption="♻️ Reenviado desde historial"
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
        if is_group and not is_authorized_group(db, source_group_id):
            print("IGNORED_REASON = group_not_authorized", flush=True)
            print("IGNORED_GROUP =", source_group_id, flush=True)
            return {"ok": True, "ignored": "group_not_authorized"}
        
        if not is_group and not is_authorized_user(db, requester_wa_id):
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
                    send_group_text(source_group_id, final_msg)
                else:
                    send_text(requester_wa_id, final_msg)
        
            return {"ok": True, "ignored": "no_identifier"}
        
        act_type = detect_act_type(text_body)
        print("REQUEST_ACT_TYPE =", act_type, flush=True)

        created_any = False

        for term in terms:
            print("PROCESSING_TERM =", term, flush=True)
        
            #last_done = get_last_done_request(db, term, act_type)
        
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
                dup_msg = (
                    f"⏳ Ya existe una solicitud en proceso\n"
                    f"Dato: {term}\n"
                    f"Tipo: {act_type}"
                )
        
                if source_group_id:
                    send_group_text(source_group_id, dup_msg)
                else:
                    send_text(requester_wa_id, dup_msg)
        
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
                    send_group_text(source_group_id, limit_msg)
                else:
                    send_text(requester_wa_id, limit_msg)
        
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
                    send_group_text(source_group_id, retry_msg)
                else:
                    send_text(requester_wa_id, retry_msg)
        
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
                    send_group_text(source_group_id, dup_msg)
                else:
                    send_text(requester_wa_id, dup_msg)
            
                continue
        
            request_queue.enqueue(process_request, row.id)
            created_any = True
        
            print("ENQUEUED_REQUEST_ID =", row.id, flush=True)
            print("ENQUEUED_TERM =", row.curp, flush=True)
            print("ENQUEUED_TYPE =", row.act_type, flush=True)
            print("ENQUEUED_SOURCE_GROUP =", row.source_group_id, flush=True)

        if created_any:
            actor = push_name or requester_wa_id
            ack_msg = (
                f"🚀 DOCU EXPRES\n"
                f"Solicitud recibida de {actor}.\n"
                f"Esto puede tardar unos segundos..."
            )
        
            if source_group_id:
                send_group_text(source_group_id, ack_msg)
            else:
                send_text(requester_wa_id, ack_msg)
        else:
            print("IGNORED_REASON = nothing_created", flush=True)
        
        return {"ok": True}

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), payload)
        return {"ok": False, "error": str(e)}
