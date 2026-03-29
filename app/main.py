import os
import base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, Body, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import Base, engine, get_db
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog, ProviderSetting, AppSetting
from app.queue import request_queue, redis_conn
from app.worker import process_request, provider3_keepalive_job
from app.services.provider3 import Provider3Client

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

    return provider3_keepalive_job()


def bot_is_open():
    now = datetime.now(ZoneInfo("America/Monterrey"))
    hour = now.hour
    return 8 <= hour < 22


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
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)


def _panel_period_bounds(view: str):
    view = (view or "day").strip().lower()

    if view == "week":
        start = _panel_week_start()
        end = _panel_week_end()
        return start, end, "week"

    now = _panel_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end, "day"


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
        q = q.filter(RequestLog.source_group_id == group_jid)

    if provider_name:
        q = q.filter(RequestLog.provider_name == provider_name)

    if status:
        q = q.filter(RequestLog.status == status)

    if act_type:
        q = q.filter(RequestLog.act_type == act_type)

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


def _panel_group_rows(rows: list[RequestLog]) -> list[dict]:
    data = {}

    for r in rows:
        gid = r.source_group_id or "PRIVADO"
        if gid not in data:
            data[gid] = {
                "group_jid": gid,
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
    out.sort(key=lambda x: (-x["total"], x["group_jid"]))
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


BROADCAST_ACTIVAS_MSG = """*SERVICIO DE ACTAS SUPER RAPIDAS SALIENDO EN SEGUNDOS*
⚡⚡⚡MANDEN, MANDEN💫💫

*SOLICITALAS POR:*
*CURP*
*CADENA (Identificador Electrónico)*
*CODIGO DE VERIFICACION*
*CON FOLIO O SIN FOLIO.*

HORARIOS DE LUNES A SABADO
DE 08:00 AM A 10:00 PM
"""

BROADCAST_MANTENIMIENTO_MSG = """🚧 *DOCU EXPRES EN MANTENIMIENTO*

Por el momento el sistema se encuentra en mantenimiento temporalmente.

Apenas quede restablecido les avisaremos por este medio.
Gracias por su comprensión.
"""

BROADCAST_SUSPENDIDO_MSG = """⛔ *DOCU EXPRES SUSPENDIDO TEMPORALMENTE*

Por el momento el servicio está suspendido temporalmente.
En cuanto vuelva a operar les avisaremos por este medio.
Gracias por su paciencia.
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
    "120363422785755828@g.us": "Gpo. No. 4 Karen",
    "120363426949877636@g.us": "Gpo. No. 11 Morelos",
    "120363425014097597@g.us": "Gpo. No. 7 Karen Marvin",
    "120363425275514736@g.us": "Gpo. No. 8 Ana Marvin",
    "120363406182077605@g.us": "Gpo. No. 12 Marvin",
    "120363425721043776@g.us": "Gpo. No. 3 Rodolfo",
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
    "120363427788039518@g.us": "Docify Mx 1",
    "120363424360403186@g.us": "Docify Mx 2",
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
    "120363422073988332@g.us": "Gpo. No. Day",
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
}


def _group_name(jid: str):
    if not jid:
        return ""
    return GROUP_NAME_MAP.get(jid, jid)
    

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
    by_group = _panel_group_rows(rows)
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


@app.post("/panel/broadcast/mantenimiento")
def panel_broadcast_mantenimiento(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_broadcast_job, BROADCAST_MANTENIMIENTO_MSG)
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


@app.get("/panel", response_class=HTMLResponse)
def panel_actas(
    view: str = "day",
    group_jid: str = "",
    provider_name: str = "",
    status: str = "",
    act_type: str = "",
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
        by_group = _panel_group_rows(rows)
        by_provider = _panel_provider_rows(rows)
        by_type = _panel_type_rows(rows)
    
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
            <a href="/panel?view=day" class="tool-link {'tool-link-active' if view == 'day' else ''}">Hoy</a>
            <a href="/panel?view=week" class="tool-link {'tool-link-active' if view == 'week' else ''}">Semana</a>
          </div>
    
          <div class="grid-hero">
            <div class="glass">
              <h3 class="section-title">Proveedores</h3>
    
              <div class="provider-grid">
                <div class="provider-card">
                  <div class="provider-name">ADMID DIGITAL</div>
                  <div class="provider-actions">
                    <button class="btn btn-success" onclick="toggleProvider('PROVIDER1','on')">Activar</button>
                    <button class="btn btn-danger" onclick="toggleProvider('PROVIDER1','off')">Desactivar</button>
                  </div>
                </div>
    
                <div class="provider-card">
                  <div class="provider-name">AUSTRAM BOT</div>
                  <div class="provider-actions">
                    <button class="btn btn-success" onclick="toggleProvider('PROVIDER2','on')">Activar</button>
                    <button class="btn btn-danger" onclick="toggleProvider('PROVIDER2','off')">Desactivar</button>
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
                    <button class="btn btn-primary" onclick="sendBroadcast('activas')">Enviar promoción</button>
                    <button class="btn btn-warning" onclick="sendBroadcast('mantenimiento')">Enviar mantenimiento</button>
                    <button class="btn btn-danger" onclick="sendBroadcast('suspendido')">Enviar suspendido</button>
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
            <input name="group_jid" placeholder="Grupo cliente" value="{_esc(group_jid)}">
            <input name="provider_name" placeholder="Proveedor" value="{_esc(provider_name)}">
            <input name="status" placeholder="Estado" value="{_esc(status)}">
            <input name="act_type" placeholder="Tipo de acta" value="{_esc(act_type)}">
            <button type="submit" class="btn btn-primary">Filtrar</button>
          </div>
        </form>
    
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
    
        html += """
        <div class="box">
          <div class="head"><strong>Resumen por grupo cliente</strong></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Grupo</th>
                  <th class="right">Total</th>
                  <th class="right">EN COLA</th>
                  <th class="right">PROCESANDO</th>
                  <th class="right">HECHO</th>
                  <th class="right">ERROR</th>
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
        
                action_btn = (
                    f'<button class="btn btn-success" onclick="toggleGroupBlock(\'{r["group_jid"]}\', \'unblock\')">Desbloquear</button>'
                    if blocked
                    else f'<button class="btn btn-danger" onclick="toggleGroupBlock(\'{r["group_jid"]}\', \'block\')">Bloquear</button>'
                )
        
                html += f"""
                <tr>
                  <td>{_esc(_group_name(r["group_jid"]))}</td>
                  <td class="right">{r["total"]}</td>
                  <td class="right">{r["queued"]}</td>
                  <td class="right">{r["processing"]}</td>
                  <td class="right">{r["done"]}</td>
                  <td class="right">{r["error"]}</td>
                  <td>{_esc(_fmt_dt(r["last_update"]))}</td>
                  <td>{blocked_text}</td>
                  <td>{action_btn}</td>
                </tr>
                """
        else:
            html += '<tr><td colspan="9">Sin datos.</td></tr>'
    
        html += """
              </tbody>
            </table>
          </div>
        </div>
        """
    
        html += """
        <div class="box">
          <div class="head"><strong>Solicitudes recientes</strong></div>
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
    
        setInterval(() => {
          if (!broadcastRunning) {
            location.reload();
          }
        }, 30000);
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
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "provider": provider_name.upper(), "enabled": True}


@app.post("/panel/provider/{provider_name}/off")
def panel_provider_off(provider_name: str, db: Session = Depends(get_db)):
    row = _get_or_create_provider(db, provider_name.upper(), provider_name.upper() == "PROVIDER1")
    row.is_enabled = False
    row.updated_at = datetime.utcnow()
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

    if req.created_at:
        delta = datetime.utcnow() - req.created_at
        total_seconds = max(0.0, delta.total_seconds())

        if total_seconds >= 60:
            minutes = int(total_seconds // 60)
            seconds = total_seconds % 60
            tiempo = f"{minutes} min {seconds:.2f} segundos"
        else:
            tiempo = f"{total_seconds:.2f} segundos"

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
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _get_app_setting(db: Session, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if not row or row.value is None:
        return default
    return row.value.strip()


def _set_app_setting(db: Session, key: str, value: str):
    row = db.query(AppSetting).filter(AppSetting.key == key).first()

    if row:
        row.value = value
        row.updated_at = datetime.utcnow()
    else:
        row = AppSetting(
            key=key,
            value=value,
            updated_at=datetime.utcnow(),
        )
        db.add(row)

    db.commit()
    return row


def _providers_status_text(db: Session) -> str:
    from app.services.provider3 import Provider3Client

    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)
    p3 = _get_or_create_provider(db, "PROVIDER3", False)

    s1 = "ON" if p1.is_enabled else "OFF"
    s2 = "ON" if p2.is_enabled else "OFF"
    s3 = "ON" if p3.is_enabled else "OFF"

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
        f"ADMID DIGITAL: {s1}\n"
        f"AUSTRAM BOT: {s2}\n"
        f"AUSTRAM WEB: {s3}{provider3_extra}"
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
        
        if from_me and not any(text_upper.startswith(cmd) for cmd in admin_commands):
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

        if not terms and not is_admin_command:
            if problem:
                if source_group_id:
                    send_group_text(source_group_id, problem)
                else:
                    send_text(requester_wa_id, problem)
        
                return {"ok": True, "ignored": "invalid_identifier"}
        
            # Conversación natural: no marcar como error
            return {"ok": True, "ignored": "natural_text"}

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

            if "documentMessage" in message:
                doc = message.get("documentMessage")
            elif "documentWithCaptionMessage" in message:
                doc_wrap = message.get("documentWithCaptionMessage", {})
                doc = doc_wrap.get("message", {}).get("documentMessage")
            elif "extendedTextMessage" in message:
                ext = message.get("extendedTextMessage", {})
                ctx = ext.get("contextInfo", {})
                quoted = ctx.get("quotedMessage", {})

                if "documentMessage" in quoted:
                    doc = quoted.get("documentMessage")
                elif "documentWithCaptionMessage" in quoted:
                    doc_wrap = quoted.get("documentWithCaptionMessage", {})
                    doc = doc_wrap.get("message", {}).get("documentMessage")

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
                            RequestLog.status.in_(["PROCESSING", "QUEUED", "PENDING"])
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
                            RequestLog.status.in_(["PROCESSING", "QUEUED", "PENDING"])
                        )
                        .order_by(RequestLog.created_at.desc())
                        .first()
                    )

                if not open_req:
                    print("PROVIDER_PDF_WITHOUT_MATCH =", filename, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_without_match"}

                media_json = get_media_base64("document", msg_id)
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
                
                if not pdf_bytes.startswith(b"%PDF"):
                    print("PROVIDER_PDF_INVALID_BINARY", flush=True)
                    return {"ok": True, "ignored": "provider_pdf_invalid_binary"}
                
                safe_media_b64 = base64.b64encode(pdf_bytes).decode()
                
                open_req.pdf_url = None
                open_req.provider_media_url = "BASE64_FROM_MEDIA_MESSAGE"
                open_req.status = "DONE"
                open_req.updated_at = datetime.utcnow()
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
                open_req.updated_at = datetime.utcnow()
                db.commit()

                _deliver_text_result(
                    open_req,
                    f"❌ No hay registros disponibles.\nDato: {open_req.curp}\nTipo: {open_req.act_type}\n\nVerificar que la CURP esté certificada en RENAPO"
                )
                return {"ok": True, "provider_result": "no_record"}

            print("PROVIDER_UNHANDLED_MESSAGE =", message, flush=True)
            return {"ok": True, "ignored": "provider_unhandled_message"}

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
                last.updated_at = datetime.utcnow()
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
            row.updated_at = datetime.utcnow()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER1 activado")
            return {"ok": True}

        if text_upper in ("/P1 OFF", "/PROVIDER1 OFF"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER1", True)
            row.is_enabled = False
            row.updated_at = datetime.utcnow()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER1 desactivado")
            return {"ok": True}

        if text_upper in ("/P2 ON", "/PROVIDER2 ON"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER2", False)
            row.is_enabled = True
            row.updated_at = datetime.utcnow()
            db.commit()

            _reply_to_origin(source_group_id, requester_wa_id, "✅ PROVIDER2 activado")
            return {"ok": True}

        if text_upper in ("/P2 OFF", "/PROVIDER2 OFF"):
            if not _is_admin(requester_wa_id, from_me):
                return {"ok": True, "ignored": "not_admin"}

            row = _get_or_create_provider(db, "PROVIDER2", False)
            row.is_enabled = False
            row.updated_at = datetime.utcnow()
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
                error_existing.updated_at = datetime.utcnow()
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
                error_existing.expires_at = datetime.utcnow() + timedelta(days=settings.HISTORY_DAYS)
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
                expires_at=datetime.utcnow() + timedelta(days=settings.HISTORY_DAYS),
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
