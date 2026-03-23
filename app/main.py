from datetime import datetime, timedelta
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Base, engine, get_db
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog
from app.queue import request_queue
from app.worker import process_request
from app.utils.curp import extract_curps, detect_act_type, normalize_text
from app.services.evolution import send_text, send_document

app = FastAPI(title=settings.APP_NAME)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


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


@app.post("/webhook/evolution")
async def evolution_webhook(payload: dict, db: Session = Depends(get_db)):
    try:
        event = payload.get("event", "")
        data = payload.get("data", {})

        if event != "messages.upsert":
            return {"ok": True, "ignored": event}

        key = data.get("key", {})
        message = data.get("message", {})

        remote_jid = key.get("remoteJid", "")
        from_me = key.get("fromMe", False)
        participant = key.get("participant", "")
        msg_id = key.get("id", "")

        if from_me:
            return {"ok": True, "ignored": "from_me"}

        text_body = ""
        if "conversation" in message:
            text_body = message.get("conversation", "")
        elif "extendedTextMessage" in message:
            text_body = message.get("extendedTextMessage", {}).get("text", "")

        if not text_body:
            return {"ok": True, "ignored": "no_text"}

        text_upper = normalize_text(text_body)

        is_group = remote_jid.endswith("@g.us")
        source_chat_id = remote_jid
        source_group_id = remote_jid if is_group else None
        requester_wa_id = participant.replace("@s.whatsapp.net", "") if is_group and participant else remote_jid.replace("@s.whatsapp.net", "")

        if text_upper.startswith("/ADDUSER "):
            wa = text_upper.replace("/ADDUSER", "").strip()
            if wa and not db.query(AuthorizedUser).filter_by(wa_id=wa).first():
                db.add(AuthorizedUser(wa_id=wa))
                db.commit()
            send_text(requester_wa_id, f"✅ Usuario autorizado: {wa}")
            return {"ok": True}

        if text_upper.startswith("/ADDGROUP "):
            grp = text_upper.replace("/ADDGROUP", "").strip()
            if grp and not db.query(AuthorizedGroup).filter_by(group_jid=grp).first():
                db.add(AuthorizedGroup(group_jid=grp))
                db.commit()
            send_text(requester_wa_id, f"✅ Grupo autorizado: {grp}")
            return {"ok": True}

        if text_upper.startswith("/STATUS"):
            total = db.query(RequestLog).count()
            pending = db.query(RequestLog).filter(RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])).count()
            done = db.query(RequestLog).filter(RequestLog.status == "DONE").count()
            send_text(requester_wa_id, f"📊 Total: {total}\n⏳ Pendientes: {pending}\n✅ Entregadas: {done}")
            return {"ok": True}

        if is_group and not is_authorized_group(db, source_group_id):
            return {"ok": True, "ignored": "group_not_authorized"}

        if not is_authorized_user(db, requester_wa_id):
            send_text(requester_wa_id, "⛔ Tu número no está autorizado.")
            return {"ok": True}

        curps = extract_curps(text_body)
        if not curps:
            return {"ok": True, "ignored": "no_curp"}

        act_type = detect_act_type(text_body)

        for curp in curps:
            last_done = get_last_done_request(db, curp, act_type)
            if last_done and last_done.pdf_url and last_done.expires_at > datetime.utcnow():
                send_document(
                    requester_wa_id,
                    last_done.pdf_url,
                    filename=f"{curp}_{act_type}.pdf",
                    caption="♻️ Reenviado desde historial"
                )
                continue

            request_key = build_request_key(curp, act_type, source_chat_id)

            duplicate_open = (
                db.query(RequestLog)
                .filter(
                    RequestLog.request_key == request_key,
                    RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])
                )
                .first()
            )
            if duplicate_open:
                send_text(
                    requester_wa_id,
                    f"⏳ Ya existe una solicitud en proceso\nCURP: {curp}\nTipo: {act_type}\nFolio: {duplicate_open.id}"
                )
                continue

            row = RequestLog(
                request_key=request_key,
                curp=curp,
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
            db.commit()
            db.refresh(row)

            request_queue.enqueue(process_request, row.id)

        send_text(requester_wa_id, f"✅ Solicitud recibida. CURPs detectadas: {len(curps)}")
        return {"ok": True}

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/provider/result")
def provider_result(payload: dict, db: Session = Depends(get_db)):
    request_id = payload.get("request_id")
    pdf_url = payload.get("pdf_url")
    provider_ref = payload.get("provider_ref")

    req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
    if not req:
        raise HTTPException(status_code=404, detail="request_id no encontrado")

    req.pdf_url = pdf_url
    req.provider_ref = provider_ref
    req.status = "DONE"
    req.updated_at = datetime.utcnow()
    db.commit()

    send_document(
        req.requester_wa_id,
        req.pdf_url,
        filename=f"{req.curp}_{req.act_type}.pdf",
        caption="✅ Aquí está tu acta"
    )

    return {"ok": True}