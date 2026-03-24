from datetime import datetime, timedelta
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Base, engine, get_db
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog
from app.queue import request_queue
from app.worker import process_request
from app.utils.curp import extract_curps, detect_act_type, normalize_text
from app.services.evolution import send_text, send_document, send_group_document, send_group_text

import re

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


def _deliver_text_result(req: RequestLog, text: str):
    if req.source_group_id:
        send_group_text(req.source_group_id, text)
    else:
        send_text(req.requester_wa_id, text)


def _deliver_pdf_result(req: RequestLog, pdf_url: str):
    if req.source_group_id:
        send_group_document(
            req.source_group_id,
            pdf_url,
            filename=f"{req.curp}_{req.act_type}.pdf",
            caption="✅ Aquí está tu acta"
        )
    else:
        send_document(
            req.requester_wa_id,
            pdf_url,
            filename=f"{req.curp}_{req.act_type}.pdf",
            caption="✅ Aquí está tu acta"
        )


def _provider_no_record_patterns():
    raw = settings.PROVIDER_NO_RECORD_TEXT or ""
    return [normalize_text(x) for x in raw.split("|") if x.strip()]


def _is_no_record_message(text_upper: str) -> bool:
    patterns = _provider_no_record_patterns()
    return any(p in text_upper for p in patterns)


def _extract_provider_curp_loose(text_body: str) -> str | None:
    """
    Intenta sacar una CURP del texto del proveedor.
    Primero usa la validación normal.
    Si no encuentra, busca cualquier bloque de 18 caracteres alfanuméricos.
    """
    curps = extract_curps(text_body)
    if curps:
        return curps[0]

    text_upper = normalize_text(text_body or "")
    m = re.search(r"\b([A-Z0-9]{18})\b", text_upper)
    if m:
        return m.group(1)

    return None


def _find_open_request_for_provider(db: Session, provider_wa: str, text_body: str):
    """
    SOLO busca por CURP si el proveedor la manda.
    Si no encuentra CURP clara, no adivina por antigüedad.
    """
    provider_curp = _extract_provider_curp_loose(text_body)

    if not provider_curp:
        return None

    return (
        db.query(RequestLog)
        .filter(
            RequestLog.provider_whatsapp == provider_wa,
            RequestLog.curp == provider_curp,
            RequestLog.status == "PROCESSING"
        )
        .order_by(RequestLog.created_at.asc())
        .first()
    )


def _extract_curp_from_filename(filename: str) -> str | None:
    if not filename:
        return None

    name = normalize_text(filename)
    m = re.search(r"\b([A-Z0-9]{18})\b", name)
    if m:
        return m.group(1)

    return None


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

        is_group = remote_jid.endswith("@g.us")
        source_chat_id = remote_jid
        source_group_id = remote_jid if is_group else None
        requester_wa_id = participant.replace("@s.whatsapp.net", "") if is_group and participant else remote_jid.replace("@s.whatsapp.net", "")

        text_body = ""
        if "conversation" in message:
            text_body = message.get("conversation", "")
        elif "extendedTextMessage" in message:
            text_body = message.get("extendedTextMessage", {}).get("text", "")

        text_upper = normalize_text(text_body)

        provider_wa = (settings.PROVIDER_WHATSAPP or "").strip()
        is_provider_message = requester_wa_id == provider_wa

        # =========================
        # RESPUESTA DEL PROVEEDOR
        # =========================
        if is_provider_message:
            provider_curp = _extract_provider_curp_loose(text_body or "")
            print("PROVIDER_TEXT =", text_body, flush=True)
            print("PROVIDER_CURP_DETECTED =", provider_curp, flush=True)

            # =========================
            # 1) INTENTAR DETECTAR PDF
            # =========================
            doc = None

            # documento directo
            if "documentMessage" in message:
                doc = message.get("documentMessage")

            # documento con caption
            elif "documentWithCaptionMessage" in message:
                doc_wrap = message.get("documentWithCaptionMessage", {})
                doc = doc_wrap.get("message", {}).get("documentMessage")

            # documento reenviado/citado
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
                filename_curp = _extract_curp_from_filename(filename)

                print("PROVIDER_DOC_FILENAME =", filename, flush=True)
                print("PROVIDER_DOC_FILENAME_CURP =", filename_curp, flush=True)
                print("PROVIDER_DOC_URL =", pdf_url, flush=True)

                open_req = None

                # si el nombre del archivo trae CURP, usar esa CURP
                if filename_curp:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_whatsapp == provider_wa,
                            RequestLog.curp == filename_curp,
                            RequestLog.status == "PROCESSING"
                        )
                        .order_by(RequestLog.created_at.asc())
                        .first()
                    )

                # si no trae CURP en filename, intentar por texto
                if not open_req and provider_curp:
                    open_req = (
                        db.query(RequestLog)
                        .filter(
                            RequestLog.provider_whatsapp == provider_wa,
                            RequestLog.curp == provider_curp,
                            RequestLog.status == "PROCESSING"
                        )
                        .order_by(RequestLog.created_at.asc())
                        .first()
                    )

                if not open_req:
                    print("PROVIDER_PDF_WITHOUT_MATCH =", filename, flush=True)
                    return {"ok": True, "ignored": "provider_pdf_without_match"}

                open_req.pdf_url = pdf_url
                open_req.provider_media_url = pdf_url
                open_req.status = "DONE"
                open_req.updated_at = datetime.utcnow()
                db.commit()

                if pdf_url:
                    print("PROVIDER_PDF_MATCHED_REQ_ID =", open_req.id, flush=True)
                    print("PROVIDER_PDF_MATCHED_CURP =", open_req.curp, flush=True)

                    _deliver_pdf_result(open_req, pdf_url)
                    return {"ok": True, "provider_result": "pdf_delivered"}

                return {"ok": True, "ignored": "provider_pdf_without_url"}

            # =========================
            # 2) SI NO HAY PDF, INTENTAR TEXTO
            # =========================
            open_req = None
            if provider_curp:
                open_req = (
                    db.query(RequestLog)
                    .filter(
                        RequestLog.provider_whatsapp == provider_wa,
                        RequestLog.curp == provider_curp,
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
                    f"❌ No se encontró el acta en sistema.\nCURP: {open_req.curp}\nTipo: {open_req.act_type}\nFolio: {open_req.id}"
                )
                return {"ok": True, "provider_result": "no_record"}

            print("PROVIDER_UNHANDLED_MESSAGE =", message, flush=True)
            return {"ok": True, "ignored": "provider_unhandled_message"}

            
        # =========================
        # COMANDOS ADMIN
        # =========================
        if text_upper.startswith("/ADDUSER "):
            wa = text_upper.replace("/ADDUSER", "").strip()
            if wa and not db.query(AuthorizedUser).filter_by(wa_id=wa).first():
                db.add(AuthorizedUser(wa_id=wa))
                db.commit()
            send_text(requester_wa_id, f"✅ Usuario autorizado: {wa}")
            return {"ok": True}

        if text_upper.startswith("/RMUSER "):
            wa = text_upper.replace("/RMUSER", "").strip()
            row = db.query(AuthorizedUser).filter_by(wa_id=wa).first()
            if row:
                db.delete(row)
                db.commit()
                send_text(requester_wa_id, f"✅ Usuario eliminado: {wa}")
            else:
                send_text(requester_wa_id, f"⚠️ Usuario no encontrado: {wa}")
            return {"ok": True}

        if text_upper.startswith("/ADDGROUP"):
            if is_group:
                if not db.query(AuthorizedGroup).filter_by(group_jid=source_group_id).first():
                    db.add(AuthorizedGroup(group_jid=source_group_id, group_name=""))
                    db.commit()
                send_text(requester_wa_id, f"✅ Grupo autorizado: {source_group_id}")
            else:
                send_text(requester_wa_id, "⚠️ /ADDGROUP solo se usa dentro del grupo.")
            return {"ok": True}

        if text_upper.startswith("/STATUS"):
            total = db.query(RequestLog).count()
            pending = db.query(RequestLog).filter(RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])).count()
            done = db.query(RequestLog).filter(RequestLog.status == "DONE").count()
            errors = db.query(RequestLog).filter(RequestLog.status == "ERROR").count()
            send_text(requester_wa_id, f"📊 Total: {total}\n⏳ Pendientes: {pending}\n✅ Entregadas: {done}\n❌ Error/Sin registro: {errors}")
            return {"ok": True}

        if text_upper.startswith("/PENDING"):
            rows = db.query(RequestLog).filter(RequestLog.status.in_(["QUEUED", "PROCESSING", "PENDING"])).order_by(RequestLog.created_at.desc()).limit(15).all()
            if not rows:
                send_text(requester_wa_id, "✅ No hay pendientes.")
            else:
                body = "\n".join([f"{r.id} | {r.curp} | {r.act_type} | {r.status}" for r in rows])
                send_text(requester_wa_id, f"⏳ Pendientes:\n{body}")
            return {"ok": True}

        if text_upper.startswith("/LAST "):
            curp = text_upper.replace("/LAST", "").strip()
            last = (
                db.query(RequestLog)
                .filter(RequestLog.curp == curp, RequestLog.status == "DONE")
                .order_by(RequestLog.created_at.desc())
                .first()
            )
            if last and last.pdf_url:
                send_document(requester_wa_id, last.pdf_url, filename=f"{last.curp}_{last.act_type}.pdf", caption="♻️ Reenviado desde historial")
            else:
                send_text(requester_wa_id, "⚠️ No encontré PDF reciente para esa CURP.")
            return {"ok": True}

        if text_upper.startswith("/REQUEUE "):
            curp = text_upper.replace("/REQUEUE", "").strip()
            last = (
                db.query(RequestLog)
                .filter(RequestLog.curp == curp)
                .order_by(RequestLog.created_at.desc())
                .first()
            )
            if not last:
                send_text(requester_wa_id, "⚠️ No encontré solicitud previa para esa CURP.")
            else:
                last.status = "QUEUED"
                last.updated_at = datetime.utcnow()
                db.commit()
                request_queue.enqueue(process_request, last.id)
                send_text(requester_wa_id, f"🔁 Reintentando folio {last.id}")
            return {"ok": True}

        # =========================
        # FLUJO NORMAL DE USUARIO
        # =========================
        if is_group and not is_authorized_group(db, source_group_id):
            return {"ok": True, "ignored": "group_not_authorized"}

        if not is_authorized_user(db, requester_wa_id):
            send_text(requester_wa_id, "") #⛔ Tu número no está autorizado.
            return {"ok": True}

        if not text_body:
            return {"ok": True, "ignored": "no_text"}

        curps = extract_curps(text_body)
        if not curps:
            return {"ok": True, "ignored": "no_curp"}

        act_type = detect_act_type(text_body)

        for curp in curps:
            last_done = get_last_done_request(db, curp, act_type)
            if last_done and last_done.pdf_url and last_done.expires_at > datetime.utcnow():
                _deliver_pdf_result(last_done, last_done.pdf_url)
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
        print("WEBHOOK ERROR:", str(e), payload)
        return {"ok": False, "error": str(e)}
