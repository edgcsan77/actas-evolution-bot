from datetime import datetime, timedelta
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Base, engine, get_db
from app.models import AuthorizedUser, AuthorizedGroup, RequestLog, ProviderSetting
from app.queue import request_queue
from app.worker import process_request
from app.utils.curp import (
    extract_request_terms,
    detect_act_type,
    normalize_text,
    extract_identifier_loose,
    extract_identifier_from_filename,
    detect_identifier_problem,
)
from app.services.evolution import send_text, send_document, send_group_document, send_group_text

app = FastAPI(title=settings.APP_NAME)


def _normalize_wa_actor(value: str) -> str:
    value = (value or "").strip()
    value = value.replace("@s.whatsapp.net", "")
    value = value.replace("@lid", "")
    value = value.replace("@g.us", "")
    return value


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

    db = Session(bind=engine)
    try:
        _get_or_create_provider(db, "PROVIDER1", True)
        _get_or_create_provider(db, "PROVIDER2", False)
    finally:
        db.close()
        

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
            filename=f"{req.curp}.pdf",
            caption=""
        )
    else:
        send_document(
            req.requester_wa_id,
            pdf_url,
            filename=f"{req.curp}.pdf",
            caption=""
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
    admin = (settings.ADMIN_PHONE or "").replace("+", "").replace(" ", "").strip()
    return from_me or requester_wa_id == admin


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
        settings.PROVIDER2_GROUP,
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


def _providers_status_text(db: Session) -> str:
    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)

    s1 = "ON" if p1.is_enabled else "OFF"
    s2 = "ON" if p2.is_enabled else "OFF"

    return (
        "⚙️ Estado de proveedores\n"
        f"• PROVIDER1: {s1}\n"
        f"• PROVIDER2: {s2}"
    )


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
        
        is_group = remote_jid.endswith("@g.us")
        source_chat_id = remote_jid
        source_group_id = remote_jid if is_group else None
        requester_wa_id = _normalize_wa_actor(participant) if is_group and participant else _normalize_wa_actor(remote_jid)
        is_group and participant else _normalize_wa_actor(remote_jid)
        
        text_body = ""
        if "conversation" in message:
            text_body = message.get("conversation", "")
        elif "extendedTextMessage" in message:
            text_body = message.get("extendedTextMessage", {}).get("text", "")
        
        text_upper = normalize_text(text_body)
        
        admin_commands = (
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
                            RequestLog.status == "PROCESSING"
                        )
                        .order_by(RequestLog.created_at.asc())
                        .first()
                    )
                
                if not open_req and provider_id:
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
                    f"❌ No hay registros disponibles.\nDato: {open_req.curp}\nTipo: {open_req.act_type}"
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
        
        terms = extract_request_terms(text_body)
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

        for term in terms:
            print("PROCESSING_TERM =", term, flush=True)
            last_done = get_last_done_request(db, term, act_type)
            if last_done and last_done.pdf_url and last_done.expires_at > datetime.utcnow():
                _deliver_pdf_result(last_done, last_done.pdf_url)
                continue

            request_key = build_request_key(term, act_type, source_chat_id)

            duplicate_open = (
                db.query(RequestLog)
                .filter(
                    RequestLog.request_key == request_key,
                    RequestLog.status.in_(["QUEUED", "PROCESSING"])
                )
                .first()
            )
            if duplicate_open:
                dup_msg = f"⏳ Ya existe una solicitud en proceso\nDato: {term}\nTipo: {act_type}"
                if source_group_id:
                    send_group_text(source_group_id, dup_msg)
                else:
                    send_text(requester_wa_id, dup_msg)
                continue

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
            db.commit()
            db.refresh(row)

            request_queue.enqueue(process_request, row.id)
            print("ENQUEUED_REQUEST_ID =", row.id, flush=True)
            print("ENQUEUED_TERM =", row.curp, flush=True)
            print("ENQUEUED_TYPE =", row.act_type, flush=True)
            print("ENQUEUED_SOURCE_GROUP =", row.source_group_id, flush=True)

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
            
        return {"ok": True}

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), payload)
        return {"ok": False, "error": str(e)}
