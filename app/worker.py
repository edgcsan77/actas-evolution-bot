from datetime import datetime
from app.db import SessionLocal
from app.models import RequestLog
from app.services.evolution import send_text
from app.config import settings


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        req.status = "PROCESSING"
        req.updated_at = datetime.utcnow()
        req.provider_whatsapp = settings.PROVIDER_WHATSAPP

        text_to_provider = req.curp
        if req.act_type and req.act_type != "NACIMIENTO":
            text_to_provider = f"{req.curp} {req.act_type}"

        req.provider_message = text_to_provider
        db.commit()

        if not settings.PROVIDER_WHATSAPP:
            raise RuntimeError("PROVIDER_WHATSAPP_EMPTY")

        send_text(settings.PROVIDER_WHATSAPP, text_to_provider)

        send_text(
            req.requester_wa_id,
            f"✅ Solicitud enviada al proveedor\nCURP: {req.curp}\nTipo: {req.act_type}\nFolio: {req.id}"
        )

    except Exception as e:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if req:
            req.status = "ERROR"
            req.error_message = str(e)
            req.updated_at = datetime.utcnow()
            db.commit()
        raise
    finally:
        db.close()
