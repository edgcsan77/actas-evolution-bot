from datetime import datetime
from app.db import SessionLocal
from app.models import RequestLog
from app.services.provider import request_acta
from app.services.evolution import send_text


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        req.status = "PROCESSING"
        req.updated_at = datetime.utcnow()
        db.commit()

        result = request_acta(req.curp, req.act_type, req.id)

        req.provider_ref = result.get("provider_ref")
        req.updated_at = datetime.utcnow()
        db.commit()

        send_text(
            req.requester_wa_id,
            f"✅ Solicitud recibida\nCURP: {req.curp}\nTipo: {req.act_type}\nFolio: {req.id}"
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