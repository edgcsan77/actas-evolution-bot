from datetime import datetime
from app.db import SessionLocal
from app.models import RequestLog
from app.services.evolution import send_group_text, send_text
from app.config import settings


def _pick_provider_group(act_type: str, request_id: int) -> str:
    act_type = (act_type or "").upper().strip()

    nacimiento_groups = [
        settings.PROVIDER_GROUP_NACIMIENTO_1,
        settings.PROVIDER_GROUP_NACIMIENTO_2,
        settings.PROVIDER_GROUP_NACIMIENTO_3,
    ]
    nacimiento_groups = [g for g in nacimiento_groups if g]

    especiales_group = settings.PROVIDER_GROUP_ESPECIALES

    if act_type == "NACIMIENTO":
        if not nacimiento_groups:
            raise RuntimeError("NO_BIRTH_PROVIDER_GROUPS_CONFIGURED")
        idx = (request_id - 1) % len(nacimiento_groups)
        return nacimiento_groups[idx]

    if not especiales_group:
        raise RuntimeError("NO_SPECIAL_PROVIDER_GROUP_CONFIGURED")

    return especiales_group


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        req.status = "PROCESSING"
        req.updated_at = datetime.utcnow()

        text_to_provider = req.curp
        if req.act_type and req.act_type != "NACIMIENTO":
            text_to_provider = f"{req.curp} {req.act_type}"

        req.provider_message = text_to_provider
        req.provider_group_id = _pick_provider_group(req.act_type, req.id)
        db.commit()

        print("WORKER_PROVIDER_GROUP_ID =", req.provider_group_id, flush=True)
        print("WORKER_TEXT_TO_PROVIDER =", text_to_provider, flush=True)

        send_group_text(req.provider_group_id, text_to_provider)

        ack = (
            f"✅ Solicitud enviada al proveedor\n"
            f"CURP: {req.curp}\n"
            f"Tipo: {req.act_type}\n"
            f"Folio: {req.id}"
        )

        if req.source_group_id:
            send_group_text(req.source_group_id, ack)
        else:
            send_text(req.requester_wa_id, ack)

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
