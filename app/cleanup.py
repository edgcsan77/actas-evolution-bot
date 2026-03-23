from datetime import datetime, timedelta
from app.db import SessionLocal
from app.models import RequestLog
from app.config import settings
from app.services.evolution import send_group_text, send_text


def cleanup_expired_and_mark_pending():
    db = SessionLocal()
    try:
        # borrar historial vencido
        db.query(RequestLog).filter(RequestLog.expires_at < datetime.utcnow()).delete()

        # marcar timeout como PENDING
        limit = datetime.utcnow() - timedelta(minutes=settings.REQUEST_TIMEOUT_MINUTES)
        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.created_at <= limit
            )
            .all()
        )

        for r in rows:
            r.status = "PENDING"
            r.updated_at = datetime.utcnow()
            r.error_message = f"Timeout automático a los {settings.REQUEST_TIMEOUT_MINUTES} minutos"

            try:
                msg = f"⚠️ Solicitud demorada\nCURP: {r.curp}\nTipo: {r.act_type}\nFolio: {r.id}\nEstado: PENDIENTE"
                if r.source_group_id:
                    send_group_text(r.source_group_id, msg)
                else:
                    send_text(r.requester_wa_id, msg)

                if settings.ADMIN_PHONE:
                    send_text(settings.ADMIN_PHONE, f"⚠️ Timeout en folio {r.id}\nCURP: {r.curp}\nTipo: {r.act_type}")
            except Exception:
                pass

        db.commit()
        print("cleanup ok")
    finally:
        db.close()


if __name__ == "__main__":
    cleanup_expired_and_mark_pending()
