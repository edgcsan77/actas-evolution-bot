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

        # timeout solo para solicitudes ya enviadas al proveedor
        limit = datetime.utcnow() - timedelta(minutes=settings.REQUEST_TIMEOUT_MINUTES)

        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.status == "PROCESSING",
                RequestLog.updated_at <= limit
            )
            .all()
        )

        print("CLEANUP_TIMEOUT_LIMIT =", limit, flush=True)
        print("CLEANUP_TIMEOUT_ROWS =", len(rows), flush=True)

        for r in rows:
            r.status = "ERROR"
            r.updated_at = datetime.utcnow()
            r.error_message = f"Timeout automático a los {settings.REQUEST_TIMEOUT_MINUTES} minutos"

            try:
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {r.curp}\n"
                    f"Tipo: {r.act_type}\n\n"
                    f"Reenviar nuevamente"
                )

                if r.source_group_id:
                    send_group_text(r.source_group_id, msg)
                else:
                    send_text(r.requester_wa_id, msg)

                print("CLEANUP_SENT_TIMEOUT_MSG =", r.id, flush=True)

            except Exception as e:
                print("CLEANUP_SEND_ERROR =", str(e), flush=True)

        db.commit()
        print("cleanup ok", flush=True)

    finally:
        db.close()


if __name__ == "__main__":
    cleanup_expired_and_mark_pending()
