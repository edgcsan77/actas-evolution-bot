from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.db import SessionLocal
from app.models import RequestLog
from app.config import settings
from app.services.evolution import send_group_text, send_text


def _mx_now():
    return datetime.now(ZoneInfo("America/Monterrey"))


def cleanup_expired_and_mark_pending():
    db = SessionLocal()
    try:
        now = _mx_now()

        # borrar historial vencido
        db.query(RequestLog).filter(RequestLog.expires_at < now).delete()

        # timeout solo para solicitudes ya enviadas al proveedor
        limit = now - timedelta(minutes=settings.REQUEST_TIMEOUT_MINUTES)

        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.status == "PROCESSING",
                RequestLog.updated_at <= limit
            )
            .all()
        )

        print("CLEANUP_NOW =", now, flush=True)
        print("CLEANUP_TIMEOUT_LIMIT =", limit, flush=True)
        print("CLEANUP_TIMEOUT_ROWS =", len(rows), flush=True)

        for r in rows:
            r.status = "ERROR"
            r.updated_at = _mx_now()
            r.error_message = f"Timeout automático a los {settings.REQUEST_TIMEOUT_MINUTES} minutos"

            try:
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {r.curp}\n"
                    f"Tipo: {r.act_type}\n\n"
                    f"Reenviar nuevamente en unos minutos"
                )

                if r.source_group_id:
                    send_group_text(r.source_group_id, msg)
                else:
                    send_text(r.requester_wa_id, msg)

                print("CLEANUP_SENT_TIMEOUT_MSG =", r.id, flush=True)

            except Exception as e:
                print("CLEANUP_SEND_ERROR =", str(e), flush=True)

        db.commit()
        print("CLEANUP_OK", flush=True)

    finally:
        db.close()


if __name__ == "__main__":
    cleanup_expired_and_mark_pending()
