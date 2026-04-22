from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import SQLAlchemyError

from app.db import SessionLocal
from app.models import RequestLog
from app.config import settings
from app.services.evolution import send_group_text, send_text

NO_FAIL_NOTIFY_GROUPS = {
    "120363427267191472@g.us"
}


def should_notify_failure(group_id: str | None) -> bool:
    if not group_id:
        return True
    return group_id not in NO_FAIL_NOTIFY_GROUPS
    

def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def cleanup_expired_and_mark_pending():
    db = SessionLocal()
    try:
        now = _utc_now_naive()

        print("CLEANUP_NOW_UTC =", now, flush=True)

        # 1) borrar historial vencido
        deleted_count = (
            db.query(RequestLog)
            .filter(
                RequestLog.expires_at.is_not(None),
                RequestLog.expires_at < now
            )
            .delete(synchronize_session=False)
        )

        print("CLEANUP_DELETED_EXPIRED =", deleted_count, flush=True)

        # 2) marcar PROCESSING vencidos como ERROR
        limit = now - timedelta(minutes=settings.REQUEST_TIMEOUT_MINUTES)

        rows = (
            db.query(RequestLog)
            .filter(
                RequestLog.status == "PROCESSING",
                RequestLog.updated_at.is_not(None),
                RequestLog.updated_at <= limit
            )
            .all()
        )

        print("CLEANUP_TIMEOUT_LIMIT =", limit, flush=True)
        print("CLEANUP_TIMEOUT_ROWS =", len(rows), flush=True)

        changed_ids = []

        for r in rows:
            r.status = "ERROR"
            r.updated_at = now
            r.error_message = (
                f"Timeout automático a los "
                f"{settings.REQUEST_TIMEOUT_MINUTES} minutos"
            )
            changed_ids.append(r.id)

        db.commit()
        print("CLEANUP_MARKED_ERROR_IDS =", changed_ids, flush=True)

        # 3) avisar por WhatsApp después del commit
        for r in rows:
            try:
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {r.curp}\n"
                    f"Tipo: {r.act_type}\n\n"
                    f"Reenviar nuevamente en unos minutos"
                )

                if r.source_group_id:
                    if should_notify_failure(r.source_group_id):
                        send_group_text(r.source_group_id, msg)
                else:
                    send_text(r.requester_wa_id, msg)

                print("CLEANUP_SENT_TIMEOUT_MSG =", r.id, flush=True)

            except Exception as e:
                print(
                    f"CLEANUP_SEND_ERROR id={r.id} error={str(e)}",
                    flush=True
                )

        print("CLEANUP_OK", flush=True)

    except SQLAlchemyError as e:
        try:
            db.rollback()
        except Exception:
            pass
        print(f"CLEANUP_DB_ERROR = {repr(e)}", flush=True)

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        print(f"CLEANUP_GENERAL_ERROR = {repr(e)}", flush=True)

    finally:
        db.close()


if __name__ == "__main__":
    cleanup_expired_and_mark_pending()
