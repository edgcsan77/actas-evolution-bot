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

        # 2) marcar WHATSAPP vencidos a los 8 minutos y WEB vencidos a los 11 minutos.
        whatsapp_timeout_minutes = int(
            getattr(settings, "REQUEST_TIMEOUT_MINUTES", 8) or 8
        )
        
        web_timeout_minutes = int(
            getattr(settings, "WEB_REQUEST_TIMEOUT_MINUTES", 11) or 11
        )
        
        # API externa: no debe retener saldo por horas.
        # 45 minutos permite que los proveedores lentos terminen,
        # pero evita reservas eternas.
        api_timeout_minutes = int(
            getattr(settings, "API_REQUEST_TIMEOUT_MINUTES", 45) or 45
        )
        
        whatsapp_limit = now - timedelta(minutes=whatsapp_timeout_minutes)
        web_limit = now - timedelta(minutes=web_timeout_minutes)
        api_limit = now - timedelta(minutes=api_timeout_minutes)

        whatsapp_providers = [
            "PROVIDER1",
            "PROVIDER2",
            "PROVIDER5",
            "PROVIDER6",
            "PROVIDER8",
            "PROVIDER9",
            "PROVIDER12",
            "MAYAPROVIDER",
        ]

        web_providers = [
            "PROVIDER3",
            "PROVIDER4",
            "PROVIDER10",
            "PROVIDER11",
        ]

        rows_whatsapp = (
            db.query(RequestLog)
            .filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.api_client_id.is_(None),
                RequestLog.created_at.is_not(None),
                RequestLog.created_at <= whatsapp_limit,
                RequestLog.provider_name.in_(whatsapp_providers),
            )
            .all()
        )

        rows_web = (
            db.query(RequestLog)
            .filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.api_client_id.is_(None),
                RequestLog.created_at.is_not(None),
                RequestLog.created_at <= web_limit,
                RequestLog.provider_name.in_(web_providers),
            )
            .all()
        )

        rows_api = (
            db.query(RequestLog)
            .filter(
                RequestLog.status.in_(["QUEUED", "PROCESSING"]),
                RequestLog.api_client_id.isnot(None),
                RequestLog.created_at.is_not(None),
                RequestLog.created_at <= api_limit,
                RequestLog.api_charged == False,
            )
            .all()
        )

        print("CLEANUP_WHATSAPP_TIMEOUT_LIMIT =", whatsapp_limit, flush=True)
        print("CLEANUP_WEB_TIMEOUT_LIMIT =", web_limit, flush=True)
        print("CLEANUP_API_TIMEOUT_LIMIT =", api_limit, flush=True)
        
        print("CLEANUP_WHATSAPP_TIMEOUT_ROWS =", len(rows_whatsapp), flush=True)
        print("CLEANUP_WEB_TIMEOUT_ROWS =", len(rows_web), flush=True)
        print("CLEANUP_API_TIMEOUT_ROWS =", len(rows_api), flush=True)
        
        rows = rows_whatsapp + rows_web + rows_api
        changed_ids = []

        for r in rows:
            provider = (r.provider_name or "").upper()
            is_api = r.api_client_id is not None
        
            if is_api:
                label = (
                    "API_STALE_TIMEOUT:"
                    f"sin resultado después de {api_timeout_minutes} minutos"
                )
        
            elif provider in web_providers:
                label = f"Auto-cierre (>{web_timeout_minutes} min) WEB cleanup"
        
            else:
                label = (
                    f"Auto-cierre (>{whatsapp_timeout_minutes} min) "
                    "WHATSAPP cleanup"
                )
        
            r.status = "ERROR"
            r.updated_at = now
            r.error_message = label
            changed_ids.append(r.id)

        db.commit()
        print("CLEANUP_MARKED_ERROR_IDS =", changed_ids, flush=True)

        # 3) avisar por WhatsApp después del commit
        for r in rows:
            try:
                # Las solicitudes API reciben resultado por endpoint/API,
                # no se debe enviar aviso por WhatsApp.
                if r.api_client_id is not None:
                    print(
                        "CLEANUP_API_TIMEOUT_NO_WHATSAPP =",
                        r.id,
                        flush=True,
                    )
                    continue
                
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {r.curp}\n"
                    f"Tipo: {r.act_type}\n\n"
                    f"Reenviar nuevamente en unos minutos"
                )

                instance = r.instance_name or settings.EVOLUTION_INSTANCE

                if r.source_group_id:
                    if should_notify_failure(r.source_group_id):
                        send_group_text(
                            r.source_group_id,
                            msg,
                            instance_name=instance,
                        )
                else:
                    send_text(
                        r.requester_wa_id,
                        msg,
                        instance_name=instance,
                    )

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
