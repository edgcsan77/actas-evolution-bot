from sqlalchemy import text
from app.db import SessionLocal
from app.services.evolution import send_group_text
from app.client_messages import processing_error_message, resolve_bot_name


FAIL_PATTERNS = [
    "HTTPSCONNECTIONPOOL",
    "TRAMITESFULL",
    "READ TIMED OUT",
    "TIMEOUT_WAITING_PDF",
    "NEW_API_CHAIN_NOT_SUPPORTED_YET",
]


def _is_web_provider(provider_name: str | None) -> bool:
    return (provider_name or "").upper() in {
        "PROVIDER4",
        "PROVIDER10",
        "PROVIDER11",
    }


def _is_fail_error(error_message: str | None) -> bool:
    raw = (error_message or "").upper()
    if "CLIENT_NOTIFIED_FAIL" in raw:
        return False
    return any(p in raw for p in FAIL_PATTERNS)


def _client_message(row) -> str:
    requester = (
        str(row.get("requester_name") or "").strip()
        or str(row.get("source_group_id") or "").strip()
        or str(row.get("source_chat_id") or "").strip()
        or "Usuario"
    )

    return processing_error_message(
        requester=requester,
        detail="Intenta nuevamente en unos minutos.",
        act_type=row.get("act_type"),
        bot_name=resolve_bot_name(
            row.get("instance_name"),
        ),
        dato=row.get("curp"),
    )


def main():
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT
                id,
                curp,
                act_type,
                provider_name,
                status,
                error_message,
                source_group_id,
                source_chat_id,
                instance_name,
                requester_name,
                updated_at
            FROM request_logs
            WHERE status = 'ERROR'
              AND provider_name IN ('PROVIDER4','PROVIDER10','PROVIDER11')
              AND updated_at >= NOW() - INTERVAL '20 minutes'
              AND COALESCE(error_message,'') NOT ILIKE '%CLIENT_NOTIFIED_FAIL%'
              AND (
                    error_message ILIKE '%HTTPSCONNECTIONPOOL%'
                 OR error_message ILIKE '%TRAMITESFULL%'
                 OR error_message ILIKE '%READ TIMED OUT%'
                 OR error_message ILIKE '%TIMEOUT_WAITING_PDF%'
                 OR error_message ILIKE '%NEW_API_CHAIN_NOT_SUPPORTED_YET%'
              )
            ORDER BY updated_at ASC
            LIMIT 10;
        """)).mappings().all()

        sent = 0
        skipped = 0
        failed = 0

        for row in rows:
            group_jid = row["source_group_id"] or row["source_chat_id"]
            instance = row["instance_name"]

            if not group_jid:
                skipped += 1
                print(f"CLIENT_FAIL_NOTIFY_SKIP_NO_GROUP request_id={row['id']}", flush=True)
                continue

            msg = _client_message(row)

            try:
                print(
                    f"CLIENT_FAIL_NOTIFY_SEND request_id={row['id']} group={group_jid} instance={instance}",
                    flush=True,
                )

                send_group_text(
                    group_jid=group_jid,
                    text=msg,
                    instance_name=instance,
                )

                db.execute(text("""
                    UPDATE request_logs
                    SET
                        error_message = CONCAT(COALESCE(error_message,''), ' | CLIENT_NOTIFIED_FAIL'),
                        updated_at = NOW()
                    WHERE id = :id
                      AND COALESCE(error_message,'') NOT ILIKE '%CLIENT_NOTIFIED_FAIL%';
                """), {"id": row["id"]})
                db.commit()

                sent += 1

            except Exception as e:
                db.rollback()
                failed += 1
                print(
                    f"CLIENT_FAIL_NOTIFY_ERROR request_id={row['id']} error={e}",
                    flush=True,
                )

        print(
            f"CLIENT_FAIL_NOTIFY_DONE sent={sent} skipped={skipped} failed={failed}",
            flush=True,
        )

    finally:
        db.close()


if __name__ == "__main__":
    main()
