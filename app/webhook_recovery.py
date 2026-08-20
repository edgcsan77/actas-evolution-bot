import os
import time
import requests

from app.config import settings
from app.db import SessionLocal
from app.models import AuthorizedGroup, BotControl
from app.queue import redis_conn


RECOVERY_INTERVAL_SEC = int(
    os.getenv("WEBHOOK_RECOVERY_INTERVAL_SEC", "10")
)

RECOVERY_LOOKBACK_SEC = int(
    os.getenv("WEBHOOK_RECOVERY_LOOKBACK_SEC", "180")
)

RECOVERY_MESSAGE_LIMIT = int(
    os.getenv("WEBHOOK_RECOVERY_MESSAGE_LIMIT", "12")
)

LOCAL_WEBHOOK_URL = os.getenv(
    "WEBHOOK_RECOVERY_LOCAL_URL",
    "http://127.0.0.1:8000/webhook/evolution",
)

LOCK_NAME = "actas:webhook_recovery:lock"

session = requests.Session()


def _headers():
    return {
        "apikey": settings.EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


def _authorized_groups_by_instance():
    db = SessionLocal()

    try:
        blocked_instances = {
            (
                value.decode("utf-8", errors="ignore")
                if isinstance(value, bytes)
                else str(value)
            ).strip()
            for value in redis_conn.smembers(
                "blocked_instances_no_response"
            )
        }

        blocked_instances.update({
            (
                value.decode("utf-8", errors="ignore")
                if isinstance(value, bytes)
                else str(value)
            ).strip()
            for value in redis_conn.smembers(
                "admin_blocked_instances_no_minipanel_unlock"
            )
        })

        active_instances = {
            (row.instance_name or "").strip()
            for row in (
                db.query(BotControl)
                .filter(BotControl.is_active == True)
                .all()
            )
            if (
                (row.instance_name or "").strip()
                and
                (row.instance_name or "").strip()
                not in blocked_instances
            )
        }

        rows = (
            db.query(
                AuthorizedGroup.owner_instance,
                AuthorizedGroup.group_jid,
            )
            .filter(
                AuthorizedGroup.owner_instance.isnot(None),
                AuthorizedGroup.group_jid.isnot(None),
            )
            .all()
        )

        out = {}

        for owner_instance, group_jid in rows:
            instance = (owner_instance or "").strip()
            gid = (group_jid or "").strip()

            if not instance:
                continue

            if instance not in active_instances:
                continue

            if not gid.endswith("@g.us"):
                continue

            out.setdefault(instance, set()).add(gid)

        return out

    finally:
        db.close()


def _find_chats(instance_name):
    url = (
        f"{settings.EVOLUTION_BASE_URL.rstrip('/')}"
        f"/chat/findChats/{instance_name}"
    )

    r = session.post(
        url,
        headers=_headers(),
        json={},
        timeout=(4, 15),
    )

    r.raise_for_status()

    data = r.json()

    return data if isinstance(data, list) else []


def _find_messages(instance_name, remote_jid):
    url = (
        f"{settings.EVOLUTION_BASE_URL.rstrip('/')}"
        f"/chat/findMessages/{instance_name}"
    )

    r = session.post(
        url,
        headers=_headers(),
        json={
            "where": {
                "key": {
                    "remoteJid": remote_jid,
                }
            },
            "page": 1,
            "offset": RECOVERY_MESSAGE_LIMIT,
        },
        timeout=(4, 15),
    )

    r.raise_for_status()

    data = r.json()

    if not isinstance(data, dict):
        return []

    messages = data.get("messages")

    if not isinstance(messages, dict):
        return []

    records = messages.get("records")

    return records if isinstance(records, list) else []


def _msg_seen(msg_id):
    if not msg_id:
        return True

    return bool(
        redis_conn.exists(
            f"wa:webhook:msg:{msg_id}"
        )
    )


def _recent(timestamp):
    try:
        ts = int(timestamp or 0)
    except Exception:
        return False

    if ts <= 0:
        return False

    age = time.time() - ts

    return -30 <= age <= RECOVERY_LOOKBACK_SEC


def _candidate(record, allowed_groups):
    if not isinstance(record, dict):
        return False

    key = record.get("key") or {}

    if not isinstance(key, dict):
        return False

    msg_id = (key.get("id") or "").strip()
    remote_jid = (key.get("remoteJid") or "").strip()

    if not msg_id:
        return False

    if key.get("fromMe") is True:
        return False

    if remote_jid not in allowed_groups:
        return False

    if not _recent(record.get("messageTimestamp")):
        return False

    message = record.get("message")

    if not isinstance(message, dict):
        return False

    # Recovery solo para mensajes con texto procesable.
    has_text = bool(
        (message.get("conversation") or "").strip()
        or
        (
            (
                message.get("extendedTextMessage")
                or {}
            ).get("text")
            or ""
        ).strip()
    )

    if not has_text:
        return False

    if _msg_seen(msg_id):
        return False

    return True


def _inject(instance_name, record):
    msg_id = (
        (record.get("key") or {}).get("id")
        or ""
    )

    remote_jid = (
        (record.get("key") or {}).get("remoteJid")
        or ""
    )

    payload = {
        "event": "messages.upsert",
        "instance": instance_name,
        "data": record,
    }

    r = session.post(
        LOCAL_WEBHOOK_URL,
        json=payload,
        timeout=(3, 20),
    )

    r.raise_for_status()

    try:
        body = r.json()
    except Exception:
        body = {}

    ignored = body.get("ignored")

    processed = (
        body.get("ok") is True
        and not ignored
    )

    print(
        "WEBHOOK_RECOVERY_INJECT =",
        {
            "instance": instance_name,
            "msg_id": msg_id,
            "remote_jid": remote_jid,
            "status": r.status_code,
            "processed": processed,
            "ignored": ignored,
        },
        flush=True,
    )

    return processed


def recovery_cycle():
    recovered = 0
    inspected_chats = 0
    candidate_messages = 0

    authorized = _authorized_groups_by_instance()

    for instance_name, allowed_groups in sorted(
        authorized.items()
    ):
        if not allowed_groups:
            continue

        try:
            chats = _find_chats(instance_name)

        except Exception as exc:
            print(
                "WEBHOOK_RECOVERY_FIND_CHATS_ERROR =",
                {
                    "instance": instance_name,
                    "error": repr(exc),
                },
                flush=True,
            )
            continue

        for chat in chats:
            if not isinstance(chat, dict):
                continue

            remote_jid = (
                chat.get("remoteJid")
                or ""
            ).strip()

            # CRÍTICO:
            # jamás revisar un chat que no sea grupo
            # autorizado para ESA instancia.
            if remote_jid not in allowed_groups:
                continue

            last_message = chat.get("lastMessage")

            if not isinstance(last_message, dict):
                continue

            if not _recent(
                last_message.get("messageTimestamp")
            ):
                continue

            inspected_chats += 1

            try:
                records = _find_messages(
                    instance_name,
                    remote_jid,
                )

            except Exception as exc:
                print(
                    "WEBHOOK_RECOVERY_FIND_MESSAGES_ERROR =",
                    {
                        "instance": instance_name,
                        "remote_jid": remote_jid,
                        "error": repr(exc),
                    },
                    flush=True,
                )
                continue

            records = sorted(
                records,
                key=lambda x: int(
                    (x or {}).get("messageTimestamp")
                    or 0
                ),
            )

            for record in records:
                if not _candidate(
                    record,
                    allowed_groups,
                ):
                    continue

                candidate_messages += 1

                try:
                    if _inject(
                        instance_name,
                        record,
                    ):
                        recovered += 1

                except Exception as exc:
                    print(
                        "WEBHOOK_RECOVERY_INJECT_ERROR =",
                        {
                            "instance": instance_name,
                            "msg_id": (
                                (record.get("key") or {})
                                .get("id")
                            ),
                            "error": repr(exc),
                        },
                        flush=True,
                    )

    print(
        "WEBHOOK_RECOVERY_CYCLE =",
        {
            "instances": len(authorized),
            "recent_authorized_chats": inspected_chats,
            "candidates": candidate_messages,
            "recovered": recovered,
        },
        flush=True,
    )


def main():
    print(
        "WEBHOOK_RECOVERY_STARTED =",
        {
            "interval_sec": RECOVERY_INTERVAL_SEC,
            "lookback_sec": RECOVERY_LOOKBACK_SEC,
            "message_limit": RECOVERY_MESSAGE_LIMIT,
        },
        flush=True,
    )

    while True:
        lock = redis_conn.lock(
            LOCK_NAME,
            timeout=max(
                60,
                RECOVERY_INTERVAL_SEC * 4,
            ),
            blocking=False,
        )

        acquired = False

        try:
            acquired = lock.acquire(
                blocking=False
            )

            if acquired:
                recovery_cycle()

        except Exception as exc:
            print(
                "WEBHOOK_RECOVERY_CYCLE_ERROR =",
                repr(exc),
                flush=True,
            )

        finally:
            if acquired:
                try:
                    lock.release()
                except Exception:
                    pass

        time.sleep(RECOVERY_INTERVAL_SEC)


if __name__ == "__main__":
    main()
