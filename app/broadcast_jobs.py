import time
import json

from app.evolution import send_group_text
from app.queue import redis_conn


def _broadcast_progress_key(job_id: str) -> str:
    return f"botpanel:broadcast:{job_id}"


def _save_progress(job_id: str, data: dict):
    redis_conn.setex(
        _broadcast_progress_key(job_id),
        3600,
        json.dumps(data),
    )


def botpanel_broadcast_job(
    job_id: str,
    instance_name: str,
    message: str,
    group_jids: list[str],
):
    sent = 0
    errors = 0
    skipped = 0
    total = len(group_jids)

    rate_limits = {
        "docifybot8": 1.5,
        "docifybot8xpress": 1.5,
        "docifybot9": 1.5,
        "docifybot10": 1.5,
    }

    delay = rate_limits.get(instance_name, 1.5)

    try:
        _save_progress(job_id, {
            "ok": True,
            "status": "running",
            "instance": instance_name,
            "total": total,
            "sent": sent,
            "errors": errors,
            "skipped": skipped,
            "current": "",
        })

        for group_jid in group_jids:
            if not group_jid or "@g.us" not in group_jid:
                skipped += 1
                continue

            _save_progress(job_id, {
                "ok": True,
                "status": "running",
                "instance": instance_name,
                "total": total,
                "sent": sent,
                "errors": errors,
                "skipped": skipped,
                "current": group_jid,
            })

            try:
                send_group_text(
                    group_jid,
                    message,
                    instance_name=instance_name,
                )
                sent += 1
            except Exception as e:
                errors += 1
                print(
                    "BOTPANEL_BROADCAST_JOB_ERROR =",
                    instance_name,
                    group_jid,
                    str(e),
                    flush=True,
                )

            _save_progress(job_id, {
                "ok": True,
                "status": "running",
                "instance": instance_name,
                "total": total,
                "sent": sent,
                "errors": errors,
                "skipped": skipped,
                "current": group_jid,
            })

            time.sleep(delay)

        _save_progress(job_id, {
            "ok": True,
            "status": "done",
            "instance": instance_name,
            "total": total,
            "sent": sent,
            "errors": errors,
            "skipped": skipped,
            "current": "",
        })

    except Exception as e:
        _save_progress(job_id, {
            "ok": False,
            "status": "error",
            "instance": instance_name,
            "error": str(e),
            "total": total,
            "sent": sent,
            "errors": errors,
            "skipped": skipped,
            "current": "",
        })
        raise
