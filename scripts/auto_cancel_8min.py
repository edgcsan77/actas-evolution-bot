import redis
from rq import Queue
from rq.job import Job
from rq.registry import ScheduledJobRegistry, StartedJobRegistry
from app.db import SessionLocal
from app.models import RequestLog

TIMEOUT_ERROR = "TIMEOUT_8MIN_CANCELLED: solicitud cancelada automáticamente por superar 8 minutos sin PDF útil"

r = redis.Redis(host="127.0.0.1", port=6379, db=0)
db = SessionLocal()

try:
    rows = (
        db.query(RequestLog)
        .filter(RequestLog.status.in_(["QUEUED", "PROCESSING"]))
        .filter(RequestLog.created_at < __import__("datetime").datetime.utcnow() - __import__("datetime").timedelta(minutes=8))
        .all()
    )

    cancelled_ids = set()

    for req in rows:
        req.status = "ERROR"
        req.error_message = TIMEOUT_ERROR
        req.updated_at = __import__("datetime").datetime.utcnow()
        cancelled_ids.add(req.id)

    db.commit()

    deleted = 0

    def rid_from_job(job):
        for a in (job.args or []):
            if isinstance(a, int):
                return a
            if isinstance(a, str) and a.isdigit():
                return int(a)
        for v in (job.kwargs or {}).values():
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        return None

    for qname in ["actas", "actas_slow"]:
        q = Queue(qname, connection=r)

        for jid in list(q.job_ids):
            try:
                job = Job.fetch(jid, connection=r)
                rid = rid_from_job(job)
                if rid in cancelled_ids:
                    job.cancel()
                    job.delete()
                    deleted += 1
            except Exception:
                pass

        for Reg in [ScheduledJobRegistry, StartedJobRegistry]:
            reg = Reg(qname, connection=r)
            for jid in list(reg.get_job_ids()):
                try:
                    job = Job.fetch(jid, connection=r)
                    rid = rid_from_job(job)
                    if rid in cancelled_ids:
                        try:
                            reg.remove(jid, delete_job=True)
                        except TypeError:
                            reg.remove(jid)
                            job.delete()
                        deleted += 1
                except Exception:
                    pass

    # ACK no sirve si ya se atrasó; se limpia para que no sature.
    try:
        Queue("ack", connection=r).empty()
    except Exception:
        pass

    print(f"AUTO_CANCEL_8MIN cancelled_db={len(cancelled_ids)} deleted_redis={deleted}")

finally:
    db.close()
