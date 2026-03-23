from datetime import datetime
from app.db import SessionLocal
from app.models import RequestLog


def cleanup_expired():
    db = SessionLocal()
    try:
        db.query(RequestLog).filter(RequestLog.expires_at < datetime.utcnow()).delete()
        db.commit()
        print("cleanup ok")
    finally:
        db.close()


if __name__ == "__main__":
    cleanup_expired()