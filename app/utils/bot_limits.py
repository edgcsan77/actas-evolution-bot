from sqlalchemy.orm import Session
from app.models import RequestLog, AppSetting
from app.services.evolution import block_instance


def _bot_limit_key(instance_name: str) -> str:
    return f"bot_limit:{instance_name}"


def _bot_used_key(instance_name: str) -> str:
    return f"bot_used:{instance_name}"


def _app_setting_set(db: Session, key: str, value: str):
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(AppSetting(key=key, value=value))
    db.commit()


def _app_setting_get(db: Session, key: str, default: str = "0") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    return row.value if row and row.value is not None else default


def get_bot_limit(db: Session, instance_name: str) -> int:
    try:
        return int(_app_setting_get(db, _bot_limit_key(instance_name), "0"))
    except Exception:
        return 0


def get_bot_used(db: Session, instance_name: str) -> int:
    try:
        return (
            db.query(RequestLog)
            .filter(
                RequestLog.instance_name == instance_name,
                RequestLog.status == "DONE",
            )
            .count()
        )
    except Exception:
        return 0


def set_bot_limit(db: Session, instance_name: str, limit_value: int):
    _app_setting_set(db, _bot_limit_key(instance_name), str(max(0, int(limit_value))))


def set_bot_used(db: Session, instance_name: str, used_value: int):
    _app_setting_set(db, _bot_used_key(instance_name), str(max(0, int(used_value))))


def increment_bot_used_and_maybe_block(db: Session, instance_name: str) -> tuple[int, int, bool]:
    used = get_bot_used(db, instance_name) + 1
    limit_value = get_bot_limit(db, instance_name)

    set_bot_used(db, instance_name, used)

    blocked_now = False
    if limit_value > 0 and used >= limit_value:
        block_instance(instance_name)
        blocked_now = True

    return used, limit_value, blocked_now
