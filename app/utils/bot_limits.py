from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.config import settings

from app.models import AppSetting, RequestLog
from app.queue import redis_conn

BLOCKED_INSTANCES_KEY = "blocked_instances_no_response"


def block_instance(instance_name: str):
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return
    redis_conn.sadd(BLOCKED_INSTANCES_KEY, instance_name)


def unblock_instance(instance_name: str):
    instance_name = (instance_name or "").strip()
    if not instance_name:
        return
    redis_conn.srem(BLOCKED_INSTANCES_KEY, instance_name)
    redis_conn.srem(BLOCKED_INSTANCES_KEY, instance_name.encode("utf-8"))


# =========================
# TIME
# =========================
def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# =========================
# APP SETTINGS (KV STORE)
# =========================
def _app_setting_get(db: Session, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    return (row.value or default) if row else default


def _app_setting_set(db: Session, key: str, value: str):
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if row:
        row.value = str(value)
        row.updated_at = _utc_now_naive()
    else:
        row = AppSetting(
            key=key,
            value=str(value),
            updated_at=_utc_now_naive(),
        )
        db.add(row)
    db.commit()


# =========================
# KEYS
# =========================
def _bot_limit_key(instance_name: str) -> str:
    return f"bot_limit:{instance_name}"


def _bot_used_key(instance_name: str) -> str:
    return f"bot_used:{instance_name}"


def _bot_used_offset_key(instance_name: str) -> str:
    return f"bot_used_offset:{instance_name}"


# =========================
# GETTERS
# =========================
def get_bot_limit(db: Session, instance_name: str) -> int:
    try:
        return int(_app_setting_get(db, _bot_limit_key(instance_name), "0") or "0")
    except Exception:
        return 0


# BOT_USED_DONE_SOURCE_OF_TRUTH_V1
# BOT_USED_ACCOUNTABLE_DONE_V2
#
# Debe reflejar las mismas exclusiones de contabilidad
# que usa el worker al terminar una solicitud.
BOT_USED_HIDDEN_NO_ACCOUNTING_GROUPS = {
    "docifybot8mx": {
        "120363407565721999@g.us",
        "120363408048979577@g.us",
        "120363424360403186@g.us",
    },
}


def _count_accountable_done(
    db: Session,
    instance_name: str,
) -> int:

    instance_name = (
        instance_name
        or ""
    ).strip()

    if not instance_name:
        return 0

    q = (
        db.query(RequestLog)
        .filter(
            RequestLog.instance_name
            == instance_name,
            RequestLog.status
            == "DONE",

            # API externa tiene su propia
            # contabilidad y cobro.
            RequestLog.api_client_id
            .is_(None),

            # Blindaje adicional para datos
            # API históricos.
            or_(
                RequestLog.source_group_id
                .is_(None),
                ~RequestLog.source_group_id
                .like("api_cliente_%"),
            ),

            # MAYAPROVIDER es privado y jamás
            # consume contador principal.
            or_(
                RequestLog.provider_name
                .is_(None),
                RequestLog.provider_name
                != "MAYAPROVIDER",
            ),
        )
    )

    hidden_groups = (
        BOT_USED_HIDDEN_NO_ACCOUNTING_GROUPS
        .get(
            instance_name.lower(),
            set(),
        )
    )

    if hidden_groups:
        q = q.filter(
            or_(
                RequestLog.source_group_id
                .is_(None),
                ~RequestLog.source_group_id
                .in_(hidden_groups),
            )
        )

    # Seguridad adicional para Gestoría Maya:
    # aunque provider_name histórico venga vacío/mal,
    # sus grupos privados no consumen.
    if instance_name.lower() == "docifybot8maya":
        maya_groups = {
            str(g).strip()
            for g in (
                getattr(
                    settings,
                    "MAYAPROVIDER_GROUP_1",
                    "",
                ),
                getattr(
                    settings,
                    "MAYAPROVIDER_GROUP_2",
                    "",
                ),
            )
            if str(g or "").strip()
        }

        if maya_groups:
            q = q.filter(
                or_(
                    RequestLog.provider_group_id
                    .is_(None),
                    ~RequestLog.provider_group_id
                    .in_(maya_groups),
                )
            )

    return int(
        q.count()
        or 0
    )


def get_bot_used(
    db: Session,
    instance_name: str,
) -> int:
    """
    Contador persistente y monotónico.

    Desde MONOTONIC_BOT_LEDGER_V1 jamás se vuelve
    a reconstruir desde request_logs, porque esa
    tabla expira por cleanup.
    """
    instance_name = (instance_name or "").strip()

    if not instance_name:
        return 0

    try:
        return max(
            0,
            int(
                _app_setting_get(
                    db,
                    _bot_used_key(instance_name),
                    "0",
                )
                or "0"
            ),
        )
    except Exception:
        return 0


# =========================
# SETTERS
# =========================
def set_bot_limit(db: Session, instance_name: str, limit_value: int):
    _app_setting_set(
        db,
        _bot_limit_key(instance_name),
        str(max(0, int(limit_value))),
    )


def set_bot_used(
    db: Session,
    instance_name: str,
    used_value: int,
):
    instance_name = (instance_name or "").strip()

    if not instance_name:
        return

    used_value = max(0, int(used_value or 0))

    _app_setting_set(
        db,
        _bot_used_key(instance_name),
        str(used_value),
    )


# =========================
# MAIN LOGIC
# =========================
def increment_bot_used_and_maybe_block(
    db: Session,
    instance_name: str,
    request_id: int | None = None,
) -> tuple[int, int, bool]:
    """
    MONOTONIC_BOT_LEDGER_V1

    Cada request_id cobrable puede consumir una sola vez.

    bot_usage_consumptions.request_id es PRIMARY KEY,
    por lo que protege también contra doble accounting
    entre worker.py y main.py.
    """
    from sqlalchemy import text

    instance_name = (instance_name or "").strip()

    if not instance_name:
        return 0, 0, False

    if request_id is None:
        raise RuntimeError(
            "BOT_USAGE_REQUEST_ID_REQUIRED"
        )

    request_id = int(request_id)

    inserted = db.execute(
        text(
            """
            INSERT INTO bot_usage_consumptions
                (request_id, instance_name, used_after, created_at)
            VALUES
                (:request_id, :instance_name, NULL,
                 (NOW() AT TIME ZONE 'UTC'))
            ON CONFLICT (request_id) DO NOTHING
            RETURNING request_id
            """
        ),
        {
            "request_id": request_id,
            "instance_name": instance_name,
        },
    ).scalar()

    key = _bot_used_key(instance_name)

    if inserted is not None:
        db.execute(
            text(
                """
                INSERT INTO app_settings
                    (key, value, updated_at)
                VALUES
                    (:key, '1', (NOW() AT TIME ZONE 'UTC'))
                ON CONFLICT (key)
                DO UPDATE SET
                    value = (
                        COALESCE(
                            NULLIF(app_settings.value, ''),
                            '0'
                        )::bigint + 1
                    )::text,
                    updated_at = (NOW() AT TIME ZONE 'UTC')
                """
            ),
            {"key": key},
        )

    used = int(
        db.execute(
            text(
                """
                SELECT COALESCE(value, '0')::bigint
                FROM app_settings
                WHERE key = :key
                """
            ),
            {"key": key},
        ).scalar()
        or 0
    )

    if inserted is not None:
        db.execute(
            text(
                """
                UPDATE bot_usage_consumptions
                SET used_after = :used
                WHERE request_id = :request_id
                """
            ),
            {
                "used": used,
                "request_id": request_id,
            },
        )

    db.commit()

    limit_value = get_bot_limit(
        db,
        instance_name,
    )

    blocked_now = False

    if (
        limit_value > 0
        and used >= limit_value
    ):
        block_instance(instance_name)
        blocked_now = True

    return (
        used,
        limit_value,
        blocked_now,
    )
