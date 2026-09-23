from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, or_

from app.models import RequestLog


# ============================================================
# REQUEST_DAILY_CLIENT_SEQUENCE_V1
#
# Número visible para el cliente:
#
#   día local America/Monterrey
#   + grupo WhatsApp
#   + bot / instancia
#
# Ejemplo:
#
#   docifybot8mx + grupo A
#       #1
#       #2
#       #3
#
# Otro bot usando el mismo grupo empieza su propia secuencia.
#
# El número depende de created_at + id, NO del orden de
# finalización. Por eso permanece estable en retries/fallbacks.
# ============================================================

_SEQUENCE_TZ = ZoneInfo("America/Monterrey")


def _created_at_utc_naive(created_at):
    if created_at.tzinfo is None:
        return created_at

    return (
        created_at
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


def request_daily_sequence(
    db,
    req,
) -> int | None:

    group_id = str(
        getattr(
            req,
            "source_group_id",
            "",
        )
        or ""
    ).strip()

    if not group_id:
        # Solicitudes sin grupo WhatsApp no llevan #N.
        return None

    request_id = int(
        getattr(
            req,
            "id",
            0,
        )
        or 0
    )

    created_at = getattr(
        req,
        "created_at",
        None,
    )

    if (
        request_id <= 0
        or created_at is None
    ):
        return None

    instance_name = str(
        getattr(
            req,
            "instance_name",
            "",
        )
        or "docifybot8"
    ).strip().lower()

    created_utc = (
        _created_at_utc_naive(
            created_at
        )
    )

    created_aware = (
        created_utc.replace(
            tzinfo=timezone.utc
        )
    )

    created_local = (
        created_aware.astimezone(
            _SEQUENCE_TZ
        )
    )

    # Día al que pertenece ESTA solicitud.
    # Si termina después de medianoche conserva
    # el número correspondiente al día en que entró.
    start_local = datetime(
        created_local.year,
        created_local.month,
        created_local.day,
        0,
        0,
        0,
        tzinfo=_SEQUENCE_TZ,
    )

    end_local = (
        start_local
        .replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
        )
    )

    start_utc = (
        start_local
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )

    end_utc = (
        end_local
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )

    total = (
        db.query(
            func.count(
                RequestLog.id
            )
        )
        .filter(
            RequestLog.source_group_id
            == group_id,

            func.lower(
                func.coalesce(
                    RequestLog.instance_name,
                    "docifybot8",
                )
            )
            == instance_name,

            # REQUEST_DAILY_SEQUENCE_EXCLUDE_ERROR_V2
            # Los ERROR no cuentan en el mini panel "Hoy",
            # por lo que tampoco deben consumir un #N visible.
            RequestLog.status != "ERROR",

            RequestLog.created_at
            >= start_utc,

            RequestLog.created_at
            <= end_utc,

            or_(
                RequestLog.created_at
                < created_utc,

                and_(
                    RequestLog.created_at
                    == created_utc,

                    RequestLog.id
                    <= request_id,
                ),
            ),
        )
        .scalar()
    )

    total = int(
        total
        or 0
    )

    if total <= 0:
        return None

    return total


def build_delivery_caption(
    db,
    req,
    time_text: str,
) -> str:

    base = (
        "⏱️ Tiempo total: "
        f"{time_text}"
    )

    try:
        sequence = (
            request_daily_sequence(
                db,
                req,
            )
        )

    except Exception as exc:

        # JAMÁS impedir entrega del PDF porque falló
        # el contador visual.
        print(
            "REQUEST_DAILY_SEQUENCE_ERROR =",
            {
                "request_id": getattr(
                    req,
                    "id",
                    None,
                ),
                "group": getattr(
                    req,
                    "source_group_id",
                    None,
                ),
                "instance": getattr(
                    req,
                    "instance_name",
                    None,
                ),
                "error": str(exc)[:300],
            },
            flush=True,
        )

        return base

    if sequence is None:
        return base

    caption = (
        f"#{sequence} · "
        f"{base}"
    )

    print(
        "REQUEST_DAILY_SEQUENCE =",
        {
            "request_id": getattr(
                req,
                "id",
                None,
            ),
            "group": getattr(
                req,
                "source_group_id",
                None,
            ),
            "instance": getattr(
                req,
                "instance_name",
                None,
            ),
            "sequence": sequence,
            "caption": caption,
        },
        flush=True,
    )

    return caption


# ============================================================
# REQUEST_DAILY_SEQUENCE_DONE_ONLY_V3
#
# Número FINAL visible al cliente.
#
# Se calcula JUSTO antes del sendMedia final:
#
#     DONE actuales del grupo/bot/día + 1
#
# ERROR / QUEUED / PROCESSING no consumen número.
#
# MAYAPROVIDER privado no consume contador, igual que panel.
# ============================================================

def request_next_done_sequence(
    db,
    req,
) -> int | None:

    group_id = str(
        getattr(
            req,
            "source_group_id",
            "",
        )
        or ""
    ).strip()

    if not group_id:
        return None

    request_id = int(
        getattr(
            req,
            "id",
            0,
        )
        or 0
    )

    created_at = getattr(
        req,
        "created_at",
        None,
    )

    if created_at is None:
        return None

    instance_name = str(
        getattr(
            req,
            "instance_name",
            "",
        )
        or "docifybot8"
    ).strip().lower()

    provider_name = str(
        getattr(
            req,
            "provider_name",
            "",
        )
        or ""
    ).strip().upper()

    # Gestoría Maya privada expresamente NO cuenta
    # en panel/promoción/límite.
    if provider_name == "MAYAPROVIDER":
        return None

    created_utc = _created_at_utc_naive(
        created_at
    )

    created_local = (
        created_utc
        .replace(tzinfo=timezone.utc)
        .astimezone(_SEQUENCE_TZ)
    )

    start_local = datetime(
        created_local.year,
        created_local.month,
        created_local.day,
        0,
        0,
        0,
        tzinfo=_SEQUENCE_TZ,
    )

    end_local = start_local.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
    )

    start_utc = (
        start_local
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )

    end_utc = (
        end_local
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )

    done_count = (
        db.query(
            func.count(
                RequestLog.id
            )
        )
        .filter(
            RequestLog.source_group_id
            == group_id,

            func.lower(
                func.coalesce(
                    RequestLog.instance_name,
                    "docifybot8",
                )
            )
            == instance_name,

            RequestLog.status
            == "DONE",

            RequestLog.created_at
            >= start_utc,

            RequestLog.created_at
            <= end_utc,

            # La actual todavía está PROCESSING,
            # pero queda blindado por si se llama
            # posteriormente.
            RequestLog.id
            != request_id,

            # Privado Maya no forma parte del panel.
            func.coalesce(
                RequestLog.provider_name,
                "",
            )
            != "MAYAPROVIDER",
        )
        .scalar()
    )

    done_count = int(
        done_count
        or 0
    )

    return done_count + 1


def build_final_delivery_caption(
    db,
    req,
    time_text: str,
) -> str:

    base = (
        "⏱️ Tiempo total: "
        f"{time_text}"
    )

    try:
        sequence = (
            request_next_done_sequence(
                db,
                req,
            )
        )

    except Exception as exc:

        # El contador jamás debe impedir
        # la entrega del PDF.
        print(
            "REQUEST_DONE_SEQUENCE_ERROR =",
            {
                "request_id": getattr(
                    req,
                    "id",
                    None,
                ),
                "group": getattr(
                    req,
                    "source_group_id",
                    None,
                ),
                "instance": getattr(
                    req,
                    "instance_name",
                    None,
                ),
                "error": str(exc)[:300],
            },
            flush=True,
        )

        return base

    if sequence is None:
        return base

    caption = (
        f"#{sequence} · "
        f"{base}"
    )

    print(
        "REQUEST_DONE_SEQUENCE_FINAL =",
        {
            "request_id": getattr(
                req,
                "id",
                None,
            ),
            "group": getattr(
                req,
                "source_group_id",
                None,
            ),
            "instance": getattr(
                req,
                "instance_name",
                None,
            ),
            "sequence": sequence,
            "caption": caption,
        },
        flush=True,
    )

    return caption

