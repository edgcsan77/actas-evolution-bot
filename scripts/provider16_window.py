from __future__ import annotations

import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

from app.db import SessionLocal
from app.models import ProviderSetting
from app.queue import redis_conn


TZ = ZoneInfo("America/Mexico_City")

WORKERS = [
    f"actas-provider16-worker@{i}.service"
    for i in range(1, 11)
]


def clear_panel_cache() -> None:
    try:
        for key in redis_conn.scan_iter("panel:*"):
            redis_conn.delete(key)

        redis_conn.delete(
            "panel:providers_status_text:v1"
        )

    except Exception as exc:
        print(
            "PROVIDER16_WINDOW_CACHE_WARN =",
            repr(exc),
            flush=True,
        )


def set_provider_enabled(
    enabled: bool,
) -> None:

    db = SessionLocal()

    try:
        row = (
            db.query(ProviderSetting)
            .filter(
                ProviderSetting.provider_name
                == "PROVIDER16"
            )
            .first()
        )

        if row is None:
            raise RuntimeError(
                "PROVIDER16_SETTING_NOT_FOUND"
            )

        row.is_enabled = bool(enabled)

        db.commit()
        db.refresh(row)

        print(
            "PROVIDER16_WINDOW_DB =",
            {
                "provider": "PROVIDER16",
                "enabled": bool(
                    row.is_enabled
                ),
            },
            flush=True,
        )

    finally:
        db.close()


def set_workers(
    enabled: bool,
) -> None:

    action = (
        "start"
        if enabled
        else "stop"
    )

    subprocess.run(
        [
            "systemctl",
            action,
            *WORKERS,
        ],
        check=True,
    )

    print(
        "PROVIDER16_WINDOW_WORKERS =",
        {
            "action": action,
            "workers": "1..10",
        },
        flush=True,
    )


def main() -> None:

    now = datetime.now(TZ)

    minutes = (
        now.hour * 60
        + now.minute
    )

    open_now = (
        7 * 60
        <= minutes
        < 23 * 60
    )

    stamp = now.strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )

    if open_now:

        print(
            "PROVIDER16_WINDOW_OPEN",
            stamp,
            flush=True,
        )

        # Primero habilitar routing.
        set_provider_enabled(True)

        # Después asegurar los 10 workers.
        set_workers(True)

    else:

        print(
            "PROVIDER16_WINDOW_CLOSED",
            stamp,
            flush=True,
        )

        # Primero impedir nuevas asignaciones.
        set_provider_enabled(False)

        # Después detener workers de forma segura.
        set_workers(False)

    clear_panel_cache()

    print(
        "PROVIDER16_WINDOW_RECONCILE_OK",
        {
            "open": open_now,
            "time": stamp,
        },
        flush=True,
    )


if __name__ == "__main__":
    main()
