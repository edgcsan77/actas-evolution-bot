from __future__ import annotations

import json
import os
from typing import Any

from cryptography.fernet import (
    Fernet,
    InvalidToken,
)

from app.db import SessionLocal
from app.models import AppSetting


SIDEA_ACCOUNTS_SETTING_KEY = (
    "provider16_sidea_accounts_v1"
)

SIDEA_ACCOUNT_SLOTS = tuple(
    f"sidea{i}"
    for i in range(1, 11)
)

SIDEA_MAX_DAILY_LIMIT = 2000
SIDEA_DEFAULT_DAILY_LIMIT = 1000


def _blank_slot() -> dict:
    return {
        "username": "",
        "password_enc": "",
        "daily_limit": SIDEA_DEFAULT_DAILY_LIMIT,
        "enabled": False,
    }


def _blank_store() -> dict:
    return {
        "version": 1,
        "slots": {
            slot: _blank_slot()
            for slot in SIDEA_ACCOUNT_SLOTS
        },
    }


def _normalize_bool(
    value: Any,
    default: bool = False,
) -> bool:

    if isinstance(value, bool):
        return value

    if value is None:
        return default

    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "si",
        "sí",
        "on",
        "enabled",
    }


def _normalize_limit(value: Any) -> int:
    try:
        limit = int(value)
    except Exception:
        limit = SIDEA_DEFAULT_DAILY_LIMIT

    if limit < 1:
        limit = 1

    if limit > SIDEA_MAX_DAILY_LIMIT:
        limit = SIDEA_MAX_DAILY_LIMIT

    return limit


def _fernet() -> Fernet:
    raw = (
        os.getenv(
            "SIDEA_CREDENTIALS_KEY",
            "",
        )
        or ""
    ).strip()

    if not raw:
        raise RuntimeError(
            "SIDEA_CREDENTIALS_KEY_MISSING"
        )

    try:
        return Fernet(
            raw.encode("ascii")
        )
    except Exception as exc:
        raise RuntimeError(
            "SIDEA_CREDENTIALS_KEY_INVALID"
        ) from exc


def _load_store(db) -> dict:
    row = (
        db.query(AppSetting)
        .filter(
            AppSetting.key
            == SIDEA_ACCOUNTS_SETTING_KEY
        )
        .first()
    )

    if not row or not (row.value or "").strip():
        return _blank_store()

    try:
        data = json.loads(row.value)
    except Exception as exc:
        raise RuntimeError(
            "SIDEA_ACCOUNTS_DB_JSON_INVALID"
        ) from exc

    if not isinstance(data, dict):
        raise RuntimeError(
            "SIDEA_ACCOUNTS_DB_NOT_OBJECT"
        )

    raw_slots = data.get("slots")

    if not isinstance(raw_slots, dict):
        raw_slots = {}

    result = _blank_store()

    for slot in SIDEA_ACCOUNT_SLOTS:
        raw = raw_slots.get(slot)

        if not isinstance(raw, dict):
            continue

        result["slots"][slot] = {
            "username": (
                str(
                    raw.get("username")
                    or ""
                ).strip()
            ),
            "password_enc": (
                str(
                    raw.get("password_enc")
                    or ""
                ).strip()
            ),
            "daily_limit": _normalize_limit(
                raw.get(
                    "daily_limit",
                    SIDEA_DEFAULT_DAILY_LIMIT,
                )
            ),
            "enabled": _normalize_bool(
                raw.get("enabled"),
                False,
            ),
        }

    return result


def _save_store(
    db,
    store: dict,
) -> None:

    payload = json.dumps(
        store,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    row = (
        db.query(AppSetting)
        .filter(
            AppSetting.key
            == SIDEA_ACCOUNTS_SETTING_KEY
        )
        .first()
    )

    if row:
        row.value = payload
    else:
        row = AppSetting(
            key=SIDEA_ACCOUNTS_SETTING_KEY,
            value=payload,
        )
        db.add(row)

    db.commit()


def get_sidea_accounts_for_panel(
    db,
) -> list[dict]:

    store = _load_store(db)

    result = []

    for slot in SIDEA_ACCOUNT_SLOTS:
        item = store["slots"][slot]

        result.append(
            {
                "slot": slot,
                "label": slot.upper(),
                "username": (
                    item.get("username")
                    or ""
                ),
                "password_configured": bool(
                    item.get("password_enc")
                ),
                "daily_limit": int(
                    item.get("daily_limit")
                    or SIDEA_DEFAULT_DAILY_LIMIT
                ),
                "enabled": bool(
                    item.get("enabled")
                ),
                "configured": bool(
                    item.get("username")
                ),
            }
        )

    return result


def save_sidea_account_slot(
    db,
    *,
    slot: str,
    username: str,
    password: str | None,
    daily_limit: int,
    enabled: bool,
) -> dict:

    slot = (
        slot
        or ""
    ).strip().lower()

    if slot not in SIDEA_ACCOUNT_SLOTS:
        raise ValueError(
            "SIDEA_SLOT_INVALID"
        )

    username = (
        username
        or ""
    ).strip()

    limit = _normalize_limit(
        daily_limit
    )

    enabled = bool(enabled)

    if enabled and not username:
        raise ValueError(
            "SIDEA_USERNAME_REQUIRED"
        )

    store = _load_store(db)

    # Evitar que dos slots usen el mismo usuario.
    if username:
        username_upper = username.upper()

        for other_slot in SIDEA_ACCOUNT_SLOTS:
            if other_slot == slot:
                continue

            other_username = (
                store["slots"][
                    other_slot
                ].get("username")
                or ""
            ).strip()

            if (
                other_username
                and other_username.upper()
                == username_upper
            ):
                raise ValueError(
                    "SIDEA_USERNAME_DUPLICATED:"
                    f"{other_slot.upper()}"
                )

    current = dict(
        store["slots"][slot]
    )

    old_username = (
        current.get("username")
        or ""
    ).strip()

    username_changed = (
        old_username.upper()
        != username.upper()
    )

    password_changed = False

    password_text = (
        password
        if password is not None
        else ""
    )

    # Contraseña vacía = conservar la anterior.
    if password_text:
        encrypted = (
            _fernet()
            .encrypt(
                password_text.encode(
                    "utf-8"
                )
            )
            .decode("ascii")
        )

        current["password_enc"] = encrypted
        password_changed = True

    current["username"] = username
    current["daily_limit"] = limit
    current["enabled"] = enabled

    store["slots"][slot] = current

    _save_store(
        db,
        store,
    )

    return {
        "slot": slot,
        "label": slot.upper(),
        "username": username,
        "daily_limit": limit,
        "enabled": enabled,
        "password_configured": bool(
            current.get("password_enc")
        ),
        "username_changed": (
            username_changed
        ),
        "password_changed": (
            password_changed
        ),
    }


def load_sidea_account_dicts(
) -> list[dict]:
    """
    Lee las cuentas centrales desde PostgreSQL.

    Nunca devuelve password_enc.
    El password se descifra únicamente en memoria.
    """

    db = SessionLocal()

    try:
        row = (
            db.query(AppSetting)
            .filter(
                AppSetting.key
                == SIDEA_ACCOUNTS_SETTING_KEY
            )
            .first()
        )

        # Sin configuración central:
        # el caller puede usar el legacy env.
        if not row or not (row.value or "").strip():
            return []

        store = _load_store(db)

        result = []
        fernet = None

        for slot in SIDEA_ACCOUNT_SLOTS:
            item = store["slots"][slot]

            username = (
                item.get("username")
                or ""
            ).strip()

            # Slot completamente vacío:
            # no se entrega al pool.
            if not username:
                continue

            password = ""

            password_enc = (
                item.get("password_enc")
                or ""
            ).strip()

            if password_enc:
                if fernet is None:
                    fernet = _fernet()

                try:
                    password = (
                        fernet.decrypt(
                            password_enc.encode(
                                "ascii"
                            )
                        )
                        .decode("utf-8")
                    )
                except InvalidToken as exc:
                    raise RuntimeError(
                        "SIDEA_PASSWORD_DECRYPT_FAILED:"
                        f"{slot}"
                    ) from exc

            result.append(
                {
                    "key": slot,
                    "username": username,
                    "password": password,
                    "daily_limit": int(
                        item.get(
                            "daily_limit"
                        )
                        or SIDEA_DEFAULT_DAILY_LIMIT
                    ),
                    "enabled": bool(
                        item.get("enabled")
                    ),
                }
            )

        return result

    finally:
        db.close()


# ============================================================
# SIDEA_ACCOUNT_CREDENTIALS_V1
# Descifrado exclusivamente en memoria para login SIDEA.
# ============================================================

def get_sidea_account_credentials(
    slot: str,
) -> dict | None:

    slot = (
        slot
        or ""
    ).strip().lower()

    if slot not in SIDEA_ACCOUNT_SLOTS:
        return None

    accounts = (
        load_sidea_account_dicts()
    )

    for account in accounts:
        if (
            account.get("key")
            == slot
        ):
            return account

    return None
