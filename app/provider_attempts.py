import json
import time
import uuid


# PROVIDER_ATTEMPTS_V2
ATTEMPTS_KEY_PREFIX = "provider_attempts:v2:"
ATTEMPT_MSG_KEY_PREFIX = "provider_attempt_msg:v2:"
WINNER_KEY_PREFIX = "provider_pdf_winner:v2:"

ATTEMPTS_TTL_SECONDS = 86400


def _decode(value):
    if isinstance(value, bytes):
        return value.decode(
            "utf-8",
            errors="ignore",
        )
    return value


def attempts_key(request_id: int) -> str:
    return (
        f"{ATTEMPTS_KEY_PREFIX}"
        f"{int(request_id)}"
    )


def attempt_message_key(
    message_id: str,
) -> str:
    return (
        f"{ATTEMPT_MSG_KEY_PREFIX}"
        f"{(message_id or '').strip()}"
    )


def winner_key(request_id: int) -> str:
    return (
        f"{WINNER_KEY_PREFIX}"
        f"{int(request_id)}"
    )


def register_provider_attempt(
    redis_conn,
    *,
    request_id: int,
    provider_name: str,
    provider_group_id: str | None,
    provider_message_id: str | None = None,
    provider_message: str | None = None,
    sent_ts: float | None = None,
) -> dict:

    attempt = {
        "attempt_id": uuid.uuid4().hex,
        "request_id": int(request_id),
        "provider_name": (
            provider_name
            or ""
        ).strip().upper(),
        "provider_group_id": (
            provider_group_id
            or ""
        ).strip(),
        "provider_message_id": (
            provider_message_id
            or ""
        ).strip(),
        "provider_message": (
            provider_message
            or ""
        ),
        "sent_ts": float(
            sent_ts
            if sent_ts is not None
            else time.time()
        ),
    }

    key = attempts_key(
        request_id
    )

    payload = json.dumps(
        attempt,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    redis_conn.rpush(
        key,
        payload,
    )

    redis_conn.expire(
        key,
        ATTEMPTS_TTL_SECONDS,
    )

    message_id = attempt[
        "provider_message_id"
    ]

    if message_id:
        redis_conn.setex(
            attempt_message_key(
                message_id
            ),
            ATTEMPTS_TTL_SECONDS,
            payload,
        )

    return attempt


def get_provider_attempts(
    redis_conn,
    request_id: int,
) -> list[dict]:

    rows = redis_conn.lrange(
        attempts_key(request_id),
        0,
        -1,
    )

    result = []

    for raw in rows:
        try:
            item = json.loads(
                _decode(raw)
            )

            if isinstance(
                item,
                dict,
            ):
                result.append(item)

        except Exception:
            continue

    return result


def find_provider_attempt_by_message(
    redis_conn,
    message_id: str | None,
) -> dict | None:

    message_id = (
        message_id
        or ""
    ).strip()

    if not message_id:
        return None

    raw = redis_conn.get(
        attempt_message_key(
            message_id
        )
    )

    if not raw:
        return None

    try:
        item = json.loads(
            _decode(raw)
        )

        if isinstance(
            item,
            dict,
        ):
            return item

    except Exception:
        pass

    return None


def find_provider_attempt(
    redis_conn,
    *,
    request_id: int,
    source_chat_id: str | None = None,
    quoted_message_id: str | None = None,
) -> dict | None:

    source = (
        source_chat_id
        or ""
    ).strip()

    quoted = (
        quoted_message_id
        or ""
    ).strip()

    if quoted:
        direct = (
            find_provider_attempt_by_message(
                redis_conn,
                quoted,
            )
        )

        if (
            direct
            and int(
                direct.get(
                    "request_id"
                )
                or 0
            )
            == int(request_id)
        ):
            return direct

    attempts = get_provider_attempts(
        redis_conn,
        request_id,
    )

    if quoted:
        for attempt in reversed(
            attempts
        ):
            if (
                attempt.get(
                    "provider_message_id"
                )
                == quoted
            ):
                return attempt

    if source:
        matches = [
            attempt
            for attempt in attempts
            if (
                attempt.get(
                    "provider_group_id"
                )
                == source
            )
        ]

        if matches:
            return matches[-1]

    return None


def claim_pdf_winner(
    redis_conn,
    *,
    request_id: int,
    attempt: dict | None,
) -> tuple[bool, dict | None]:

    key = winner_key(
        request_id
    )

    winner = {
        "request_id": int(
            request_id
        ),
        "won_at": time.time(),
    }

    if attempt:
        winner.update(
            {
                "attempt_id": (
                    attempt.get(
                        "attempt_id"
                    )
                ),
                "provider_name": (
                    attempt.get(
                        "provider_name"
                    )
                ),
                "provider_group_id": (
                    attempt.get(
                        "provider_group_id"
                    )
                ),
                "provider_message_id": (
                    attempt.get(
                        "provider_message_id"
                    )
                ),
                "sent_ts": (
                    attempt.get(
                        "sent_ts"
                    )
                ),
            }
        )

    payload = json.dumps(
        winner,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    won = bool(
        redis_conn.set(
            key,
            payload,
            nx=True,
            ex=ATTEMPTS_TTL_SECONDS,
        )
    )

    if won:
        return True, winner

    raw = redis_conn.get(
        key
    )

    if not raw:
        return False, None

    try:
        return (
            False,
            json.loads(
                _decode(raw)
            ),
        )

    except Exception:
        return False, None


def provider_attempt_latency(
    attempt: dict | None,
    now_ts: float | None = None,
) -> float | None:

    if not attempt:
        return None

    try:
        sent_ts = float(
            attempt.get(
                "sent_ts"
            )
        )

    except Exception:
        return None

    end_ts = float(
        now_ts
        if now_ts is not None
        else time.time()
    )

    return max(
        0.0,
        end_ts - sent_ts,
    )


# ============================================================
# PROVIDER_ATTEMPTS_V3_RELEASE
# ============================================================

def get_pdf_winner(
    redis_conn,
    request_id: int,
) -> dict | None:

    raw = redis_conn.get(
        winner_key(request_id)
    )

    if not raw:
        return None

    try:
        item = json.loads(
            _decode(raw)
        )

        if isinstance(
            item,
            dict,
        ):
            return item

    except Exception:
        pass

    return None


def release_pdf_winner_if_owned(
    redis_conn,
    *,
    request_id: int,
    attempt: dict | None,
) -> bool:
    """
    Libera el winner solamente si sigue siendo
    exactamente el winner perteneciente a este intento.

    Se usa cuando el primer PDF valido gano,
    pero NO pudo persistirse correctamente en R2.
    """

    key = winner_key(
        request_id
    )

    raw = redis_conn.get(
        key
    )

    if not raw:
        return False

    try:
        current = json.loads(
            _decode(raw)
        )
    except Exception:
        return False

    current_attempt_id = (
        current.get(
            "attempt_id"
        )
        or None
    )

    expected_attempt_id = (
        (
            attempt
            or {}
        ).get(
            "attempt_id"
        )
        or None
    )

    if (
        current_attempt_id
        != expected_attempt_id
    ):
        return False

    script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    deleted = redis_conn.eval(
        script,
        1,
        key,
        raw,
    )

    return bool(
        deleted
    )
