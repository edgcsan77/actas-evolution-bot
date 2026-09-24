# PROVIDER_HEALTH_THARD_V1

import json
import math
import time
import uuid


HEALTH_WINDOW_SEC = 6 * 60 * 60

THARD_BOOTSTRAP_SEC = 90
THARD_MIN_SEC = 45
THARD_MAX_SEC = 180

MIN_DYNAMIC_SAMPLES = 10


def _norm_provider(provider_name):
    return (
        provider_name
        or ""
    ).strip().upper()


def _act_bucket(act_type):
    raw = (
        act_type
        or ""
    ).strip().upper()

    raw = " ".join(
        raw.split()
    )

    if "NAC" in raw:
        if "FOL" in raw:
            return "NACIMIENTO_FOLIO"
        return "NACIMIENTO"

    if "MATR" in raw:
        return "MATRIMONIO"

    if "DEF" in raw:
        return "DEFUNCION"

    if "DIV" in raw:
        return "DIVORCIO"

    if "CAD" in raw:
        return "CADENA"

    return raw or "OTRO"


def _success_key(
    provider_name,
    act_type,
):
    return (
        "provider_health:success:v1:"
        f"{_norm_provider(provider_name)}:"
        f"{_act_bucket(act_type)}"
    )


def _timeout_key(
    provider_name,
    act_type,
):
    return (
        "provider_health:timeout:v1:"
        f"{_norm_provider(provider_name)}:"
        f"{_act_bucket(act_type)}"
    )


# THARD_BREAKER_HEALTH_GATED_V2
def _streak_key(provider_name, act_type):
    """
    Racha de timeout aislada por proveedor + tipo.

    Evita que un timeout de MATRIMONIO, por ejemplo,
    contamine NACIMIENTO del mismo proveedor.
    """
    return (
        "provider_health:timeout_streak:v2:"
        f"{_norm_provider(provider_name)}:"
        f"{_act_bucket(act_type)}"
    )


def _prune(
    redis_conn,
    key,
    now_ts,
):
    cutoff = (
        float(now_ts)
        - HEALTH_WINDOW_SEC
    )

    redis_conn.zremrangebyscore(
        key,
        0,
        cutoff,
    )

    redis_conn.expire(
        key,
        HEALTH_WINDOW_SEC * 2,
    )



# ============================================================
# PROVIDER_HEALTH_HALF_OPEN_BREAKER_V1
# ============================================================

BREAKER_COOLDOWN_SEC = 120
BREAKER_PROBE_REQUIRED_TTL_SEC = 24 * 60 * 60
BREAKER_HALF_OPEN_LOCK_SEC = 900
BREAKER_REQUEST_TTL_SEC = 60 * 60
BREAKER_RECOVERED_TTL_SEC = HEALTH_WINDOW_SEC


def _breaker_provider(
    provider_name,
):
    return _norm_provider(
        provider_name
    )


def _breaker_key(
    kind,
    provider_name,
):
    return (
        "provider_thard:"
        f"{kind}:v1:"
        f"{_breaker_provider(provider_name)}"
    )


def _breaker_request_key(
    request_id,
):
    return (
        "provider_thard:"
        "half_open_request:v1:"
        f"{int(request_id)}"
    )


def _redis_text(value):
    if isinstance(
        value,
        bytes,
    ):
        return value.decode(
            "utf-8",
            errors="ignore",
        )

    if value is None:
        return ""

    return str(value)


def get_provider_breaker_snapshot(
    redis_conn,
    provider_name,
):
    provider = _breaker_provider(
        provider_name
    )

    cooldown_key = _breaker_key(
        "cooldown",
        provider,
    )

    needs_probe_key = _breaker_key(
        "needs_probe",
        provider,
    )

    lock_key = _breaker_key(
        "half_open_lock",
        provider,
    )

    recovered_key = _breaker_key(
        "recovered",
        provider,
    )

    return {
        "provider": provider,
        "cooldown": bool(
            redis_conn.exists(
                cooldown_key
            )
        ),
        "needs_probe": bool(
            redis_conn.exists(
                needs_probe_key
            )
        ),
        "half_open_locked": bool(
            redis_conn.exists(
                lock_key
            )
        ),
        "half_open_owner": (
            _redis_text(
                redis_conn.get(
                    lock_key
                )
            )
        ),
        "recovered": bool(
            redis_conn.exists(
                recovered_key
            )
        ),
    }


def ensure_provider_probe_required(
    redis_conn,
    provider_name,
):
    provider = _breaker_provider(
        provider_name
    )

    return bool(
        redis_conn.set(
            _breaker_key(
                "needs_probe",
                provider,
            ),
            "1",
            nx=True,
            ex=(
                BREAKER_PROBE_REQUIRED_TTL_SEC
            ),
        )
    )


def mark_provider_breaker_open(
    redis_conn,
    provider_name,
    cooldown_sec=BREAKER_COOLDOWN_SEC,
):
    provider = _breaker_provider(
        provider_name
    )

    lock_key = _breaker_key(
        "half_open_lock",
        provider,
    )

    old_owner = _redis_text(
        redis_conn.get(
            lock_key
        )
    ).strip()

    pipe = redis_conn.pipeline()

    pipe.delete(
        _breaker_key(
            "recovered",
            provider,
        )
    )

    pipe.delete(
        lock_key
    )

    if old_owner.isdigit():
        pipe.delete(
            _breaker_request_key(
                int(old_owner)
            )
        )

    pipe.set(
        _breaker_key(
            "needs_probe",
            provider,
        ),
        "1",
        ex=(
            BREAKER_PROBE_REQUIRED_TTL_SEC
        ),
    )

    pipe.set(
        _breaker_key(
            "cooldown",
            provider,
        ),
        "1",
        ex=max(
            1,
            int(cooldown_sec),
        ),
    )

    pipe.execute()

    return get_provider_breaker_snapshot(
        redis_conn,
        provider,
    )


def claim_provider_half_open(
    redis_conn,
    provider_name,
    request_id,
):
    provider = _breaker_provider(
        provider_name
    )

    request_id = int(
        request_id
    )

    snapshot = (
        get_provider_breaker_snapshot(
            redis_conn,
            provider,
        )
    )

    if snapshot["cooldown"]:
        return {
            "allowed": False,
            "mode": "OPEN",
            "provider": provider,
            "reason": "COOLDOWN",
        }

    if not snapshot["needs_probe"]:
        return {
            "allowed": True,
            "mode": "NORMAL",
            "provider": provider,
        }

    lock_key = _breaker_key(
        "half_open_lock",
        provider,
    )

    owner = str(
        request_id
    )

    acquired = redis_conn.set(
        lock_key,
        owner,
        nx=True,
        ex=(
            BREAKER_HALF_OPEN_LOCK_SEC
        ),
    )

    if not acquired:
        existing_owner = (
            _redis_text(
                redis_conn.get(
                    lock_key
                )
            )
            .strip()
        )

        if (
            existing_owner
            == owner
        ):
            redis_conn.set(
                _breaker_request_key(
                    request_id
                ),
                provider,
                ex=(
                    BREAKER_REQUEST_TTL_SEC
                ),
            )

            return {
                "allowed": True,
                "mode": "HALF_OPEN",
                "provider": provider,
                "request_id": request_id,
                "owner": True,
            }

        return {
            "allowed": False,
            "mode": "HALF_OPEN_BUSY",
            "provider": provider,
            "owner": existing_owner,
        }

    redis_conn.set(
        _breaker_request_key(
            request_id
        ),
        provider,
        ex=(
            BREAKER_REQUEST_TTL_SEC
        ),
    )

    return {
        "allowed": True,
        "mode": "HALF_OPEN",
        "provider": provider,
        "request_id": request_id,
        "owner": True,
    }


# HALF_OPEN_NEUTRAL_RELEASE_V1
def release_provider_half_open_neutral(
    redis_conn,
    provider_name,
    request_id,
):
    """
    Libera solamente el ownership HALF_OPEN.

    NO marca success.
    NO elimina needs_probe.
    NO elimina cooldown.
    NO modifica recovered.

    Se usa cuando el request tomó el probe pero
    todavía NO pudo probar realmente al proveedor,
    por ejemplo SIDEA_ALL_READY_ACCOUNTS_BUSY.
    """
    provider = _breaker_provider(
        provider_name
    )

    request_id = int(
        request_id
    )

    request_key = (
        _breaker_request_key(
            request_id
        )
    )

    lock_key = _breaker_key(
        "half_open_lock",
        provider,
    )

    lua = """
local request_key = KEYS[1]
local lock_key    = KEYS[2]

local expected_provider = ARGV[1]
local expected_owner    = ARGV[2]

local request_provider = redis.call(
    'GET',
    request_key
)

local lock_owner = redis.call(
    'GET',
    lock_key
)

if request_provider ~= expected_provider then
    return 0
end

if lock_owner ~= expected_owner then
    return 0
end

redis.call(
    'DEL',
    lock_key,
    request_key
)

return 1
"""

    result = redis_conn.eval(
        lua,
        2,
        request_key,
        lock_key,
        provider,
        str(request_id),
    )

    try:
        return bool(
            int(result or 0)
        )
    except Exception:
        return False


def mark_provider_half_open_success(
    redis_conn,
    provider_name,
    request_id,
):
    """
    Cierra HALF_OPEN solamente si ESTA solicitud
    sigue siendo la dueña ACTUAL del probe.

    La comparación + liberación ocurre de forma
    atómica dentro de Redis mediante Lua.

    Esto evita que una respuesta vieja/tardía
    libere el lock de un probe más nuevo.
    """
    provider = _breaker_provider(
        provider_name
    )

    request_id = int(
        request_id
    )

    request_key = (
        _breaker_request_key(
            request_id
        )
    )

    lock_key = _breaker_key(
        "half_open_lock",
        provider,
    )

    needs_probe_key = _breaker_key(
        "needs_probe",
        provider,
    )

    cooldown_key = _breaker_key(
        "cooldown",
        provider,
    )

    recovered_key = _breaker_key(
        "recovered",
        provider,
    )

    lua = """
local request_key   = KEYS[1]
local lock_key      = KEYS[2]
local needs_key     = KEYS[3]
local cooldown_key  = KEYS[4]
local recovered_key = KEYS[5]

local expected_provider = ARGV[1]
local expected_owner    = ARGV[2]
local recovered_ttl     = tonumber(ARGV[3])

local request_provider = redis.call(
    'GET',
    request_key
)

local lock_owner = redis.call(
    'GET',
    lock_key
)

if request_provider ~= expected_provider then
    return 0
end

if lock_owner ~= expected_owner then
    return 0
end

redis.call(
    'DEL',
    needs_key,
    cooldown_key,
    lock_key,
    request_key
)

redis.call(
    'SET',
    recovered_key,
    '1',
    'EX',
    recovered_ttl
)

return 1
"""

    result = redis_conn.eval(
        lua,
        5,
        request_key,
        lock_key,
        needs_probe_key,
        cooldown_key,
        recovered_key,
        provider,
        str(request_id),
        int(
            BREAKER_RECOVERED_TTL_SEC
        ),
    )

    try:
        return bool(
            int(result or 0)
        )

    except Exception:
        return False

def record_provider_success(
    redis_conn,
    provider_name,
    act_type,
    latency_s,
):
    if latency_s is None:
        return

    latency_s = float(
        latency_s
    )

    if latency_s < 0:
        return

    now_ts = time.time()

    key = _success_key(
        provider_name,
        act_type,
    )

    payload = json.dumps(
        {
            "id": uuid.uuid4().hex,
            "ts": now_ts,
            "latency_s": latency_s,
        },
        separators=(",", ":"),
    )

    redis_conn.zadd(
        key,
        {
            payload: now_ts,
        },
    )

    _prune(
        redis_conn,
        key,
        now_ts,
    )

    streak_key = _streak_key(provider_name, act_type)

    redis_conn.set(
        streak_key,
        "0",
        ex=HEALTH_WINDOW_SEC,
    )


def record_provider_timeout(
    redis_conn,
    request_id,
    attempt_id,
    provider_name,
    act_type,
):
    once_key = (
        "provider_health:timeout_once:v1:"
        f"{request_id}:"
        f"{attempt_id or 'legacy'}"
    )

    acquired = redis_conn.set(
        once_key,
        "1",
        nx=True,
        ex=24 * 60 * 60,
    )

    if not acquired:
        return False

    now_ts = time.time()

    key = _timeout_key(
        provider_name,
        act_type,
    )

    payload = json.dumps(
        {
            "id": uuid.uuid4().hex,
            "ts": now_ts,
            "request_id": int(
                request_id
            ),
            "attempt_id": (
                attempt_id
                or ""
            ),
        },
        separators=(",", ":"),
    )

    redis_conn.zadd(
        key,
        {
            payload: now_ts,
        },
    )

    _prune(
        redis_conn,
        key,
        now_ts,
    )

    streak_key = _streak_key(provider_name, act_type)

    redis_conn.incr(
        streak_key
    )

    redis_conn.expire(
        streak_key,
        HEALTH_WINDOW_SEC,
    )

    # THARD_BREAKER_HEALTH_GATED_V2
    #
    # Registrar un timeout NO implica tumbar al proveedor.
    # El health ya distingue:
    #
    #   1 timeout  -> HEALTHY
    #   2 seguidos -> DEGRADED
    #   3 seguidos -> DOWN
    #
    # El breaker físico sólo abre con DOWN + racha >= 3.
    health_after = (
        get_provider_health_snapshot(
            redis_conn,
            provider_name,
            act_type,
        )
    )

    breaker_streak = int(
        health_after.get(
            "timeout_streak"
        )
        or 0
    )

    breaker_state = str(
        health_after.get(
            "state"
        )
        or "HEALTHY"
    ).strip().upper()

    # P16_BREAKER_TRANSIENT_STREAK_GUARD_V1
    #
    # El breaker de Provider16 es GLOBAL para las cuentas SIDEA.
    # Una rafaga corta de errores de red en un solo bucket
    # no debe detener todas las cuentas.
    #
    # Otros proveedores conservan el umbral historico de 3.
    # Provider16 exige 8 timeouts consecutivos antes de abrir
    # el breaker global.
    provider_norm = _norm_provider(
        provider_name
    )

    breaker_min_streak = (
        8
        if provider_norm == "PROVIDER16"
        else 3
    )

    breaker_should_open = (
        breaker_state == "DOWN"
        and breaker_streak >= breaker_min_streak
    )

    if (
        provider_norm == "PROVIDER16"
        and breaker_state == "DOWN"
        and not breaker_should_open
    ):
        print(
            "PROVIDER16_BREAKER_SUPPRESSED_TRANSIENT_STREAK =",
            {
                "provider": provider_name,
                "act_type": act_type,
                "request_id": request_id,
                "state": breaker_state,
                "streak": breaker_streak,
                "required_streak": breaker_min_streak,
                "timeout_ratio": health_after.get(
                    "timeout_ratio"
                ),
                "timeout_count": health_after.get(
                    "timeout_count"
                ),
                "success_count": health_after.get(
                    "success_count"
                ),
            },
            flush=True,
        )

    if breaker_should_open:
        try:
            breaker_snapshot = (
                mark_provider_breaker_open(
                    redis_conn,
                    provider_name,
                )
            )

            print(
                "PROVIDER_BREAKER_OPENED_HEALTH_GATED_V2 =",
                {
                    "provider": provider_name,
                    "act_type": act_type,
                    "request_id": request_id,
                    "state": breaker_state,
                    "streak": breaker_streak,
                    "breaker": breaker_snapshot,
                },
                flush=True,
            )

        except Exception as breaker_exc:
            print(
                "PROVIDER_BREAKER_OPEN_ERROR =",
                {
                    "provider": provider_name,
                    "act_type": act_type,
                    "request_id": request_id,
                    "state": breaker_state,
                    "streak": breaker_streak,
                    "error": str(
                        breaker_exc
                    )[:300],
                },
                flush=True,
            )

    elif breaker_state != "HEALTHY":
        print(
            "PROVIDER_BREAKER_NOT_OPENED_HEALTH_GATED_V2 =",
            {
                "provider": provider_name,
                "act_type": act_type,
                "request_id": request_id,
                "state": breaker_state,
                "streak": breaker_streak,
            },
            flush=True,
        )

    return True


def _percentile(
    values,
    percentile,
):
    values = sorted(
        float(x)
        for x in values
    )

    if not values:
        return None

    if len(values) == 1:
        return values[0]

    rank = (
        percentile
        / 100.0
    ) * (
        len(values)
        - 1
    )

    low = int(
        math.floor(rank)
    )

    high = int(
        math.ceil(rank)
    )

    if low == high:
        return values[low]

    weight = rank - low

    return (
        values[low]
        + (
            values[high]
            - values[low]
        )
        * weight
    )


def get_provider_health_snapshot(
    redis_conn,
    provider_name,
    act_type,
):
    now_ts = time.time()
    cutoff = (
        now_ts
        - HEALTH_WINDOW_SEC
    )

    success_key = _success_key(
        provider_name,
        act_type,
    )

    timeout_key = _timeout_key(
        provider_name,
        act_type,
    )

    _prune(
        redis_conn,
        success_key,
        now_ts,
    )

    _prune(
        redis_conn,
        timeout_key,
        now_ts,
    )

    raw_successes = (
        redis_conn.zrangebyscore(
            success_key,
            cutoff,
            "+inf",
        )
        or []
    )

    latencies = []

    for raw in raw_successes:
        try:
            if isinstance(
                raw,
                bytes,
            ):
                raw = raw.decode(
                    "utf-8",
                    errors="ignore",
                )

            data = json.loads(
                raw
            )

            latency = float(
                data.get(
                    "latency_s"
                )
            )

            if latency >= 0:
                latencies.append(
                    latency
                )

        except Exception:
            continue

    timeout_count = int(
        redis_conn.zcount(
            timeout_key,
            cutoff,
            "+inf",
        )
        or 0
    )

    success_count = len(
        latencies
    )

    event_count = (
        success_count
        + timeout_count
    )

    try:
        streak = int(
            redis_conn.get(
                _streak_key(provider_name, act_type)
            )
            or 0
        )

    except Exception:
        streak = 0

    timeout_ratio = (
        timeout_count
        / event_count
        if event_count
        else 0.0
    )

    p50 = _percentile(
        latencies,
        50,
    )

    p95 = _percentile(
        latencies,
        95,
    )

    state = "HEALTHY"

    if (
        streak >= 3
        or (
            event_count >= 8
            and timeout_ratio >= 0.50
        )
    ):
        state = "DOWN"

    elif (
        streak >= 2
        or (
            event_count >= 8
            and timeout_ratio >= 0.25
        )
    ):
        state = "DEGRADED"

    if (
        success_count
        >= MIN_DYNAMIC_SAMPLES
        and p95 is not None
    ):
        t_hard = int(
            round(
                (p95 * 1.75)
                + 5
            )
        )

        t_hard = max(
            THARD_MIN_SEC,
            min(
                THARD_MAX_SEC,
                t_hard,
            ),
        )

    else:
        t_hard = (
            THARD_BOOTSTRAP_SEC
        )

    if state == "DEGRADED":
        t_hard = min(
            t_hard,
            75,
        )

    elif state == "DOWN":
        t_hard = min(
            t_hard,
            60,
        )

    t_hard = max(
        THARD_MIN_SEC,
        t_hard,
    )

    return {
        "provider_name": (
            _norm_provider(
                provider_name
            )
        ),
        "act_bucket": (
            _act_bucket(
                act_type
            )
        ),
        "state": state,
        "success_count": (
            success_count
        ),
        "timeout_count": (
            timeout_count
        ),
        "timeout_ratio": round(
            timeout_ratio,
            4,
        ),
        "timeout_streak": streak,
        "p50_s": (
            round(p50, 3)
            if p50 is not None
            else None
        ),
        "p95_s": (
            round(p95, 3)
            if p95 is not None
            else None
        ),
        "t_hard_sec": int(
            t_hard
        ),
    }
