import time
from redis import Redis

redis_conn = Redis(
    host="127.0.0.1",
    port=6379,
    db=0,
    socket_timeout=5,
)

FIFO = "provider16:fifo:waiting:v1"
OLDSET = "provider16:emergency:old_normal_backlog:v2"

OFFSET = 5_000_000_000.0

print("P16_NEW_FIRST_V2_STARTED", flush=True)

while True:
    moved = 0

    try:
        old_ids = redis_conn.smembers(OLDSET)

        for raw in old_ids:
            try:
                request_id = int(raw)
            except Exception:
                continue

            member = f"{request_id:020d}"

            score = redis_conn.zscore(
                FIFO,
                member,
            )

            if score is None:
                continue

            score = float(score)

            # Vieja con score cronológico normal:
            # regresarla detrás de las nuevas.
            if score < OFFSET:
                redis_conn.zadd(
                    FIFO,
                    {
                        member:
                        score + OFFSET
                    },
                    xx=True,
                )
                moved += 1

        if moved:
            print(
                "P16_OLD_BACKLOG_REPARKED",
                moved,
                flush=True,
            )

    except Exception as exc:
        print(
            "P16_NEW_FIRST_LOOP_ERROR",
            repr(exc),
            flush=True,
        )

    time.sleep(0.5)
