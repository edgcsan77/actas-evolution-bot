from redis import Redis
from rq import Queue
from app.config import settings


redis_conn = Redis.from_url(
    settings.REDIS_URL,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
)

REQUEST_TIMEOUT = int(settings.REQUEST_TIMEOUT_MINUTES) * 60 + 180

request_queue = Queue(
    "actas",
    connection=redis_conn,
    default_timeout=REQUEST_TIMEOUT,
)

slow_request_queue = Queue(
    "actas_slow",
    connection=redis_conn,
    default_timeout=REQUEST_TIMEOUT,
)

broadcast_queue = Queue(
    "broadcast",
    connection=redis_conn,
    default_timeout=1800,
)

ack_queue = Queue(
    "ack",
    connection=redis_conn,
    default_timeout=120,
)
