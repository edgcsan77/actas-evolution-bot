from redis import Redis
from rq import Queue
from app.config import settings


redis_conn = Redis.from_url(
    settings.REDIS_URL,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
)

request_queue = Queue(
    "actas",
    connection=redis_conn,
    default_timeout=300
)
