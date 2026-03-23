from redis import Redis
from rq import Queue
from app.config import settings

redis_conn = Redis.from_url(settings.REDIS_URL)
request_queue = Queue("actas", connection=redis_conn)