import os
import json
import time
import requests
import redis

from app.db import SessionLocal
from app.models import BotControl


STATIC_INSTANCES = {
    "docifybot8",
    "docifybot8max",
    "docifybot8docify",
    "docifybot8maya",
    "docifybot8leli",
    "docifybot8rywya",
    "docifybot8xpress",
    "docifybot8moon",
    "docifybot8trami",
}


BASE = os.getenv("EVOLUTION_BASE_URL", "http://127.0.0.1:8080")
APIKEY = os.getenv("EVOLUTION_APIKEY", "")

for envfile in ["/root/actas-evolution-bot/.env"]:
    if os.path.exists(envfile):
        with open(envfile, "r", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")

                if k == "EVOLUTION_BASE_URL":
                    BASE = v

                if k in (
                    "EVOLUTION_APIKEY",
                    "EVOLUTION_API_KEY",
                    "EVOLUTION_TOKEN",
                ):
                    APIKEY = v


def get_instances():
    instances = set(STATIC_INSTANCES)

    db = SessionLocal()

    try:
        rows = (
            db.query(BotControl.instance_name)
            .filter(BotControl.is_active == True)
            .all()
        )

        for row in rows:
            inst = (row[0] or "").strip()
            if inst:
                instances.add(inst)

    finally:
        db.close()

    return sorted(instances)


rdb = redis.Redis(
    host="127.0.0.1",
    port=6379,
    db=0,
    decode_responses=False,
)


for inst in get_instances():
    cache_key = f"panel:evolution_state:{inst}"

    try:
        resp = requests.get(
            f"{BASE}/instance/connectionState/{inst}",
            headers={"apikey": APIKEY},
            timeout=2.5,
        )

        try:
            data = resp.json()
        except Exception:
            data = {
                "raw": (resp.text or "")[:300]
            }

        state = "unknown"

        if isinstance(data, dict):
            state = (
                data.get("instance", {}).get("state")
                or data.get("state")
                or data.get("connectionState")
                or data.get("status")
                or "unknown"
            )

        result = {
            "ok": resp.status_code < 400,
            "state": str(state or "unknown").strip().lower(),
            "cached": True,
            "checked_at": time.time(),
        }

        rdb.setex(
            cache_key,
            600,
            json.dumps(result, ensure_ascii=False),
        )

        print(inst, resp.status_code, result["state"], flush=True)

    except Exception as e:
        result = {
            "ok": False,
            "state": "unknown",
            "error": str(e),
            "cached": True,
            "checked_at": time.time(),
        }

        rdb.setex(
            cache_key,
            180,
            json.dumps(result, ensure_ascii=False),
        )

        print(inst, "ERROR", str(e)[:160], flush=True)
