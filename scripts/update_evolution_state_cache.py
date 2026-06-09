import os
import json
import time
import requests
import redis

BASE = os.getenv("EVOLUTION_BASE_URL", "http://127.0.0.1:8080")
APIKEY = os.getenv("EVOLUTION_APIKEY", "")

# Cargar .env si existe
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
                if k in ("EVOLUTION_APIKEY", "EVOLUTION_API_KEY", "EVOLUTION_TOKEN"):
                    APIKEY = v

instances = [
    "docifybot8",
    "docifybot8mx",
    "docifybot8maya",
    "docifybot8moon",
    "docifybot8trami",
    "docifybot8papeamigos",
    "docifybot8gestorfiable",
    "docifybot8max",
    "docifybot8gestoriaeducativa",
    "docifybot8gestoriagama",
    "docifybot8gestoriacaheva",
    "docifybot8gestoriainstadoc",
    "docifybot8gestoriamx",
    "docifybot8gestoriatejero",
    "docifybot8actascroo",
    "docifybot8marvin",
    "docifybot8tramitesleli",
    "docifybot8mastertramitesexpress",
]

rdb = redis.Redis(host="127.0.0.1", port=6379, db=0)

for inst in instances:
    cache_key = f"panel:evolution_state:{inst}"
    try:
        resp = requests.get(
            f"{BASE}/instance/connectionState/{inst}",
            headers={"apikey": APIKEY},
            timeout=3,
        )

        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:300]}

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
            "state": str(state or "unknown").lower(),
            "raw": data,
            "cached": True,
            "checked_at": time.time(),
        }

        rdb.setex(cache_key, 120, json.dumps(result, ensure_ascii=False))
        print(inst, resp.status_code, result["state"])

    except Exception as e:
        result = {
            "ok": False,
            "state": "unknown",
            "error": str(e),
            "cached": True,
            "checked_at": time.time(),
        }
        rdb.setex(cache_key, 60, json.dumps(result, ensure_ascii=False))
        print(inst, "ERROR", str(e)[:120])
