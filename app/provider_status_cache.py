import json
import time
from datetime import datetime, timezone, timedelta
from app.config import settings
from app.services.provider3 import Provider3Client
from app.services.provider4 import Provider4Client
from app.db import SessionLocal
from app.queue import redis_conn
from app.models import AppSetting

CACHE_KEY = "panel:providers_status_cached"
CACHE_TTL = 600  # 10 minutos


def _cache_set_json(key: str, value, ttl: int = 30):
    try:
        redis_conn.setex(key, ttl, json.dumps(value, ensure_ascii=False))
    except Exception:
        pass


def _get_app_setting(db, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if not row or row.value is None:
        return default
    return row.value.strip()


def refresh_providers_status():
    db = SessionLocal()

    result = {
        "provider3": {
            "curp": None,
            "cadena": None,
            "error": None,
            "updated_at": None
        },
        "provider4": {
            "total": None,
            "error": None,
            "updated_at": None
        }
    }

    try:
        phpsessid = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)


        if phpsessid:
            client = Provider3Client(phpsessid=phpsessid)

            try:
                lic = client.get_licenses()

                result["provider3"]["curp"] = lic.get("acta_curp")
                result["provider3"]["cadena"] = lic.get("acta_cadena")

            except Exception as e:
                err = str(e)

                if "PROVIDER3_LICENSES_RATE_LIMIT" in err:
                    print("PROVIDER3 RATE LIMIT DETECTADO, esperando 60s...", flush=True)
                    time.sleep(60)
                    result["provider3"]["error"] = "RATE LIMIT (60s)"
                else:
                    result["provider3"]["error"] = err
        else:
            result["provider3"]["error"] = "SIN PHPSESSID"

    except Exception as e:
        result["provider3"]["error"] = str(e)
        print("PROVIDER3_STATUS_ERROR =", str(e), flush=True)

    result["provider3"]["updated_at"] = datetime.now(timezone.utc).isoformat()

    try:
        now = datetime.now()

        month_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
            5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
            9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
        }
        
        fecha_param = f"{now.day:02d}/{month_map[now.month]}/{now.year}"
        
        client4 = Provider4Client()
        today_count = client4.get_done_count_for_date(fecha_param)
        
        result["provider4"]["total"] = today_count
        result["provider4"]["period"] = "today"

    except Exception as e:
        result["provider4"]["error"] = str(e)
        print("PROVIDER4_STATUS_ERROR =", str(e), flush=True)

    result["provider4"]["updated_at"] = datetime.now(timezone.utc).isoformat()

    _cache_set_json(CACHE_KEY, result, ttl=CACHE_TTL)

    print("PROVIDERS_STATUS_REFRESH_OK =", result, flush=True)

    db.close()
