import requests
from app.config import settings


def request_acta(curp: str, act_type: str, request_id: int) -> dict:
    if not settings.PROVIDER_API_URL:
        return {
            "ok": True,
            "provider_ref": f"SIM-{request_id}"
        }

    headers = {
        "Authorization": f"Bearer {settings.PROVIDER_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "request_id": request_id,
        "curp": curp,
        "act_type": act_type,
    }
    resp = requests.post(settings.PROVIDER_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()