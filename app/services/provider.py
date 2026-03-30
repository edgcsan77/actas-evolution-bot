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

    print("PROVIDER_REQUEST_URL =", settings.PROVIDER_API_URL, flush=True)
    print("PROVIDER_REQUEST_PAYLOAD =", payload, flush=True)

    resp = requests.post(
        settings.PROVIDER_API_URL,
        headers=headers,
        json=payload,
        timeout=60
    )

    print("PROVIDER_RESPONSE_STATUS =", resp.status_code, flush=True)
    print("PROVIDER_RESPONSE_BODY =", resp.text[:1000], flush=True)

    resp.raise_for_status()

    try:
        return resp.json()
    except Exception:
        return {
            "ok": False,
            "error": "Proveedor devolvió respuesta no JSON",
            "raw": resp.text[:1000]
        }
