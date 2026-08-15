import os
import requests


def replay_provider_webhook(payload: dict):
    """
    Reinyecta un webhook de proveedor que llegó antes de que la solicitud
    estuviera lista para hacer match. Se ejecuta desde RQ/ack y por eso
    sobrevive al retorno HTTP del webhook original.
    """
    url = (
        os.getenv("ACTAS_WEBHOOK_RETRY_URL")
        or "http://127.0.0.1:8000/webhook/evolution"
    ).strip()

    if not url:
        raise RuntimeError("ACTAS_WEBHOOK_RETRY_URL_EMPTY")

    response = requests.post(url, json=payload, timeout=90)
    response.raise_for_status()
    return {
        "ok": True,
        "status_code": response.status_code,
        "response": (response.text or "")[:1000],
    }
