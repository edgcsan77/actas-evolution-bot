import requests
from app.config import settings


def _headers():
    return {
        "apikey": settings.EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


def _normalize_number(number: str) -> str:
    if not number:
        return ""
    number = str(number)
    number = number.replace("@s.whatsapp.net", "")
    number = number.replace("@g.us", "")
    number = number.replace("+", "")
    number = number.replace(" ", "")
    return number.strip()


def send_text(number: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"

    clean_number = _normalize_number(number)
    clean_text = (text or "").strip()

    payload = {
        "number": clean_number,
        "text": clean_text,
    }

    print("SEND_TEXT_URL =", url, flush=True)
    print("SEND_TEXT_PAYLOAD =", payload, flush=True)

    resp = requests.post(url, headers=_headers(), json=payload, timeout=30)

    print("SEND_TEXT_STATUS =", resp.status_code, flush=True)
    print("SEND_TEXT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()
