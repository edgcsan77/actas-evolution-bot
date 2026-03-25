import requests
import base64
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


def send_document(number: str, pdf_url: str, filename: str = "acta.pdf", caption: str = ""):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{settings.EVOLUTION_INSTANCE}"

    # descargar el PDF
    r = requests.get(pdf_url, timeout=60)
    r.raise_for_status()

    # convertir a base64
    media_b64 = base64.b64encode(r.content).decode()

    payload = {
        "number": _normalize_number(number),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": media_b64
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_DOCUMENT_URL =", url, flush=True)
    print("SEND_DOCUMENT_STATUS =", resp.status_code, flush=True)
    print("SEND_DOCUMENT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def send_group_text(group_jid: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "number": _normalize_number(group_jid),
        "text": (text or "").strip()
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=30)

    print("SEND_GROUP_TEXT_URL =", url, flush=True)
    print("SEND_GROUP_TEXT_PAYLOAD =", payload, flush=True)
    print("SEND_GROUP_TEXT_STATUS =", resp.status_code, flush=True)
    print("SEND_GROUP_TEXT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def send_group_document(group_jid: str, pdf_url: str, filename: str = "acta.pdf", caption: str = ""):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{settings.EVOLUTION_INSTANCE}"

    r = requests.get(pdf_url, timeout=60)
    r.raise_for_status()

    media_b64 = base64.b64encode(r.content).decode()

    payload = {
        "number": _normalize_number(group_jid),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": media_b64
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_GROUP_DOCUMENT_URL =", url, flush=True)
    print("SEND_GROUP_DOCUMENT_STATUS =", resp.status_code, flush=True)
    print("SEND_GROUP_DOCUMENT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def get_media_base64(media_type: str, message_id: str):
    url = f"{settings.EVOLUTION_BASE_URL}/chat/getBase64FromMediaMessage/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "message": {
            "key": {
                "id": message_id
            }
        },
        "convertToMp4": False
    }
    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


def send_document_base64(number: str, media_b64: str, filename: str = "acta.pdf", caption: str = ""):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{settings.EVOLUTION_INSTANCE}"

    payload = {
        "number": _normalize_number(number),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": media_b64,
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_DOCUMENT_BASE64_URL =", url, flush=True)
    print("SEND_DOCUMENT_BASE64_STATUS =", resp.status_code, flush=True)
    print("SEND_DOCUMENT_BASE64_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def send_group_document_base64(group_jid: str, media_b64: str, filename: str = "acta.pdf", caption: str = ""):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{settings.EVOLUTION_INSTANCE}"

    payload = {
        "number": _normalize_number(group_jid),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": media_b64,
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_GROUP_DOCUMENT_BASE64_URL =", url, flush=True)
    print("SEND_GROUP_DOCUMENT_BASE64_STATUS =", resp.status_code, flush=True)
    print("SEND_GROUP_DOCUMENT_BASE64_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()
