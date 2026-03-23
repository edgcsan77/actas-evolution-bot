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
    number = number.strip()

    return number

def send_text(number: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"

    payload = {
        "number": _normalize_number(number),
        "text": text
    }

    resp = requests.post(
        url,
        headers=_headers(),
        json=payload,
        timeout=30
    )

    print("SEND_TEXT_URL =", url, flush=True)
    print("SEND_TEXT_PAYLOAD =", payload, flush=True)
    print("SEND_TEXT_STATUS =", resp.status_code, flush=True)
    print("SEND_TEXT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def send_document(number: str, pdf_url: str, filename: str = "acta.pdf", caption: str = ""):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "number": _normalize_number(number),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "media": pdf_url,
        "fileName": filename
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_DOCUMENT_URL =", url, flush=True)
    print("SEND_DOCUMENT_PAYLOAD =", payload, flush=True)
    print("SEND_DOCUMENT_STATUS =", resp.status_code, flush=True)
    print("SEND_DOCUMENT_BODY =", resp.text, flush=True)

    resp.raise_for_status()
    return resp.json()


def send_group_text(group_jid: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "number": group_jid,
        "textMessage": {
            "text": text
        }
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
    payload = {
        "number": group_jid,
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "media": pdf_url,
        "fileName": filename
    }

    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)

    print("SEND_GROUP_DOCUMENT_URL =", url, flush=True)
    print("SEND_GROUP_DOCUMENT_PAYLOAD =", payload, flush=True)
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
