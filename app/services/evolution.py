import requests
from app.config import settings


def _headers():
    return {
        "apikey": settings.EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


def _normalize_number(number: str) -> str:
    return (number or "").replace("@s.whatsapp.net", "").strip()


def send_text(number: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "number": _normalize_number(number),
        "text": text
    }
    resp = requests.post(url, headers=_headers(), json=payload, timeout=30)
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
    resp.raise_for_status()
    return resp.json()


def send_group_text(group_jid: str, text: str):
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{settings.EVOLUTION_INSTANCE}"
    payload = {
        "number": group_jid,
        "text": text
    }
    resp = requests.post(url, headers=_headers(), json=payload, timeout=30)
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
    resp.raise_for_status()
    return resp.json()
