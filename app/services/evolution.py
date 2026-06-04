import requests
import base64
import time
from app.config import settings


def _headers():
    return {
        "apikey": settings.EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }



def _is_internal_api_group(group_jid: str | None) -> bool:
    gid = (group_jid or "").strip().lower()
    return gid.startswith("api_") or gid.startswith("api:")


def _normalize_number(number: str) -> str:
    if not number:
        return ""

    number = str(number).strip()

    # si es grupo no tocar
    if "@g.us" in number:
        return number

    # si es usuario normal limpiar
    number = number.replace("@s.whatsapp.net", "")
    number = number.replace("+", "")
    number = number.replace(" ", "")

    return number


def _post_send_media_with_retries(url: str, payload: dict, *, label: str, max_attempts: int = 3):
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"{label}_ATTEMPT =", attempt, flush=True)

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=180,
            )

            print(f"{label}_STATUS =", resp.status_code, flush=True)
            print(f"{label}_BODY =", resp.text[:1000], flush=True)

            if resp.status_code in (200, 201):
                return resp.json()

            last_error = requests.HTTPError(
                f"{resp.status_code} Server Error for url: {url} | body={resp.text[:1000]}",
                response=resp,
            )

            # Reintentar solo errores temporales/server
            if resp.status_code not in (408, 429, 500, 502, 503, 504):
                raise last_error

        except Exception as e:
            last_error = e
            print(f"{label}_ERROR_ATTEMPT_{attempt} =", str(e), flush=True)

        if attempt < max_attempts:
            time.sleep(5 * attempt)

    raise last_error


def _post_send_text_with_retries(url: str, payload: dict, *, label: str, max_attempts: int = 3):
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"{label}_ATTEMPT =", attempt, flush=True)

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=30,
            )

            print(f"{label}_STATUS =", resp.status_code, flush=True)
            print(f"{label}_BODY =", resp.text[:1000], flush=True)

            if resp.status_code in (200, 201):
                return resp.json()

            last_error = requests.HTTPError(
                f"{resp.status_code} Server Error for url: {url} | body={resp.text[:1000]}",
                response=resp,
            )

            body_text = resp.text or ""
            connection_closed = "Connection Closed" in body_text or "CONNECTION CLOSED" in body_text.upper()
            
            # Reintentar errores temporales/server.
            # Evolution a veces responde 400 aunque el problema real es socket/conexión cerrada.
            if resp.status_code not in (408, 429, 500, 502, 503, 504) and not connection_closed:
                raise last_error

        except Exception as e:
            last_error = e
            print(f"{label}_ERROR_ATTEMPT_{attempt} =", str(e), flush=True)

        if attempt < max_attempts:
            time.sleep(2 * attempt)

    raise last_error


def send_text(number: str, text: str, instance_name: str = None):
    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{instance}"

    clean_number = _normalize_number(number)
    clean_text = (text or "").strip()

    payload = {
        "number": clean_number,
        "text": clean_text,
    }

    print("SEND_TEXT_URL =", url, flush=True)
    print("SEND_TEXT_PAYLOAD =", payload, flush=True)

    return _post_send_text_with_retries(
        url,
        payload,
        label="SEND_TEXT",
        max_attempts=3,
    )


def send_document(number: str, pdf_url: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    r = requests.get(pdf_url, timeout=60)
    r.raise_for_status()

    if b"%PDF" not in r.content[:20]:
        raise ValueError("La URL no devolvió un PDF válido")

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


def send_group_text(group_jid: str, text: str, instance_name: str = None):
    if _is_internal_api_group(group_jid):
        print("SEND_GROUP_TEXT_SKIPPED_INTERNAL_API_GROUP =", group_jid, flush=True)
        return {"ok": True, "skipped": "internal_api_group"}

    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendText/{instance}"

    payload = {
        "number": _normalize_number(group_jid),
        "text": (text or "").strip(),
    }

    print("SEND_GROUP_TEXT_URL =", url, flush=True)
    print("SEND_GROUP_TEXT_PAYLOAD =", payload, flush=True)

    return _post_send_text_with_retries(
        url,
        payload,
        label="SEND_GROUP_TEXT",
        max_attempts=3,
    )


def send_group_document(group_jid: str, pdf_url: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    if _is_internal_api_group(group_jid):
        print("SEND_GROUP_DOCUMENT_SKIPPED_INTERNAL_API_GROUP =", group_jid, flush=True)
        return {"ok": True, "skipped": "internal_api_group"}

    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    r = requests.get(pdf_url, timeout=60)
    r.raise_for_status()

    if b"%PDF" not in r.content[:20]:
        raise ValueError("La URL no devolvió un PDF válido")

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


def get_media_base64(media_type: str, message_id: str, instance_name: str = None):
    import time

    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/chat/getBase64FromMediaMessage/{instance}"
    
    payload = {
        "message": {
            "key": {
                "id": message_id
            }
        },
        "convertToMp4": False
    }

    print("GET_MEDIA_BASE64_URL =", url, flush=True)
    print("GET_MEDIA_BASE64_MESSAGE_ID =", message_id, flush=True)
    print("GET_MEDIA_BASE64_PAYLOAD =", payload, flush=True)

    last_error = None

    for attempt in range(1, 4):
        try:
            print("GET_MEDIA_BASE64_ATTEMPT =", attempt, flush=True)

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=180
            )

            print("GET_MEDIA_BASE64_STATUS =", resp.status_code, flush=True)
            print("GET_MEDIA_BASE64_BODY =", resp.text[:1000], flush=True)

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_error = e
            print("GET_MEDIA_BASE64_ATTEMPT_ERROR =", {
                "attempt": attempt,
                "message_id": message_id,
                "instance": instance,
                "error": str(e),
            }, flush=True)

            if attempt < 3:
                time.sleep(3)

    raise last_error


def send_document_base64(number: str, media_b64: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    raw = (media_b64 or "").strip()
    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]

    raw = raw.replace("\n", "").replace("\r", "").strip()

    payload = {
        "number": _normalize_number(number),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": raw,
    }

    print("SEND_DOCUMENT_BASE64_URL =", url, flush=True)
    print("SEND_DOCUMENT_BASE64_CAPTION =", repr(caption), flush=True)
    print("SEND_DOCUMENT_BASE64_FILENAME =", filename, flush=True)
    print("SEND_DOCUMENT_BASE64_B64_LEN =", len(raw), flush=True)

    return _post_send_media_with_retries(
        url,
        payload,
        label="SEND_DOCUMENT_BASE64",
        max_attempts=3,
    )


def send_group_document_base64(group_jid: str, media_b64: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    if _is_internal_api_group(group_jid):
        print("SEND_GROUP_DOCUMENT_BASE64_SKIPPED_INTERNAL_API_GROUP =", group_jid, flush=True)
        return {"ok": True, "skipped": "internal_api_group"}

    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    raw = (media_b64 or "").strip()
    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]

    raw = raw.replace("\n", "").replace("\r", "").strip()

    payload = {
        "number": _normalize_number(group_jid),
        "mediatype": "document",
        "mimetype": "application/pdf",
        "caption": caption,
        "fileName": filename,
        "media": raw,
    }

    print("SEND_GROUP_DOCUMENT_BASE64_URL =", url, flush=True)
    print("SEND_GROUP_DOCUMENT_BASE64_CAPTION =", repr(caption), flush=True)
    print("SEND_GROUP_DOCUMENT_BASE64_FILENAME =", filename, flush=True)
    print("SEND_GROUP_DOCUMENT_BASE64_B64_LEN =", len(raw), flush=True)

    return _post_send_media_with_retries(
        url,
        payload,
        label="SEND_GROUP_DOCUMENT_BASE64",
        max_attempts=3,
    )
