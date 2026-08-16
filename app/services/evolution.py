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


def _is_retryable_evolution_error(status_code: int | None, body_text: str = "", exc: Exception | None = None) -> bool:
    body_up = (body_text or "").upper()
    exc_up = str(exc or "").upper()

    if status_code in (408, 425, 429, 500, 502, 503, 504):
        return True

    retry_texts = (
        "CONNECTION CLOSED",
        "SERVICE-UNAVAILABLE",
        "ECONNRESET",
        "ECONNREFUSED",
        "ETIMEDOUT",
        "SOCKET",
        "TIMEOUT",
        "PRISMACLIENTKNOWNREQUESTERROR",
        "FAILED TO ESTABLISH A NEW CONNECTION",
        "REMOTE END CLOSED CONNECTION",
        "CONNECTION ABORTED",
    )

    return any(t in body_up or t in exc_up for t in retry_texts)


def _post_send_media_with_retries(url: str, payload: dict, *, label: str, max_attempts: int = 4):
    last_error = None
    delays = [2, 5, 10]

    media_len = len((payload or {}).get("media") or "")
    number = (payload or {}).get("number") or ""
    filename = (payload or {}).get("fileName") or ""

    if not number:
        raise ValueError(f"{label}_EMPTY_NUMBER")

    if not media_len:
        raise ValueError(f"{label}_EMPTY_MEDIA")

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"{label}_ATTEMPT =", attempt, flush=True)
            print(f"{label}_TARGET =", {"number": number, "fileName": filename, "media_len": media_len}, flush=True)

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=(8, 90),
            )

            body_text = resp.text or ""

            print(f"{label}_STATUS =", resp.status_code, flush=True)
            print(f"{label}_BODY =", body_text[:1000], flush=True)

            if resp.status_code in (200, 201):
                try:
                    return resp.json()
                except Exception:
                    return {"ok": True, "raw": body_text[:1000]}

            last_error = requests.HTTPError(
                f"{resp.status_code} Server Error for url: {url} | body={body_text[:1000]}",
                response=resp,
            )

            if not _is_retryable_evolution_error(resp.status_code, body_text):
                raise last_error

        except Exception as e:
            last_error = e
            print(f"{label}_ERROR_ATTEMPT_{attempt} =", str(e), flush=True)

            if attempt >= max_attempts:
                break

            if not _is_retryable_evolution_error(None, "", e):
                break

        if attempt < max_attempts:
            delay = delays[min(attempt - 1, len(delays) - 1)]
            print(f"{label}_RETRY_SLEEP =", delay, flush=True)
            time.sleep(delay)

    raise last_error


def _post_send_text_with_retries(
    url: str,
    payload: dict,
    *,
    label: str,
    max_attempts: int = 5,
):
    last_error = None
    delays = [2, 4, 8, 15]

    number = (payload or {}).get("number") or ""

    if not number:
        raise ValueError(f"{label}_EMPTY_NUMBER")

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"{label}_ATTEMPT =", attempt, flush=True)

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=(5, 35),
            )

            body_text = resp.text or ""

            print(f"{label}_STATUS =", resp.status_code, flush=True)
            print(f"{label}_BODY =", body_text[:1000], flush=True)

            if resp.status_code in (200, 201):
                try:
                    return resp.json()
                except Exception:
                    return {"ok": True, "raw": body_text[:1000]}

            last_error = requests.HTTPError(
                f"{resp.status_code} Server Error for url: {url} | body={body_text[:1000]}",
                response=resp,
            )

            if not _is_retryable_evolution_error(resp.status_code, body_text):
                raise last_error

        except Exception as e:
            last_error = e
            print(f"{label}_ERROR_ATTEMPT_{attempt} =", str(e), flush=True)

            if attempt >= max_attempts:
                break

            if not _is_retryable_evolution_error(None, "", e):
                break

        if attempt < max_attempts:
            delay = delays[min(attempt - 1, len(delays) - 1)]
            print(f"{label}_RETRY_SLEEP =", delay, flush=True)
            time.sleep(delay)

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
        max_attempts=5,
    )


def _normalize_jid_for_reaction(number: str) -> str:
    raw = str(number or "").strip()

    if not raw:
        return ""

    if raw.endswith("@g.us") or raw.endswith("@s.whatsapp.net"):
        return raw

    clean = _normalize_number(raw)

    if not clean:
        return ""

    return f"{clean}@s.whatsapp.net"


def send_reaction(
    number: str,
    message_id: str,
    emoji: str = "🙌",
    instance_name: str = None,
    from_me: bool = False,
):
    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendReaction/{instance}"

    remote_jid = _normalize_jid_for_reaction(number)

    if not remote_jid:
        raise ValueError("SEND_REACTION_EMPTY_REMOTE_JID")

    if not message_id:
        raise ValueError("SEND_REACTION_EMPTY_MESSAGE_ID")

    payload = {
        "key": {
            "remoteJid": remote_jid,
            "fromMe": from_me,
            "id": message_id,
        },
        "reaction": emoji,
    }

    print("SEND_REACTION_URL =", url, flush=True)
    print("SEND_REACTION_PAYLOAD =", payload, flush=True)

    last_error = None
    delays = [2, 4, 8, 15]
    
    for attempt in range(1, 6):
        try:
            print("SEND_REACTION_ATTEMPT =", attempt, flush=True)
    
            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=(5, 35),
            )
    
            body_text = resp.text or ""
    
            print("SEND_REACTION_STATUS =", resp.status_code, flush=True)
            print("SEND_REACTION_BODY =", body_text[:1000], flush=True)
    
            if resp.status_code in (200, 201):
                try:
                    return resp.json()
                except Exception:
                    return {"ok": True, "raw": body_text[:1000]}
    
            last_error = requests.HTTPError(
                f"{resp.status_code} Error for url: {url} | "
                f"body={body_text[:1000]}",
                response=resp,
            )
    
            if not _is_retryable_evolution_error(
                resp.status_code,
                body_text,
            ):
                raise last_error
    
        except Exception as e:
            last_error = e
    
            print(
                f"SEND_REACTION_ERROR_ATTEMPT_{attempt} =",
                str(e),
                flush=True,
            )
    
            if attempt >= 5:
                break
    
            if not _is_retryable_evolution_error(None, "", e):
                break
    
        if attempt < 5:
            delay = delays[min(attempt - 1, len(delays) - 1)]
            print("SEND_REACTION_RETRY_SLEEP =", delay, flush=True)
            time.sleep(delay)
    
    raise last_error


def find_recent_messages(
    instance_name: str,
    remote_jid: str,
    limit: int = 30,
):
    instance = instance_name or settings.EVOLUTION_INSTANCE

    urls = [
        f"{settings.EVOLUTION_BASE_URL}/chat/findMessages/{instance}",
        f"{settings.EVOLUTION_BASE_URL}/messages/findMessages/{instance}",
    ]

    payload = {
        "where": {
            "key": {
                "remoteJid": remote_jid,
            }
        },
        "limit": limit,
    }

    last_error = None

    for url in urls:
        try:
            print(
                "FIND_RECENT_MESSAGES_ATTEMPT =",
                {
                    "url": url,
                    "instance": instance,
                    "remote_jid": remote_jid,
                    "limit": limit,
                },
                flush=True,
            )

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=(5, 20),
            )

            print(
                "FIND_RECENT_MESSAGES_STATUS =",
                {
                    "url": url,
                    "status": resp.status_code,
                },
                flush=True,
            )

            if resp.status_code not in (200, 201):
                last_error = RuntimeError(
                    f"HTTP {resp.status_code}: {(resp.text or '')[:500]}"
                )
                continue

            data = resp.json()

            if isinstance(data, list):
                return data

            if isinstance(data, dict):
                for key in ("messages", "records", "data"):
                    value = data.get(key)
                    if isinstance(value, list):
                        return value

                # Algunas respuestas vienen anidadas.
                for value in data.values():
                    if isinstance(value, list):
                        return value

            return []

        except Exception as exc:
            last_error = exc
            print(
                "FIND_RECENT_MESSAGES_ERROR =",
                {
                    "url": url,
                    "error": str(exc),
                },
                flush=True,
            )

    if last_error:
        raise last_error

    return []


def send_document(number: str, pdf_url: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    r = requests.get(pdf_url, timeout=(3, 10))
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

    print("SEND_DOCUMENT_URL =", url, flush=True)

    return _post_send_media_with_retries(
        url,
        payload,
        label="SEND_DOCUMENT",
        max_attempts=4,
    )


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
        max_attempts=5,
    )


def send_group_document(group_jid: str, pdf_url: str, filename: str = "acta.pdf", caption: str = "", instance_name: str = None):
    if _is_internal_api_group(group_jid):
        print("SEND_GROUP_DOCUMENT_SKIPPED_INTERNAL_API_GROUP =", group_jid, flush=True)
        return {"ok": True, "skipped": "internal_api_group"}

    instance = instance_name or settings.EVOLUTION_INSTANCE
    url = f"{settings.EVOLUTION_BASE_URL}/message/sendMedia/{instance}"

    r = requests.get(pdf_url, timeout=(3, 10))
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

    print("SEND_GROUP_DOCUMENT_URL =", url, flush=True)

    return _post_send_media_with_retries(
        url,
        payload,
        label="SEND_GROUP_DOCUMENT",
        max_attempts=4,
    )


def get_media_base64(
    media_type: str,
    message_id: str,
    instance_name: str = None,
):
    """
    Obtiene el base64 de un medio de Evolution.

    Evolution puede emitir messages.upsert antes de que el archivo
    esté disponible en getBase64FromMediaMessage.

    Una respuesta HTTP 200 sin base64 todavía no se considera éxito.
    """

    import time

    instance = instance_name or settings.EVOLUTION_INSTANCE

    url = (
        f"{settings.EVOLUTION_BASE_URL}"
        f"/chat/getBase64FromMediaMessage/{instance}"
    )

    payload = {
        "message": {
            "key": {
                "id": message_id,
            }
        },
        "convertToMp4": False,
    }

    print("GET_MEDIA_BASE64_URL =", url, flush=True)
    print("GET_MEDIA_BASE64_MESSAGE_ID =", message_id, flush=True)
    print("GET_MEDIA_BASE64_PAYLOAD =", payload, flush=True)

    max_attempts = 5
    retry_delays = [1, 2, 3, 5]
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(
                "GET_MEDIA_BASE64_ATTEMPT =",
                attempt,
                flush=True,
            )

            resp = requests.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=(3, 20),
            )

            print(
                "GET_MEDIA_BASE64_STATUS =",
                resp.status_code,
                flush=True,
            )

            resp.raise_for_status()

            media_json = resp.json()

            media_b64 = (
                media_json.get("base64")
                or media_json.get("data")
                or media_json.get("media")
                or ""
            )

            safe_log = {
                "mediaType": media_json.get("mediaType"),
                "fileName": media_json.get("fileName"),
                "mimetype": media_json.get("mimetype"),
                "has_base64": bool(media_b64),
                "base64_len": len(media_b64 or ""),
                "size": media_json.get("size"),
            }

            print(
                "GET_MEDIA_BASE64_BODY_SAFE =",
                safe_log,
                flush=True,
            )

            # IMPORTANTE:
            # HTTP 200 sin base64 significa que Evolution
            # todavía no tiene disponible el archivo.
            if not media_b64:
                raise RuntimeError(
                    "MEDIA_NOT_READY_YET: "
                    f"message_id={message_id} "
                    f"attempt={attempt}"
                )

            return media_json

        except Exception as exc:
            last_error = exc

            print(
                "GET_MEDIA_BASE64_ATTEMPT_ERROR =",
                {
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "message_id": message_id,
                    "instance": instance,
                    "error": str(exc),
                },
                flush=True,
            )

            if attempt < max_attempts:
                time.sleep(retry_delays[attempt - 1])

    raise RuntimeError(
        "GET_MEDIA_BASE64_EXHAUSTED: "
        f"message_id={message_id} "
        f"instance={instance}: "
        f"{last_error}"
    )


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
        max_attempts=4,
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
        max_attempts=4,
    )
