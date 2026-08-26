from __future__ import annotations

import base64
import hashlib
import html as html_lib
import json
import re
import secrets
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

from app.services.provider16_accounts import (
    get_sidea_account_credentials,
)
from app.services.provider16_sidea import (
    SIDEA_BASE_URL,
    SIDEA_HTTP_CONNECT_TIMEOUT,
    SIDEA_HTTP_READ_TIMEOUT,
    SIDEA_TIMEZONE,
    SideaError,
    SideaPool,
    _sidea_html_is_authenticated,
)


SIDEA_LOGIN_CHALLENGE_TTL = 300
SIDEA_CAPTCHA_STYLE = "botDetect"


def _challenge_key(
    slot: str,
    challenge_id: str,
) -> str:
    return (
        "provider16:sidea:"
        "login_challenge:"
        f"{slot}:{challenge_id}"
    )


def _last_login_key(slot: str) -> str:
    return (
        "provider16:sidea:"
        f"last_login:{slot}"
    )


def _last_error_key(slot: str) -> str:
    return (
        "provider16:sidea:"
        f"last_error:{slot}"
    )


def _new_http_session() -> requests.Session:
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
            "Accept-Language":
                "es-ES,es;q=0.9",
        }
    )

    return session


def _cookies_dict(
    session: requests.Session,
) -> dict[str, str]:

    return {
        cookie.name: cookie.value
        for cookie in session.cookies
        if cookie.name
    }


# SIDEA_CAPTCHA_SESSION_AUDIT_V3

def _cookie_state(
    session: requests.Session,
) -> list[dict]:

    result = []

    for cookie in session.cookies:

        result.append(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain or "",
                "path": cookie.path or "/",
                "secure": bool(cookie.secure),
            }
        )

    return result


def _restore_session(
    cookies,
) -> requests.Session:

    session = _new_http_session()

    # Formato nuevo: conserva domain/path reales.
    if isinstance(cookies, list):

        for item in cookies:

            if not isinstance(item, dict):
                continue

            name = str(
                item.get("name")
                or ""
            )

            value = str(
                item.get("value")
                or ""
            )

            domain = str(
                item.get("domain")
                or ""
            )

            path = str(
                item.get("path")
                or "/"
            )

            if not name:
                continue

            kwargs = {
                "path": path,
            }

            if domain:
                kwargs["domain"] = domain

            session.cookies.set(
                name,
                value,
                **kwargs,
            )

        return session

    # Compatibilidad con challenges viejos.
    for name, value in (
        cookies
        or {}
    ).items():

        session.cookies.set(
            str(name),
            str(value),
            domain=(
                "csidea."
                "registrocivil.gob.mx"
            ),
            path="/",
        )

    return session


def _extract_hidden_fields(
    captcha_html: str,
) -> dict[str, str]:

    result = {}

    for tag in re.findall(
        r"<input\b[^>]*>",
        captcha_html or "",
        flags=re.I | re.S,
    ):
        type_m = re.search(
            r'type=["\']([^"\']+)',
            tag,
            flags=re.I,
        )

        name_m = re.search(
            r'name=["\']([^"\']+)',
            tag,
            flags=re.I,
        )

        value_m = re.search(
            r'value=["\']([^"\']*)',
            tag,
            flags=re.I,
        )

        input_type = (
            type_m.group(1).strip().lower()
            if type_m
            else ""
        )

        if (
            input_type != "hidden"
            or not name_m
        ):
            continue

        name = name_m.group(1).strip()

        value = (
            html_lib.unescape(
                value_m.group(1)
            )
            if value_m
            else ""
        )

        if name:
            result[name] = value

    return result


def _extract_captcha_image_url(
    captcha_html: str,
    endpoint: str,
) -> str:

    for tag in re.findall(
        r"<img\b[^>]*>",
        captcha_html or "",
        flags=re.I | re.S,
    ):
        id_m = re.search(
            r'id=["\']([^"\']+)',
            tag,
            flags=re.I,
        )

        src_m = re.search(
            r'src=["\']([^"\']+)',
            tag,
            flags=re.I,
        )

        if not src_m:
            continue

        tag_id = (
            id_m.group(1)
            if id_m
            else ""
        )

        if (
            tag_id
            != "botDetect_CaptchaImage"
        ):
            continue

        src = html_lib.unescape(
            src_m.group(1)
        )

        return urljoin(
            endpoint,
            src,
        )

    raise SideaError(
        "SIDEA_CAPTCHA_IMAGE_NOT_FOUND"
    )



# ============================================================
# SIDEA_BOTDETECT_VALIDATION_V2
# El plugin oficial deriva validationUrl sustituyendo:
# get=image -> get=validation-result
# ============================================================


# ============================================================
# SIDEA_BOTDETECT_PROOF_V4
#
# Reproduce exclusivamente la transformación JavaScript que
# BotDetect aplica al CAPTCHA introducido manualmente.
#
# JS original:
#
# fp:
#   startingPos = BDC_SP
#   SHA1(str(p) + BDC_VCID) == BDC_Hs
#
# m:
#   mask = p % 65533 + 1
#
# cc2c:
#   cambia upper/lower según los bits de mask.
#
# NO interpreta ni resuelve la imagen CAPTCHA.
# ============================================================

SIDEA_BOTDETECT_MAX_PROOF_STEPS = 2_000_000


def _sidea_botdetect_find_proof(
    vcid: str,
    hs: str,
    sp: str,
) -> int:

    vcid = str(
        vcid
        or ""
    )

    hs = str(
        hs
        or ""
    ).strip().lower()

    sp_text = str(
        sp
        or ""
    ).strip()

    if not vcid:
        raise SideaError(
            "SIDEA_BOTDETECT_VCID_MISSING"
        )

    if not re.fullmatch(
        r"[0-9a-f]{40}",
        hs,
    ):
        raise SideaError(
            "SIDEA_BOTDETECT_HS_INVALID"
        )

    try:
        current = int(
            sp_text
        )
    except Exception as exc:
        raise SideaError(
            "SIDEA_BOTDETECT_SP_INVALID"
        ) from exc

    if (
        current < 0
        or current > 2147483647
    ):
        raise SideaError(
            "SIDEA_BOTDETECT_SP_RANGE_INVALID"
        )

    end = min(
        2147483647,
        current
        + SIDEA_BOTDETECT_MAX_PROOF_STEPS
    )

    vcid_bytes = (
        vcid.encode(
            "utf-8"
        )
    )

    while current <= end:

        candidate = (
            str(current).encode(
                "ascii"
            )
            + vcid_bytes
        )

        digest = (
            hashlib.sha1(
                candidate
            ).hexdigest()
        )

        if digest == hs:
            return current

        current += 1

    raise SideaError(
        "SIDEA_BOTDETECT_PROOF_NOT_FOUND"
    )


def _sidea_botdetect_transform_code(
    captcha_code: str,
    hidden_fields: dict,
) -> str:

    code = str(
        captcha_code
        or ""
    ).strip()

    vcid = str(
        hidden_fields.get(
            "BDC_VCID_botDetect"
        )
        or ""
    )

    hs = str(
        hidden_fields.get(
            "BDC_Hs_botDetect"
        )
        or ""
    )

    sp = str(
        hidden_fields.get(
            "BDC_SP_botDetect"
        )
        or ""
    )

    proof = (
        _sidea_botdetect_find_proof(
            vcid,
            hs,
            sp,
        )
    )

    # JS:
    # d.cm = function(a) {
    #     return a % 65533 + 1
    # }
    mask = (
        proof % 65533
    ) + 1

    # JS:
    # (a >>> 0).toString(2)
    bits = bin(
        mask & 0xFFFFFFFF
    )[2:]

    chars = list(code)

    # Replica cc2c():
    #
    # recorre el texto de derecha a izquierda
    # y consume los bits también desde la derecha.
    for offset in range(
        1,
        len(chars) + 1,
    ):

        char_index = (
            len(chars)
            - offset
        )

        bit_index = (
            len(bits)
            - offset
        )

        bit = (
            bits[bit_index]
            if bit_index >= 0
            else None
        )

        if bit == "1":
            chars[char_index] = (
                chars[
                    char_index
                ].upper()
            )
        else:
            chars[char_index] = (
                chars[
                    char_index
                ].lower()
            )

    return "".join(chars)


def _sidea_captcha_validation_ok(
    response: requests.Response,
) -> bool:

    text = (
        response.text
        or ""
    ).strip()

    if not text:
        return False

    try:
        value = response.json()

        if isinstance(value, bool):
            return value

        if isinstance(value, int):
            return value == 1

        if isinstance(value, str):
            return (
                value.strip().lower()
                in {
                    "true",
                    "1",
                    "yes",
                    "ok",
                }
            )

        if isinstance(value, dict):

            for key in (
                "result",
                "valid",
                "isValid",
                "success",
            ):
                if key not in value:
                    continue

                item = value.get(key)

                if isinstance(item, bool):
                    return item

                if (
                    str(item)
                    .strip()
                    .lower()
                    in {
                        "true",
                        "1",
                        "yes",
                        "ok",
                    }
                ):
                    return True

    except Exception:
        pass

    normalized = (
        text.strip()
        .strip('"')
        .strip("'")
        .strip()
        .lower()
    )

    return normalized in {
        "true",
        "1",
        "yes",
        "ok",
    }


def _login_error_message(
    html: str,
) -> str:

    text = re.sub(
        r"<[^>]+>",
        " ",
        html or "",
    )

    text = html_lib.unescape(text)

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip().lower()

    if (
        "captcha" in text
        or "código de verificación"
        in text
        or "codigo de verificacion"
        in text
    ):
        return (
            "SIDEA no aceptó el inicio "
            "de sesión. Revisa el CAPTCHA."
        )

    if (
        "usuario" in text
        and "contraseña" in text
    ):
        return (
            "SIDEA no aceptó el inicio "
            "de sesión. Revisa las credenciales "
            "o el CAPTCHA."
        )

    return (
        "SIDEA no aceptó el inicio de sesión."
    )


def sidea_start_login_captcha(
    redis_conn,
    slot: str,
) -> dict:

    slot = (
        slot
        or ""
    ).strip().lower()

    account = (
        get_sidea_account_credentials(
            slot
        )
    )

    if not account:
        raise SideaError(
            f"SIDEA_ACCOUNT_NOT_CONFIGURED:{slot}"
        )

    if not account.get("enabled"):
        raise SideaError(
            f"SIDEA_ACCOUNT_DISABLED:{slot}"
        )

    if not (
        account.get("username")
        or ""
    ).strip():
        raise SideaError(
            f"SIDEA_USERNAME_MISSING:{slot}"
        )

    if not (
        account.get("password")
        or ""
    ):
        raise SideaError(
            f"SIDEA_PASSWORD_MISSING:{slot}"
        )

    session = _new_http_session()

    try:
        login_response = session.get(
            f"{SIDEA_BASE_URL}/",
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
            allow_redirects=True,
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_LOGIN_PAGE_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    login_html = (
        login_response.text
        or ""
    )

    if (
        login_response.status_code
        != 200
        or "autenticacion.do"
        not in login_html
    ):
        raise SideaError(
            "SIDEA_LOGIN_PAGE_INVALID"
        )

    captcha_endpoint = urljoin(
        login_response.url,
        "simple-captcha-endpoint",
    )

    try:
        captcha_response = session.get(
            captcha_endpoint,
            params={
                "get": "html",
                "c": SIDEA_CAPTCHA_STYLE,
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_CAPTCHA_HTML_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    if captcha_response.status_code != 200:
        raise SideaError(
            "SIDEA_CAPTCHA_HTML_BAD_STATUS:"
            f"{captcha_response.status_code}"
        )

    captcha_html = (
        captcha_response.text
        or ""
    )

    hidden_fields = (
        _extract_hidden_fields(
            captcha_html
        )
    )

    required = {
        "BDC_VCID_botDetect",
        "BDC_BackWorkaround_botDetect",
        "BDC_Hs_botDetect",
        "BDC_SP_botDetect",
    }

    missing = sorted(
        required
        - set(hidden_fields)
    )

    if missing:
        raise SideaError(
            "SIDEA_CAPTCHA_FIELDS_MISSING:"
            + ",".join(missing)
        )

    image_url = (
        _extract_captcha_image_url(
            captcha_html,
            captcha_endpoint,
        )
    )

    # --------------------------------------------------------
    # BotDetect oficial:
    # después de get=html carga get=script-include.
    # Esto reproduce el mismo ciclo del navegador.
    # --------------------------------------------------------

    vcid = (
        hidden_fields.get(
            "BDC_VCID_botDetect"
        )
        or ""
    ).strip()

    if not vcid:
        raise SideaError(
            "SIDEA_CAPTCHA_VCID_MISSING"
        )

    try:
        script_response = session.get(
            captcha_endpoint,
            params={
                "get":
                    "script-include",
                "c":
                    SIDEA_CAPTCHA_STYLE,
                "t":
                    vcid,
                "cs":
                    "2",
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_CAPTCHA_SCRIPT_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    if script_response.status_code != 200:
        raise SideaError(
            "SIDEA_CAPTCHA_SCRIPT_BAD_STATUS:"
            f"{script_response.status_code}"
        )

    try:
        image_response = session.get(
            image_url,
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_CAPTCHA_IMAGE_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    content_type = (
        image_response.headers.get(
            "Content-Type"
        )
        or ""
    ).split(";")[0].strip()

    if (
        image_response.status_code != 200
        or not image_response.content
        or not content_type.lower().startswith(
            "image/"
        )
    ):
        raise SideaError(
            "SIDEA_CAPTCHA_IMAGE_INVALID"
        )

    challenge_id = (
        secrets.token_urlsafe(24)
    )

    payload = {
        "slot": slot,
        "cookies":
            _cookies_dict(session),

        "cookie_state":
            _cookie_state(session),

        "hidden_fields":
            hidden_fields,

        # URL temporal BotDetect.
        # Permite derivar validation-result
        # exactamente como lo hace jquery-captcha.
        "image_url":
            image_url,

        "created_at":
            datetime.now(
                ZoneInfo(SIDEA_TIMEZONE)
            ).isoformat(),
    }

    redis_conn.setex(
        _challenge_key(
            slot,
            challenge_id,
        ),
        SIDEA_LOGIN_CHALLENGE_TTL,
        json.dumps(
            payload,
            ensure_ascii=False,
        ),
    )

    image_b64 = (
        base64.b64encode(
            image_response.content
        ).decode("ascii")
    )

    return {
        "ok": True,
        "slot": slot,
        "challenge_id":
            challenge_id,
        "expires_in":
            SIDEA_LOGIN_CHALLENGE_TTL,
        "image_data_url": (
            f"data:{content_type};"
            f"base64,{image_b64}"
        ),
    }


def sidea_finish_login(
    redis_conn,
    slot: str,
    challenge_id: str,
    captcha_code: str,
) -> dict:

    slot = (
        slot
        or ""
    ).strip().lower()

    challenge_id = (
        challenge_id
        or ""
    ).strip()

    captcha_code = (
        captcha_code
        or ""
    ).strip()

    if not re.fullmatch(
        r"[A-Za-z0-9]{4,6}",
        captcha_code,
    ):
        raise SideaError(
            "SIDEA_CAPTCHA_CODE_INVALID"
        )

    account = (
        get_sidea_account_credentials(
            slot
        )
    )

    if not account:
        raise SideaError(
            f"SIDEA_ACCOUNT_NOT_CONFIGURED:{slot}"
        )

    username = (
        account.get("username")
        or ""
    ).strip()

    password = (
        account.get("password")
        or ""
    )

    if not username:
        raise SideaError(
            "SIDEA_USERNAME_MISSING"
        )

    if not password:
        raise SideaError(
            "SIDEA_PASSWORD_MISSING"
        )

    key = _challenge_key(
        slot,
        challenge_id,
    )

    raw = redis_conn.get(key)

    if raw is None:
        raise SideaError(
            "SIDEA_CAPTCHA_EXPIRED"
        )

    if isinstance(raw, bytes):
        raw = raw.decode(
            "utf-8",
            errors="replace",
        )

    try:
        challenge = json.loads(raw)
    except Exception as exc:
        redis_conn.delete(key)

        raise SideaError(
            "SIDEA_CAPTCHA_STATE_INVALID"
        ) from exc

    session = _restore_session(
        challenge.get("cookie_state")
        or challenge.get("cookies")
        or {}
    )

    # ========================================================
    # VALIDACION AJAX OFICIAL BOTDETECT
    #
    # jquery-captcha:
    # validationUrl =
    # imageSrcUrl.replace(
    #     "get=image",
    #     "get=validation-result"
    # )
    #
    # Luego:
    # GET validationUrl?i=CODIGO
    # ========================================================

    image_url = (
        challenge.get(
            "image_url"
        )
        or ""
    ).strip()

    hidden_fields = dict(
        challenge.get(
            "hidden_fields"
        )
        or {}
    )

    if (
        not image_url
        or "get=image"
        not in image_url
    ):
        redis_conn.delete(key)

        raise SideaError(
            "SIDEA_CAPTCHA_VALIDATION_URL_MISSING"
        )

    validation_url = (
        image_url.replace(
            "get=image",
            "get=validation-result",
            1,
        )
    )

    # El usuario ya resolvió visualmente el CAPTCHA.
    # Aquí solamente reproducimos la transformación
    # upper/lower que ejecuta BotDetect en el navegador.
    captcha_submit_code = (
        _sidea_botdetect_transform_code(
            captcha_code,
            hidden_fields,
        )
    )

    try:
        validation_response = (
            session.get(
                validation_url,
                params={
                    "i":
                        captcha_submit_code,
                },
                headers={
                    "Referer":
                        f"{SIDEA_BASE_URL}/",
                    "X-Requested-With":
                        "XMLHttpRequest",
                    "Accept":
                        "*/*",
                },
                timeout=(
                    SIDEA_HTTP_CONNECT_TIMEOUT,
                    SIDEA_HTTP_READ_TIMEOUT,
                ),
            )
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_CAPTCHA_VALIDATION_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    if validation_response.status_code != 200:

        raise SideaError(
            "SIDEA_CAPTCHA_VALIDATION_BAD_STATUS:"
            f"{validation_response.status_code}"
        )

    captcha_valid = (
        _sidea_captcha_validation_ok(
            validation_response
        )
    )

    if not captcha_valid:

        # Ese challenge ya no debe reutilizarse.
        redis_conn.delete(key)

        message = (
            "CAPTCHA incorrecto. "
            "Se cargará uno nuevo."
        )

        redis_conn.setex(
            _last_error_key(slot),
            86400,
            message,
        )

        return {
            "ok": False,
            "slot": slot,
            "error": message,
            "needs_new_captcha": True,
        }

    # CAPTCHA válido.
    # Lo consideramos challenge de un solo uso desde aquí.
    redis_conn.delete(key)

    data = dict(
        hidden_fields
    )

    data.update(
        {
            "usuario": username,
            "contrasenia": password,

            # Debe ser exactamente el mismo valor
            # transformado que BotDetect validó.
            "captcha":
                captcha_submit_code,
        }
    )

    try:
        response = session.post(
            f"{SIDEA_BASE_URL}/autenticacion.do",
            data=data,
            headers={
                "Referer":
                    f"{SIDEA_BASE_URL}/",
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
            allow_redirects=True,
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_LOGIN_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    html = response.text or ""

    # El CAPTCHA es de un solo uso.
    redis_conn.delete(key)

    if not _sidea_html_is_authenticated(
        html
    ):
        message = (
            _login_error_message(
                html
            )
        )

        redis_conn.setex(
            _last_error_key(slot),
            86400 * 7,
            message,
        )

        # IMPORTANTE:
        # No destruimos una sesión READY previa
        # si el usuario intentó renovarla y escribió
        # mal el CAPTCHA.
        return {
            "ok": False,
            "slot": slot,
            "error": message,
            "needs_new_captcha": True,
        }

    cookies = (
        _cookies_dict(session)
    )

    pool = SideaPool(
        redis_conn
    )

    pool.save_session(
        account_key=slot,
        cookies=cookies,
    )

    now_text = (
        datetime.now(
            ZoneInfo(SIDEA_TIMEZONE)
        ).isoformat()
    )

    redis_conn.set(
        _last_login_key(slot),
        now_text,
    )

    redis_conn.delete(
        _last_error_key(slot)
    )

    return {
        "ok": True,
        "slot": slot,
        "status": "READY",
        "logged_in_at": now_text,
    }
