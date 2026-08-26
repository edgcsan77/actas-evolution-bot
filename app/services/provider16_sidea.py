from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from pypdf import PdfReader, PdfWriter


# ============================================================
# SIDEA / PROVIDER16
# ============================================================

SIDEA_BASE_URL = (
    os.getenv(
        "SIDEA_BASE_URL",
        "https://csidea.registrocivil.gob.mx/SideaV2",
    )
    .strip()
    .rstrip("/")
)

SIDEA_TIMEZONE = os.getenv(
    "SIDEA_TIMEZONE",
    "America/Mexico_City",
).strip() or "America/Mexico_City"

SIDEA_SESSION_TTL_SEC = int(
    os.getenv("SIDEA_SESSION_TTL_SEC", "64800")
)

SIDEA_DEFAULT_DAILY_LIMIT = int(
    os.getenv("SIDEA_DEFAULT_DAILY_LIMIT", "1000")
)

SIDEA_HTTP_CONNECT_TIMEOUT = float(
    os.getenv("SIDEA_HTTP_CONNECT_TIMEOUT", "15")
)

SIDEA_HTTP_READ_TIMEOUT = float(
    os.getenv("SIDEA_HTTP_READ_TIMEOUT", "60")
)


# ============================================================
# ERRORES
# ============================================================

class SideaError(RuntimeError):
    pass


class SideaNeedLogin(SideaError):
    pass


class SideaDailyLimit(SideaError):
    pass


class SideaNoReadyAccount(SideaError):
    pass


class SideaPdfError(SideaError):
    pass


# ============================================================
# CUENTAS
# ============================================================

@dataclass(frozen=True)
class SideaAccount:
    key: str
    username: str
    password: str
    daily_limit: int = SIDEA_DEFAULT_DAILY_LIMIT
    enabled: bool = True


def _bool_value(value: Any, default: bool = True) -> bool:
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()

    if text in {"1", "true", "yes", "si", "sí", "on"}:
        return True

    if text in {"0", "false", "no", "off"}:
        return False

    return default


def load_sidea_accounts() -> list[SideaAccount]:
    """
    SIDEA_ACCOUNTS_JSON ejemplo:

    [
      {
        "key": "jornada_siete",
        "username": "USUARIO1",
        "password": "PASSWORD1",
        "daily_limit": 1000,
        "enabled": true
      },
      {
        "key": "sidea_02",
        "username": "USUARIO2",
        "password": "PASSWORD2",
        "daily_limit": 1000,
        "enabled": true
      }
    ]

    IMPORTANTE:
    - nunca imprimir password
    - nunca subir SIDEA_ACCOUNTS_JSON a Git
    """

    raw = os.getenv("SIDEA_ACCOUNTS_JSON", "[]").strip()

    try:
        data = json.loads(raw)
    except Exception as exc:
        raise SideaError(
            f"SIDEA_ACCOUNTS_JSON_INVALID:{exc}"
        ) from exc

    if not isinstance(data, list):
        raise SideaError(
            "SIDEA_ACCOUNTS_JSON_MUST_BE_LIST"
        )

    result: list[SideaAccount] = []
    seen_keys: set[str] = set()
    seen_users: set[str] = set()

    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise SideaError(
                f"SIDEA_ACCOUNT_INVALID_ITEM:{idx}"
            )

        key = (
            str(item.get("key") or "")
            .strip()
            .lower()
        )

        username = (
            str(item.get("username") or "")
            .strip()
        )

        password = str(
            item.get("password") or ""
        )

        if not key:
            raise SideaError(
                f"SIDEA_ACCOUNT_EMPTY_KEY:{idx}"
            )

        if not re.fullmatch(
            r"[a-z0-9][a-z0-9_-]{0,63}",
            key,
        ):
            raise SideaError(
                f"SIDEA_ACCOUNT_BAD_KEY:{key}"
            )

        if not username:
            raise SideaError(
                f"SIDEA_ACCOUNT_EMPTY_USERNAME:{key}"
            )

        if key in seen_keys:
            raise SideaError(
                f"SIDEA_ACCOUNT_DUPLICATE_KEY:{key}"
            )

        username_upper = username.upper()

        if username_upper in seen_users:
            raise SideaError(
                f"SIDEA_ACCOUNT_DUPLICATE_USERNAME:{username}"
            )

        try:
            daily_limit = int(
                item.get(
                    "daily_limit",
                    SIDEA_DEFAULT_DAILY_LIMIT,
                )
            )
        except Exception as exc:
            raise SideaError(
                f"SIDEA_ACCOUNT_BAD_LIMIT:{key}"
            ) from exc

        if daily_limit < 1:
            raise SideaError(
                f"SIDEA_ACCOUNT_BAD_LIMIT:{key}:{daily_limit}"
            )

        result.append(
            SideaAccount(
                key=key,
                username=username,
                password=password,
                daily_limit=daily_limit,
                enabled=_bool_value(
                    item.get("enabled"),
                    True,
                ),
            )
        )

        seen_keys.add(key)
        seen_users.add(username_upper)

    return result


# ============================================================
# REDIS / SESIONES / CONTADOR DIARIO
# ============================================================

class SideaPool:
    """
    Estado runtime por cuenta:

      provider16:sidea:session:<account>
      provider16:sidea:status:<account>
      provider16:sidea:usage:<YYYY-MM-DD>:<account>

    La contraseña NO se guarda aquí.
    """

    def __init__(self, redis_conn):
        self.redis = redis_conn

    @staticmethod
    def _decode(raw):
        if raw is None:
            return None

        if isinstance(raw, bytes):
            return raw.decode(
                "utf-8",
                errors="replace",
            )

        return str(raw)

    def _today(self) -> str:
        now = datetime.now(
            ZoneInfo(SIDEA_TIMEZONE)
        )

        return now.strftime("%Y-%m-%d")

    def _session_key(self, account_key: str) -> str:
        return (
            f"provider16:sidea:"
            f"session:{account_key}"
        )

    def _status_key(self, account_key: str) -> str:
        return (
            f"provider16:sidea:"
            f"status:{account_key}"
        )

    def _usage_key(self, account_key: str) -> str:
        return (
            f"provider16:sidea:usage:"
            f"{self._today()}:"
            f"{account_key}"
        )

    def set_status(
        self,
        account_key: str,
        status: str,
        ttl_sec: int = SIDEA_SESSION_TTL_SEC,
    ) -> None:
        self.redis.setex(
            self._status_key(account_key),
            int(ttl_sec),
            str(status).strip().upper(),
        )

    def get_status(
        self,
        account_key: str,
    ) -> str:
        raw = self.redis.get(
            self._status_key(account_key)
        )

        return (
            self._decode(raw)
            or "UNKNOWN"
        ).strip().upper()

    def save_session(
        self,
        account_key: str,
        cookies: dict[str, str],
        session_id: str = "",
        usuario: str = "",
        usuario_rol: str = "",
        usuario_entidad: str = "",
    ) -> None:

        clean_cookies = {
            str(k).strip(): str(v)
            for k, v in (cookies or {}).items()
            if str(k).strip()
            and v is not None
        }

        if "oimjsessionid" not in clean_cookies:
            raise SideaError(
                "SIDEA_SESSION_MISSING_OIMJSESSIONID"
            )

        payload = {
            "cookies": clean_cookies,
            "session_id": (
                session_id or ""
            ).strip(),
            "usuario": (
                usuario or ""
            ).strip(),
            "usuario_rol": (
                usuario_rol or ""
            ).strip(),
            "usuario_entidad": (
                usuario_entidad or ""
            ).strip(),
            "saved_at": datetime.now(
                ZoneInfo(SIDEA_TIMEZONE)
            ).isoformat(),
        }

        self.redis.setex(
            self._session_key(account_key),
            SIDEA_SESSION_TTL_SEC,
            json.dumps(
                payload,
                ensure_ascii=False,
            ),
        )

        self.set_status(
            account_key,
            "READY",
        )

    def save_session_from_cookie_header(
        self,
        account_key: str,
        cookie_header: str,
        session_id: str = "",
        usuario: str = "",
        usuario_rol: str = "",
        usuario_entidad: str = "",
    ) -> None:

        cookies: dict[str, str] = {}

        for chunk in (
            cookie_header or ""
        ).split(";"):

            chunk = chunk.strip()

            if not chunk or "=" not in chunk:
                continue

            name, value = chunk.split(
                "=",
                1,
            )

            name = name.strip()
            value = value.strip()

            if name:
                cookies[name] = value

        self.save_session(
            account_key=account_key,
            cookies=cookies,
            session_id=session_id,
            usuario=usuario,
            usuario_rol=usuario_rol,
            usuario_entidad=usuario_entidad,
        )

    def get_session(
        self,
        account_key: str,
    ) -> dict:

        raw = self.redis.get(
            self._session_key(account_key)
        )

        if raw is None:
            raise SideaNeedLogin(
                f"SIDEA_NEED_LOGIN:{account_key}"
            )

        text = self._decode(raw)

        try:
            payload = json.loads(text)
        except Exception as exc:
            raise SideaNeedLogin(
                f"SIDEA_BAD_STORED_SESSION:"
                f"{account_key}"
            ) from exc

        if not isinstance(
            payload.get("cookies"),
            dict,
        ):
            raise SideaNeedLogin(
                f"SIDEA_BAD_STORED_COOKIES:"
                f"{account_key}"
            )

        return payload

    def clear_session(
        self,
        account_key: str,
        reason: str = "SESSION_EXPIRED",
    ) -> None:

        self.redis.delete(
            self._session_key(account_key)
        )

        self.set_status(
            account_key,
            reason,
            ttl_sec=86400,
        )

    def build_http_session(
        self,
        account_key: str,
    ) -> tuple[requests.Session, dict]:

        state = self.get_session(
            account_key
        )

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
                "Accept-Language": "es-ES,es;q=0.9",
            }
        )

        for name, value in (
            state.get("cookies")
            or {}
        ).items():

            session.cookies.set(
                name,
                value,
                domain=(
                    "csidea."
                    "registrocivil.gob.mx"
                ),
                path="/",
            )

        return session, state

    def validate_session(
        self,
        account_key: str,
    ) -> bool:

        session, state = (
            self.build_http_session(
                account_key
            )
        )

        try:
            response = session.get(
                f"{SIDEA_BASE_URL}/solicitudes.do",
                timeout=(
                    SIDEA_HTTP_CONNECT_TIMEOUT,
                    SIDEA_HTTP_READ_TIMEOUT,
                ),
                allow_redirects=True,
            )
        except Exception as exc:
            raise SideaError(
                f"SIDEA_SESSION_CHECK_HTTP_ERROR:"
                f"{account_key}:{exc}"
            ) from exc

        html = response.text or ""

        login_seen = (
            "autenticacion.do" in html
            and "contrasenia" in html.lower()
        )

        authenticated_seen = (
            "logOutAction.do" in html
            and (
                "solicitudXCURP.do" in html
                or "Impresi&oacute;n de Actos" in html
                or "Impresión de Actos" in html
            )
        )

        if (
            response.status_code == 200
            and authenticated_seen
            and not login_seen
        ):
            # Si SIDEA renovó cookies,
            # persistirlas.
            updated_cookies = {
                c.name: c.value
                for c in session.cookies
            }

            self.save_session(
                account_key=account_key,
                cookies=updated_cookies,
                session_id=(
                    state.get("session_id")
                    or ""
                ),
                usuario=(
                    state.get("usuario")
                    or ""
                ),
                usuario_rol=(
                    state.get("usuario_rol")
                    or ""
                ),
                usuario_entidad=(
                    state.get("usuario_entidad")
                    or ""
                ),
            )

            return True

        self.clear_session(
            account_key,
            reason="NEED_LOGIN",
        )

        return False

    def usage(
        self,
        account_key: str,
    ) -> int:

        raw = self.redis.get(
            self._usage_key(account_key)
        )

        if raw is None:
            return 0

        try:
            return int(
                self._decode(raw)
            )
        except Exception:
            return 0

    def reserve_one(
        self,
        account: SideaAccount,
    ) -> int | None:
        """
        Reserva atómica ANTES de crear una solicitud SIDEA.

        Retorna:
          1..daily_limit = reservado
          None           = cuenta agotada

        La key incluye fecha, por lo que cada jornada
        inicia nuevamente en cero.
        """

        key = self._usage_key(
            account.key
        )

        lua = """
        local current = tonumber(
            redis.call('GET', KEYS[1]) or '0'
        )

        local hard_limit = tonumber(ARGV[1])

        if current >= hard_limit then
            return -1
        end

        local new_value = current + 1

        redis.call(
            'SET',
            KEYS[1],
            new_value,
            'EX',
            172800
        )

        return new_value
        """

        value = int(
            self.redis.eval(
                lua,
                1,
                key,
                int(account.daily_limit),
            )
        )

        if value < 0:
            self.set_status(
                account.key,
                "DAILY_LIMIT",
                ttl_sec=172800,
            )

            return None

        return value

    def release_one(
        self,
        account: SideaAccount,
    ) -> int:
        """
        SOLO usar si sabemos con certeza que SIDEA
        NO creó la solicitud.

        Ante timeout/resultado incierto NO liberar,
        para evitar exceder el límite real.
        """

        key = self._usage_key(
            account.key
        )

        lua = """
        local current = tonumber(
            redis.call('GET', KEYS[1]) or '0'
        )

        if current <= 0 then
            return 0
        end

        local new_value = current - 1

        redis.call(
            'SET',
            KEYS[1],
            new_value,
            'EX',
            172800
        )

        return new_value
        """

        return int(
            self.redis.eval(
                lua,
                1,
                key,
            )
        )

    def pick_and_reserve(
        self,
        accounts: list[SideaAccount],
    ) -> tuple[SideaAccount, int]:
        """
        Elige entre cuentas:
        - habilitadas
        - con sesión
        - READY
        - debajo de su límite

        Ordena por menor uso para repartir carga.
        La reserva final es atómica.
        """

        candidates = []

        for account in accounts:
            if not account.enabled:
                continue

            status = self.get_status(
                account.key
            )

            try:
                self.get_session(
                    account.key
                )
            except SideaNeedLogin:
                continue

            if status not in {
                "READY",
                "UNKNOWN",
            }:
                continue

            candidates.append(
                (
                    self.usage(account.key),
                    account.key,
                    account,
                )
            )

        candidates.sort(
            key=lambda item: (
                item[0],
                item[1],
            )
        )

        for _, _, account in candidates:
            reserved = self.reserve_one(
                account
            )

            if reserved is not None:
                return account, reserved

        raise SideaNoReadyAccount(
            "SIDEA_NO_READY_ACCOUNT"
        )


# ============================================================
# REVERSO POR ENTIDAD
# ============================================================

SIDEA_ENTITY_CODE_TO_ASSET = {
    "01": "AGUASCALIENTES",
    "02": "BAJA_CALIFORNIA",
    "03": "BAJA_CALIFORNIA_SUR",
    "04": "CAMPECHE",
    "05": "COAHUILA",
    "06": "COLIMA",
    "07": "CHIAPAS",
    "08": "CHIHUAHUA",
    "09": "CIUDAD_DE_MEXICO",
    "10": "DURANGO",
    "11": "GUANAJUATO",
    "12": "GUERRERO",
    "13": "HIDALGO",
    "14": "JALISCO",
    "15": "MEXICO",
    "16": "MICHOACAN",
    "17": "MORELOS",
    "18": "NAYARIT",
    "19": "NUEVO_LEON",
    "20": "OAXACA",
    "21": "PUEBLA",
    "22": "QUERETARO",
    "23": "QUINTANA_ROO",
    "24": "SAN_LUIS_POTOSI",
    "25": "SINALOA",
    "26": "SONORA",
    "27": "TABASCO",
    "28": "TAMAULIPAS",
    "29": "TLAXCALA",
    "30": "VERACRUZ",
    "31": "YUCATAN",
    "32": "ZACATECAS",
}


SIDEA_ENTITY_ALIASES = {
    "ESTADO_DE_MEXICO": "MEXICO",
    "MEXICO_ESTADO_DE": "MEXICO",
    "MICHOACAN_DE_OCAMPO": "MICHOACAN",
    "VERACRUZ_DE_IGNACIO_DE_LA_LLAVE": "VERACRUZ",
    "COAHUILA_DE_ZARAGOZA": "COAHUILA",
    "CIUDAD_DE_MEXICO": "CIUDAD_DE_MEXICO",
    "DISTRITO_FEDERAL": "CIUDAD_DE_MEXICO",
    "NUEVO_LEON": "NUEVO_LEON",
}


def _normalize_text(value: Any) -> str:
    text = str(
        value or ""
    ).strip().upper()

    text = unicodedata.normalize(
        "NFKD",
        text,
    )

    text = "".join(
        c
        for c in text
        if not unicodedata.combining(c)
    )

    text = re.sub(
        r"[^A-Z0-9]+",
        "_",
        text,
    )

    return text.strip("_")


def sidea_entity_to_asset(
    entity: Any,
) -> str:

    raw = str(
        entity or ""
    ).strip()

    if not raw:
        raise SideaPdfError(
            "SIDEA_EMPTY_REGISTRATION_ENTITY"
        )

    if raw.isdigit():
        code = raw.zfill(2)

        asset = (
            SIDEA_ENTITY_CODE_TO_ASSET
            .get(code)
        )

        if not asset:
            raise SideaPdfError(
                f"SIDEA_UNKNOWN_ENTITY_CODE:{raw}"
            )

        return asset

    normalized = _normalize_text(
        raw
    )

    normalized = (
        SIDEA_ENTITY_ALIASES
        .get(
            normalized,
            normalized,
        )
    )

    if normalized in set(
        SIDEA_ENTITY_CODE_TO_ASSET.values()
    ):
        return normalized

    raise SideaPdfError(
        f"SIDEA_UNKNOWN_ENTITY_NAME:"
        f"{raw}:{normalized}"
    )


def sidea_rear_path(
    entity: Any,
) -> Path:

    asset = sidea_entity_to_asset(
        entity
    )

    app_dir = (
        Path(__file__)
        .resolve()
        .parent
        .parent
    )

    estados_dir = (
        app_dir
        / "assets"
        / "estados"
    )

    path = estados_dir / f"{asset}.pdf"

    if not path.exists():
        raise SideaPdfError(
            f"SIDEA_SECOND_PAGE_NOT_FOUND:"
            f"{asset}:{path}"
        )

    return path


def append_sidea_rear(
    pdf_bytes: bytes,
    registration_entity: Any,
) -> bytes:
    """
    SIDEA Papel Bond:
    - esperamos exactamente 1 página frontal
    - agregamos reverso local según ENTIDAD REGISTRAL
    - resultado obligatorio: 2 páginas
    """

    if not pdf_bytes:
        raise SideaPdfError(
            "SIDEA_EMPTY_PDF"
        )

    try:
        front_reader = PdfReader(
            BytesIO(pdf_bytes)
        )
    except Exception as exc:
        raise SideaPdfError(
            f"SIDEA_INVALID_PDF:{exc}"
        ) from exc

    if len(front_reader.pages) != 1:
        raise SideaPdfError(
            "SIDEA_UNEXPECTED_ORIGINAL_PAGE_COUNT:"
            f"{len(front_reader.pages)}"
        )

    rear_path = sidea_rear_path(
        registration_entity
    )

    try:
        rear_reader = PdfReader(
            str(rear_path)
        )
    except Exception as exc:
        raise SideaPdfError(
            f"SIDEA_REAR_INVALID:"
            f"{rear_path}:{exc}"
        ) from exc

    if len(rear_reader.pages) != 1:
        raise SideaPdfError(
            "SIDEA_REAR_UNEXPECTED_PAGE_COUNT:"
            f"{rear_path}:"
            f"{len(rear_reader.pages)}"
        )

    writer = PdfWriter()

    writer.add_page(
        front_reader.pages[0]
    )

    writer.add_page(
        rear_reader.pages[0]
    )

    output = BytesIO()
    writer.write(output)

    result = output.getvalue()

    check = PdfReader(
        BytesIO(result)
    )

    if len(check.pages) != 2:
        raise SideaPdfError(
            "SIDEA_FINAL_PDF_NOT_TWO_PAGES"
        )

    return result


def sidea_asset_audit() -> dict:
    """
    Verifica los 32 reversos esperados.
    """

    available = []
    missing = []

    for code, asset in (
        SIDEA_ENTITY_CODE_TO_ASSET.items()
    ):
        try:
            path = sidea_rear_path(
                code
            )
            available.append(
                str(path)
            )
        except SideaPdfError:
            missing.append(
                {
                    "code": code,
                    "asset": asset,
                }
            )

    return {
        "expected": 32,
        "available": len(available),
        "missing": missing,
    }


# ============================================================
# SIDEA_SEARCH_CURP_IMPLEMENTATION_V1
# ============================================================

from html.parser import HTMLParser


class SideaNoRecord(SideaError):
    pass


class _SideaFormsParser(HTMLParser):

    def __init__(self):
        super().__init__(
            convert_charrefs=True
        )

        self.forms = []
        self.current_form = None

    @staticmethod
    def _attrs_dict(attrs):
        return {
            str(k or "").lower(): (
                "" if v is None else str(v)
            )
            for k, v in attrs
        }

    def handle_starttag(
        self,
        tag,
        attrs,
    ):
        tag = str(tag or "").lower()

        attrs_dict = self._attrs_dict(
            attrs
        )

        if tag == "form":

            self.current_form = {
                "action": (
                    attrs_dict.get("action")
                    or ""
                ),
                "method": (
                    attrs_dict.get("method")
                    or ""
                ).upper(),
                "inputs": [],
            }

            self.forms.append(
                self.current_form
            )

            return

        if (
            tag == "input"
            and self.current_form is not None
        ):

            name = (
                attrs_dict.get("name")
                or ""
            ).strip()

            if not name:
                return

            self.current_form[
                "inputs"
            ].append(
                {
                    "name": name,
                    "value": (
                        attrs_dict.get(
                            "value"
                        )
                        or ""
                    ),
                    "type": (
                        attrs_dict.get(
                            "type"
                        )
                        or "text"
                    ).lower(),
                    "checked": (
                        "checked"
                        in attrs_dict
                    ),
                }
            )

    def handle_endtag(
        self,
        tag,
    ):
        if str(tag or "").lower() == "form":
            self.current_form = None


def _sidea_parse_forms(
    html: str,
) -> list[dict]:

    parser = _SideaFormsParser()

    try:
        parser.feed(
            html or ""
        )
    except Exception as exc:
        raise SideaError(
            f"SIDEA_HTML_PARSE_FAILED:{exc}"
        ) from exc

    return parser.forms


def _sidea_find_form(
    html: str,
    action_suffix: str,
) -> dict | None:

    target = (
        action_suffix
        or ""
    ).strip().lower()

    for form in _sidea_parse_forms(
        html
    ):

        action = (
            form.get("action")
            or ""
        ).strip().lower()

        if action.endswith(
            target
        ):
            return form

    return None


def _sidea_form_values(
    form: dict,
) -> dict[str, str]:
    """
    Convierte los inputs del form a diccionario.

    Si hay radios repetidos:
    - prioriza el checked
    - si ninguno está checked conserva
      el primero.

    Para los campos críticos de impresión
    luego fijaremos explícitamente:
      formato=1
      impresiones=1
      dialecto=1
    """

    values = {}

    checked_values = {}

    for item in (
        form.get("inputs")
        or []
    ):

        name = (
            item.get("name")
            or ""
        ).strip()

        if not name:
            continue

        value = str(
            item.get("value")
            or ""
        )

        input_type = (
            item.get("type")
            or ""
        ).lower()

        checked = bool(
            item.get("checked")
        )

        if input_type in {
            "radio",
            "checkbox",
        }:

            if checked:
                checked_values[
                    name
                ] = value

            if name not in values:
                values[
                    name
                ] = value

            continue

        values[
            name
        ] = value

    values.update(
        checked_values
    )

    return values


def _sidea_html_is_authenticated(
    html: str,
) -> bool:

    html = html or ""
    lower = html.lower()

    # ========================================================
    # SESION EXPIRADA
    #
    # SIDEA muestra logOutAction.do también en la página
    # "Sesión finalizada", por lo que NO basta con detectar
    # logoutAction.do para considerar la sesión autenticada.
    # ========================================================

    expired_signals = (
        "sesi&oacute;n finalizada",
        "sesión finalizada",
        "sesion finalizada",
        "ha finalizado debido",
        "tiempo de inactiv",
        "acceder nuevamente",
    )

    if any(
        signal in lower
        for signal in expired_signals
    ):
        return False

    # Página normal de login.
    login_form = (
        "autenticacion.do"
        in lower
        and (
            "contrasenia"
            in lower
            or "contrase&ntilde;a"
            in lower
            or "contraseña"
            in lower
        )
    )

    if login_form:
        return False

    # Página autenticada normal.
    has_logout = (
        "logoutaction.do"
        in lower
    )

    return bool(has_logout)


def _sidea_safe_cookie_dict(
    session: requests.Session,
) -> dict[str, str]:

    return {
        cookie.name: cookie.value
        for cookie in session.cookies
        if cookie.name
    }


def sidea_search_curp(
    pool: SideaPool,
    account_key: str,
    curp: str,
    entidad: str | int,
    acto: str | int = "1",
    tipo: str | int = "1",
) -> dict:
    """
    Consulta SIDEA por CURP.

    ESTA FUNCIÓN:
    - sí hace solicitudXCURP.do
    - NO hace solicitudImpresion.do
    - NO reserva consumo diario
    - NO genera impresión

    Retorna los datos del formulario que SIDEA
    usaría al presionar Generar Impresión.
    """

    account_key = (
        account_key
        or ""
    ).strip()

    curp = (
        curp
        or ""
    ).strip().upper()

    entidad = str(
        entidad
        or ""
    ).strip()

    acto = str(
        acto
        or "1"
    ).strip()

    tipo = str(
        tipo
        or "1"
    ).strip()

    if not account_key:
        raise SideaError(
            "SIDEA_EMPTY_ACCOUNT_KEY"
        )

    if not curp:
        raise SideaError(
            "SIDEA_EMPTY_CURP"
        )

    if not entidad:
        raise SideaError(
            "SIDEA_EMPTY_SEARCH_ENTITY"
        )

    session, state = (
        pool.build_http_session(
            account_key
        )
    )

    try:
        response = session.post(
            (
                f"{SIDEA_BASE_URL}"
                "/solicitudXCURP.do"
            ),
            data={
                "tipo": tipo,
                "acto": acto,
                "entidad": entidad,
                "curp": curp,
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
            allow_redirects=True,
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_SEARCH_CURP_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    html = response.text or ""

    if not _sidea_html_is_authenticated(
        html
    ):
        pool.clear_session(
            account_key,
            reason="NEED_LOGIN",
        )

        raise SideaNeedLogin(
            f"SIDEA_NEED_LOGIN:"
            f"{account_key}"
        )

    print_form = _sidea_find_form(
        html,
        "solicitudImpresion.do",
    )

    if not print_form:

        if (
            "no existe acto"
            in html.lower()
            or "no se encontr"
            in html.lower()
        ):
            raise SideaNoRecord(
                "SIDEA_NO_RECORD"
            )

        raise SideaError(
            "SIDEA_PRINT_FORM_NOT_FOUND"
        )

    values = _sidea_form_values(
        print_form
    )

    critical = {
        "cadena": (
            values.get("cadena")
            or ""
        ).strip(),
        "curp": (
            values.get("curp")
            or ""
        ).strip().upper(),
        "primerApellido": (
            values.get(
                "primerApellido"
            )
            or ""
        ).strip(),
        "nombre": (
            values.get("nombre")
            or ""
        ).strip(),
        "fechaNacimiento": (
            values.get(
                "fechaNacimiento"
            )
            or ""
        ).strip(),
        "sexo": (
            values.get("sexo")
            or ""
        ).strip(),
        "entidad": (
            values.get("entidad")
            or entidad
        ).strip(),
        "sessionId": (
            values.get("sessionId")
            or ""
        ).strip(),
        "usuario": (
            values.get("usuario")
            or ""
        ).strip(),
        "usuario_rol": (
            values.get(
                "usuario_rol"
            )
            or ""
        ).strip(),
        "usuario_entidad": (
            values.get(
                "usuario_entidad"
            )
            or ""
        ).strip(),
    }

    if not critical[
        "cadena"
    ]:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "EMPTY_CHAIN"
        )

    returned_curp = (
        critical["curp"]
    )

    if (
        returned_curp
        and returned_curp != curp
    ):
        raise SideaError(
            "SIDEA_SEARCH_CURP_MISMATCH"
        )

    if not critical[
        "sessionId"
    ]:
        raise SideaError(
            "SIDEA_SEARCH_MISSING_SESSIONID"
        )

    # Persistimos cualquier cookie renovada
    # y también los datos de sesión que SIDEA
    # acaba de exponer en el form.
    pool.save_session(
        account_key=account_key,
        cookies=(
            _sidea_safe_cookie_dict(
                session
            )
        ),
        session_id=(
            critical["sessionId"]
        ),
        usuario=(
            critical["usuario"]
        ),
        usuario_rol=(
            critical["usuario_rol"]
        ),
        usuario_entidad=(
            critical[
                "usuario_entidad"
            ]
        ),
    )

    return {
        "account_key": (
            account_key
        ),
        "requested_curp": (
            curp
        ),
        "returned_curp": (
            returned_curp
        ),
        "cadena": (
            critical["cadena"]
        ),
        "primer_apellido": (
            critical[
                "primerApellido"
            ]
        ),
        "segundo_apellido": (
            values.get(
                "segundoApellido"
            )
            or ""
        ).strip(),
        "nombre": (
            critical["nombre"]
        ),
        "fecha_nacimiento": (
            critical[
                "fechaNacimiento"
            ]
        ),
        "sexo": (
            critical["sexo"]
        ),
        "entidad": (
            critical["entidad"]
        ),
        "session_id": (
            critical["sessionId"]
        ),
        "usuario": (
            critical["usuario"]
        ),
        "usuario_rol": (
            critical["usuario_rol"]
        ),
        "usuario_entidad": (
            critical[
                "usuario_entidad"
            ]
        ),
        "acto": (
            values.get("acto")
            or acto
        ),
        "tipo": (
            values.get("tipo")
            or tipo
        ),
        "print_form_values": values,
    }


# ============================================================
# SIDEA_SEARCH_CURP_IMPLEMENTATION_V2
# Lee el dataset JS que llena solicitudImpresion.do
# ============================================================

import ast


def _sidea_extract_balanced_array(
    text: str,
    start: int,
) -> tuple[str, int]:
    """
    Extrae un array JS/JSON desde '[' hasta su cierre,
    respetando strings y escapes.
    """

    if start < 0 or start >= len(text):
        raise SideaError(
            "SIDEA_JS_ARRAY_BAD_START"
        )

    if text[start] != "[":
        raise SideaError(
            "SIDEA_JS_ARRAY_START_NOT_BRACKET"
        )

    depth = 0
    quote = None
    escaped = False

    for pos in range(start, len(text)):

        ch = text[pos]

        if quote is not None:

            if escaped:
                escaped = False
                continue

            if ch == "\\":
                escaped = True
                continue

            if ch == quote:
                quote = None

            continue

        if ch in {"'", '"'}:
            quote = ch
            continue

        if ch == "[":
            depth += 1
            continue

        if ch == "]":
            depth -= 1

            if depth == 0:
                return (
                    text[start:pos + 1],
                    pos + 1,
                )

    raise SideaError(
        "SIDEA_JS_ARRAY_UNCLOSED"
    )


def _sidea_parse_js_array(
    raw: str,
):
    """
    Primero JSON normal.
    Fallback a literal_eval para arrays JS simples
    con comillas simples.
    """

    raw = (
        raw
        or ""
    ).strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    normalized = re.sub(
        r"\bnull\b",
        "None",
        raw,
        flags=re.I,
    )

    normalized = re.sub(
        r"\btrue\b",
        "True",
        normalized,
        flags=re.I,
    )

    normalized = re.sub(
        r"\bfalse\b",
        "False",
        normalized,
        flags=re.I,
    )

    try:
        return ast.literal_eval(
            normalized
        )
    except Exception as exc:
        raise SideaError(
            "SIDEA_JS_DATA_PARSE_FAILED"
        ) from exc


def _sidea_extract_datasets(
    html: str,
) -> list[dict]:
    """
    Busca estructuras como:

        fields : [
            {name:'CAMPO1'},
            {name:'CAMPO2'}
        ],
        recordType : 'array',
        data : [[...],[...]]

    No depende del nombre de la variable JS.
    """

    html = html or ""

    datasets = []

    for match in re.finditer(
        r"\bdata\s*:",
        html,
        flags=re.I,
    ):

        bracket_pos = html.find(
            "[",
            match.end(),
        )

        if bracket_pos < 0:
            continue

        # Evitar brincar a otro bloque lejano.
        if (
            bracket_pos
            - match.end()
            > 500
        ):
            continue

        try:
            raw_array, _ = (
                _sidea_extract_balanced_array(
                    html,
                    bracket_pos,
                )
            )

            rows = _sidea_parse_js_array(
                raw_array
            )

        except Exception:
            continue

        if not isinstance(
            rows,
            list,
        ):
            continue

        # El "fields" correspondiente debe estar
        # antes del data actual.
        prefix_start = max(
            0,
            match.start() - 15000,
        )

        prefix = html[
            prefix_start:
            match.start()
        ]

        fields_pos = max(
            prefix.lower().rfind(
                "fields :"
            ),
            prefix.lower().rfind(
                "fields:"
            ),
        )

        if fields_pos < 0:
            continue

        fields_section = prefix[
            fields_pos:
        ]

        field_names = re.findall(
            r"""
            \bname\s*:\s*
            ['"]([^'"]+)['"]
            """,
            fields_section,
            flags=re.I | re.X,
        )

        if not field_names:
            continue

        datasets.append(
            {
                "fields": field_names,
                "rows": rows,
            }
        )

    return datasets


def _sidea_record_dict(
    fields: list,
    row: list,
) -> dict[str, Any]:

    result = {}

    for idx, field in enumerate(
        fields
    ):
        if idx >= len(row):
            break

        result[
            str(field).strip().upper()
        ] = row[idx]

    return result


def _sidea_find_curp_record(
    html: str,
    requested_curp: str,
) -> dict:
    """
    Encuentra la fila EXACTA de la CURP solicitada
    dentro de cualquiera de los datasets SIDEA.
    """

    requested = (
        requested_curp
        or ""
    ).strip().upper()

    datasets = (
        _sidea_extract_datasets(
            html
        )
    )

    for dataset in datasets:

        fields = (
            dataset.get("fields")
            or []
        )

        rows = (
            dataset.get("rows")
            or []
        )

        for row in rows:

            if not isinstance(
                row,
                (list, tuple),
            ):
                continue

            record = (
                _sidea_record_dict(
                    fields,
                    list(row),
                )
            )

            candidate_values = {
                str(value or "")
                .strip()
                .upper()
                for value in record.values()
            }

            if requested in candidate_values:
                return record

    raise SideaNoRecord(
        "SIDEA_NO_RECORD:"
        "CURP_NOT_IN_DATASET"
    )


def _sidea_record_value(
    record: dict,
    *aliases: str,
) -> str:

    for alias in aliases:

        key = (
            alias
            or ""
        ).strip().upper()

        if not key:
            continue

        value = record.get(
            key
        )

        if value is None:
            continue

        text = str(
            value
        ).strip()

        if text:
            return text

    return ""


def sidea_search_curp(
    pool: SideaPool,
    account_key: str,
    curp: str,
    entidad: str | int,
    acto: str | int = "1",
    tipo: str | int = "1",
) -> dict:
    """
    V2:

    solicitudXCURP.do
        ↓
    dataset JavaScript
        ↓
    match EXACTO por CURP
        ↓
    llenar valores que usa solicitudImpresion.do

    NO genera impresión.
    NO incrementa contador.
    """

    account_key = (
        account_key
        or ""
    ).strip()

    curp = (
        curp
        or ""
    ).strip().upper()

    entidad = str(
        entidad
        or ""
    ).strip()

    acto = str(
        acto
        or "1"
    ).strip()

    tipo = str(
        tipo
        or "1"
    ).strip()

    if not account_key:
        raise SideaError(
            "SIDEA_EMPTY_ACCOUNT_KEY"
        )

    if not curp:
        raise SideaError(
            "SIDEA_EMPTY_CURP"
        )

    if not entidad:
        raise SideaError(
            "SIDEA_EMPTY_SEARCH_ENTITY"
        )

    session, state = (
        pool.build_http_session(
            account_key
        )
    )

    try:
        response = session.post(
            (
                f"{SIDEA_BASE_URL}"
                "/solicitudXCURP.do"
            ),
            data={
                "tipo": tipo,
                "acto": acto,
                "entidad": entidad,
                "curp": curp,
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
            allow_redirects=True,
        )

    except requests.RequestException as exc:

        raise SideaError(
            "SIDEA_SEARCH_CURP_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    html = response.text or ""

    if not _sidea_html_is_authenticated(
        html
    ):
        pool.clear_session(
            account_key,
            reason="NEED_LOGIN",
        )

        raise SideaNeedLogin(
            f"SIDEA_NEED_LOGIN:"
            f"{account_key}"
        )

    print_form = _sidea_find_form(
        html,
        "solicitudImpresion.do",
    )

    if not print_form:
        raise SideaError(
            "SIDEA_PRINT_FORM_NOT_FOUND"
        )

    # Valores persistentes del formulario:
    # sessionId, usuario, rol, etc.
    form_values = (
        _sidea_form_values(
            print_form
        )
    )

    # Datos reales del registro encontrado.
    record = (
        _sidea_find_curp_record(
            html,
            curp,
        )
    )

    returned_curp = (
        _sidea_record_value(
            record,
            "CURP",
            "TA07_E_CURP",
        )
        .strip()
        .upper()
    )

    cadena = _sidea_record_value(
        record,
        "CADENA",
        "TA07_C_CADENA",
        "CADENA_DIGITAL",
    )

    primer_apellido = (
        _sidea_record_value(
            record,
            "PRIMERAPELLIDO",
            "PRIMER_APELLIDO",
            "APELLIDOPATERNO",
            "APELLIDO_PATERNO",
            "TA07_C_PATERNO",
        )
    )

    segundo_apellido = (
        _sidea_record_value(
            record,
            "SEGUNDOAPELLIDO",
            "SEGUNDO_APELLIDO",
            "APELLIDOMATERNO",
            "APELLIDO_MATERNO",
            "TA07_C_MATERNO",
        )
    )

    nombre = _sidea_record_value(
        record,
        "NOMBRE",
        "NOMBRES",
        "TA07_C_NOMBRES",
    )

    fecha_nacimiento = (
        _sidea_record_value(
            record,
            "FECHA_NACIMIENTO",
            "FECHANACIMIENTO",
            "TA07_F_NACIMIENTO",
        )
    )

    sexo = _sidea_record_value(
        record,
        "SEXO",
        "TA07_C_SEXO",
    )

    entidad_record = (
        _sidea_record_value(
            record,
            "ENTIDAD",
            "ENTIDAD_REGISTRO",
            "TA07_E_ESTADODEST",
        )
        or entidad
    )

    if not returned_curp:
        raise SideaError(
            "SIDEA_SEARCH_RESULT_MISSING_CURP"
        )

    if returned_curp != curp:
        raise SideaError(
            "SIDEA_SEARCH_CURP_MISMATCH"
        )

    if not cadena:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "DATASET_EMPTY_CHAIN"
        )

    session_id = (
        form_values.get(
            "sessionId"
        )
        or state.get(
            "session_id"
        )
        or ""
    ).strip()

    usuario = (
        form_values.get("usuario")
        or state.get("usuario")
        or ""
    ).strip()

    usuario_rol = (
        form_values.get(
            "usuario_rol"
        )
        or state.get(
            "usuario_rol"
        )
        or ""
    ).strip()

    usuario_entidad = (
        form_values.get(
            "usuario_entidad"
        )
        or state.get(
            "usuario_entidad"
        )
        or ""
    ).strip()

    if not session_id:
        raise SideaError(
            "SIDEA_SEARCH_MISSING_SESSIONID"
        )

    # Recreamos EXACTAMENTE los valores que
    # el JS del navegador colocaría en el form.
    form_values.update(
        {
            "tipo": (
                form_values.get("tipo")
                or tipo
            ),
            "acto": (
                form_values.get("acto")
                or acto
            ),
            "cadena": cadena,
            "curp": returned_curp,
            "primerApellido": (
                primer_apellido
            ),
            "segundoApellido": (
                segundo_apellido
            ),
            "nombre": nombre,
            "fechaNacimiento": (
                fecha_nacimiento
            ),
            "sexo": sexo,
            "entidad": (
                entidad_record
            ),
            "sessionId": (
                session_id
            ),
            "usuario": usuario,
            "usuario_rol": (
                usuario_rol
            ),
            "usuario_entidad": (
                usuario_entidad
            ),
        }
    )

    # Persistimos cookies renovadas.
    pool.save_session(
        account_key=account_key,
        cookies=(
            _sidea_safe_cookie_dict(
                session
            )
        ),
        session_id=session_id,
        usuario=usuario,
        usuario_rol=usuario_rol,
        usuario_entidad=(
            usuario_entidad
        ),
    )

    return {
        "account_key": account_key,
        "requested_curp": curp,
        "returned_curp": (
            returned_curp
        ),
        "cadena": cadena,
        "primer_apellido": (
            primer_apellido
        ),
        "segundo_apellido": (
            segundo_apellido
        ),
        "nombre": nombre,
        "fecha_nacimiento": (
            fecha_nacimiento
        ),
        "sexo": sexo,
        "entidad": (
            entidad_record
        ),
        "session_id": (
            session_id
        ),
        "usuario": usuario,
        "usuario_rol": (
            usuario_rol
        ),
        "usuario_entidad": (
            usuario_entidad
        ),
        "acto": acto,
        "tipo": tipo,
        "print_form_values": (
            form_values
        ),

        # Solo nombres de campos para debug.
        # NO imprimimos datos personales.
        "dataset_fields": sorted(
            record.keys()
        ),
    }


# ============================================================
# SIDEA_SEARCH_CURP_IMPLEMENTATION_V3
# FUENTE REAL: hidden inputs de la fila encontrada
# ============================================================


class _SideaHiddenInputParser(HTMLParser):

    def __init__(self):
        super().__init__(
            convert_charrefs=True
        )
        self.values = {}

    def handle_starttag(
        self,
        tag,
        attrs,
    ):
        if str(tag or "").lower() != "input":
            return

        attrs_dict = {
            str(k or "").lower(): (
                "" if v is None else str(v)
            )
            for k, v in attrs
        }

        input_type = (
            attrs_dict.get("type")
            or ""
        ).strip().lower()

        if input_type != "hidden":
            return

        name = (
            attrs_dict.get("name")
            or ""
        ).strip()

        if not name:
            return

        value = (
            attrs_dict.get("value")
            or ""
        )

        # La misma fila puede tener nombres repetidos
        # en casos especiales. Conservamos el primero
        # no vacío.
        if (
            name not in self.values
            or (
                not self.values[name]
                and value
            )
        ):
            self.values[name] = value


def _sidea_find_matching_row_html(
    html: str,
    curp: str,
) -> str:

    requested = (
        curp
        or ""
    ).strip().upper()

    if not requested:
        raise SideaError(
            "SIDEA_EMPTY_CURP_FOR_ROW_MATCH"
        )

    rows = re.findall(
        r"<tr\b[^>]*>.*?</tr>",
        html or "",
        flags=re.I | re.S,
    )

    matched = [
        row
        for row in rows
        if requested.lower()
        in row.lower()
    ]

    if not matched:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "CURP_ROW_NOT_FOUND"
        )

    if len(matched) > 1:
        # No escoger a ciegas una fila ambigua.
        exact = []

        for row in matched:
            parser = (
                _SideaHiddenInputParser()
            )
            parser.feed(row)

            hidden_curp = (
                parser.values.get("curp")
                or ""
            ).strip().upper()

            if hidden_curp == requested:
                exact.append(row)

        if len(exact) == 1:
            return exact[0]

        raise SideaError(
            "SIDEA_AMBIGUOUS_CURP_ROWS:"
            f"{len(matched)}"
        )

    return matched[0]


def _sidea_hidden_values_from_row(
    row_html: str,
) -> dict[str, str]:

    parser = (
        _SideaHiddenInputParser()
    )

    try:
        parser.feed(
            row_html or ""
        )
    except Exception as exc:
        raise SideaError(
            "SIDEA_ROW_HIDDEN_PARSE_FAILED"
        ) from exc

    return {
        str(k): str(v or "")
        for k, v
        in parser.values.items()
    }


def sidea_search_curp(
    pool: SideaPool,
    account_key: str,
    curp: str,
    entidad: str | int,
    acto: str | int = "1",
    tipo: str | int = "1",
) -> dict:
    """
    V3 definitiva.

    Hace:
        POST solicitudXCURP.do
        -> encuentra TR exacto por CURP
        -> lee hidden inputs reales del registro
        -> prepara datos para impresión

    NO hace solicitudImpresion.do.
    NO consume contador diario.
    """

    account_key = (
        account_key
        or ""
    ).strip()

    curp = (
        curp
        or ""
    ).strip().upper()

    entidad = str(
        entidad
        or ""
    ).strip()

    acto = str(
        acto
        or "1"
    ).strip()

    tipo = str(
        tipo
        or "1"
    ).strip()

    if not account_key:
        raise SideaError(
            "SIDEA_EMPTY_ACCOUNT_KEY"
        )

    if not curp:
        raise SideaError(
            "SIDEA_EMPTY_CURP"
        )

    if not entidad:
        raise SideaError(
            "SIDEA_EMPTY_SEARCH_ENTITY"
        )

    session, state = (
        pool.build_http_session(
            account_key
        )
    )

    try:
        response = session.post(
            (
                f"{SIDEA_BASE_URL}"
                "/solicitudXCURP.do"
            ),
            data={
                "tipo": tipo,
                "acto": acto,
                "entidad": entidad,
                "curp": curp,
            },
            timeout=(
                SIDEA_HTTP_CONNECT_TIMEOUT,
                SIDEA_HTTP_READ_TIMEOUT,
            ),
            allow_redirects=True,
        )

    except requests.RequestException as exc:
        raise SideaError(
            "SIDEA_SEARCH_CURP_HTTP_ERROR:"
            f"{type(exc).__name__}"
        ) from exc

    html = response.text or ""

    if not _sidea_html_is_authenticated(
        html
    ):
        pool.clear_session(
            account_key,
            reason="NEED_LOGIN",
        )

        raise SideaNeedLogin(
            f"SIDEA_NEED_LOGIN:"
            f"{account_key}"
        )

    # ========================================================
    # PRIMERO VALIDAR QUE EXISTA FILA EXACTA DEL CURP
    #
    # Si la entidad no contiene el registro:
    # _sidea_find_matching_row_html() lanza SideaNoRecord.
    #
    # Solo después se valida el formulario de impresión.
    # ========================================================

    row_html = (
        _sidea_find_matching_row_html(
            html,
            curp,
        )
    )

    # ========================================================
    # FORM GENERAL DE IMPRESIÓN:
    # sessionId / usuario / rol / entidad usuario
    # ========================================================

    print_form = _sidea_find_form(
        html,
        "solicitudImpresion.do",
    )

    if not print_form:
        raise SideaError(
            "SIDEA_PRINT_FORM_NOT_FOUND"
        )

    form_values = (
        _sidea_form_values(
            print_form
        )
    )

    # ========================================================
    # FILA REAL DEL ACTA
    # ========================================================

    # row_html ya fue validado antes de revisar
    # el formulario general de impresión.

    hidden = (
        _sidea_hidden_values_from_row(
            row_html
        )
    )

    returned_curp = (
        hidden.get("curp")
        or ""
    ).strip().upper()

    cadena = (
        hidden.get("cadena")
        or ""
    ).strip()

    primer_apellido = (
        hidden.get("primerApellido")
        or ""
    ).strip()

    segundo_apellido = (
        hidden.get("segundoApellido")
        or ""
    ).strip()

    nombre = (
        hidden.get("nombre")
        or ""
    ).strip()

    fecha_nacimiento = (
        hidden.get("fnacim")
        or hidden.get(
            "fechaNacimiento"
        )
        or ""
    ).strip()

    # MUY IMPORTANTE:
    # usamos el valor interno SIDEA.
    # NO convertimos visualmente HOMBRE/MUJER.
    raw_sexo = (
        hidden.get("sexo")
        or ""
    ).strip().upper()

    sexo_map = {
        "HOMBRE": "M",
        "MASCULINO": "M",
        "H": "M",
        "M": "M",
        "MUJER": "F",
        "FEMENINO": "F",
        "F": "F",
    }

    sexo = (
        sexo_map.get(raw_sexo)
        or raw_sexo
    )

    entidad_record = (
        hidden.get("entidad")
        or entidad
    ).strip()

    # El valor que se envía a solicitudImpresion.do
    # es el tipo de acto solicitado, no metadata libre
    # tomada de otro control del HTML.
    acto_record = str(
        acto
        or hidden.get("acto")
        or "1"
    ).strip()

    # En la fila SIDEA aparece tipo=0,
    # pero el formulario real de impresión usa tipo=1.
    tipo_record = str(
        tipo
        or "1"
    ).strip()

    if not returned_curp:
        raise SideaError(
            "SIDEA_RESULT_MISSING_CURP"
        )

    if returned_curp != curp:
        raise SideaError(
            "SIDEA_SEARCH_CURP_MISMATCH"
        )

    if not cadena:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "ROW_EMPTY_CHAIN"
        )

    if not nombre:
        raise SideaError(
            "SIDEA_RESULT_MISSING_NAME"
        )

    if not primer_apellido:
        raise SideaError(
            "SIDEA_RESULT_MISSING_FIRST_LASTNAME"
        )

    if not fecha_nacimiento:
        raise SideaError(
            "SIDEA_RESULT_MISSING_BIRTH_DATE"
        )

    if not sexo:
        raise SideaError(
            "SIDEA_RESULT_MISSING_SEX"
        )

    # ========================================================
    # DATOS DE SESIÓN
    # ========================================================

    session_id = (
        form_values.get(
            "sessionId"
        )
        or state.get(
            "session_id"
        )
        or ""
    ).strip()

    usuario = (
        form_values.get("usuario")
        or state.get("usuario")
        or ""
    ).strip()

    usuario_rol = (
        form_values.get(
            "usuario_rol"
        )
        or state.get(
            "usuario_rol"
        )
        or ""
    ).strip()

    usuario_entidad = (
        form_values.get(
            "usuario_entidad"
        )
        or state.get(
            "usuario_entidad"
        )
        or ""
    ).strip()

    if not session_id:
        raise SideaError(
            "SIDEA_SEARCH_MISSING_SESSIONID"
        )

    # ========================================================
    # PAYLOAD BASE DE IMPRESIÓN
    #
    # Aún NO se envía.
    # ========================================================

    print_payload = {
        "tipo": tipo_record,
        "acto": acto_record,
        "cadena": cadena,
        "curp": returned_curp,
        "primerApellido": (
            primer_apellido
        ),
        "segundoApellido": (
            segundo_apellido
        ),
        "nombre": nombre,
        "fechaNacimiento": (
            fecha_nacimiento
        ),
        "sexo": sexo,
        "entidad": (
            entidad_record
        ),
        "usuario": usuario,
        "usuario_rol": (
            usuario_rol
        ),
        "usuario_entidad": (
            usuario_entidad
        ),
        "sessionId": (
            session_id
        ),

        # Configuración obligatoria que queremos:
        # Papel Bond + una sola impresión.
        "formato": "1",
        "impresiones": "1",
        "folioHacienda": "",
        "folioControl": "",
        "dialecto": "1",
    }

    # ========================================================
    # PERSISTENCIA DE SESIÓN RENOVADA
    # ========================================================

    pool.save_session(
        account_key=account_key,
        cookies=(
            _sidea_safe_cookie_dict(
                session
            )
        ),
        session_id=session_id,
        usuario=usuario,
        usuario_rol=usuario_rol,
        usuario_entidad=(
            usuario_entidad
        ),
    )

    return {
        "account_key": account_key,

        "requested_curp": curp,
        "returned_curp": (
            returned_curp
        ),

        "cadena": cadena,

        "primer_apellido": (
            primer_apellido
        ),
        "segundo_apellido": (
            segundo_apellido
        ),
        "nombre": nombre,

        "fecha_nacimiento": (
            fecha_nacimiento
        ),
        "sexo": sexo,

        "entidad": (
            entidad_record
        ),

        "municipio": (
            hidden.get("municipio")
            or ""
        ).strip(),

        "oficialia": (
            hidden.get("oficialia")
            or ""
        ).strip(),

        "anio_registro": (
            hidden.get("anio")
            or ""
        ).strip(),

        "numero_acta": (
            hidden.get("acta")
            or ""
        ).strip(),

        "registro": (
            hidden.get("registro")
            or ""
        ).strip(),

        "acto": acto_record,
        "tipo": tipo_record,

        "session_id": (
            session_id
        ),
        "usuario": usuario,
        "usuario_rol": (
            usuario_rol
        ),
        "usuario_entidad": (
            usuario_entidad
        ),

        "print_payload": (
            print_payload
        ),

        # Solo nombres para auditoría.
        "hidden_fields": sorted(
            hidden.keys()
        ),
    }


# ============================================================
# SIDEA_PRODUCTION_FLOW_V1
# Flujo completo:
# account pool -> lock -> search -> baseline -> reserve
# -> submit -> reconcile -> response -> PDF -> reverso
# ============================================================


class SideaSubmitUncertain(SideaError):
    pass


class SideaResponseTimeout(SideaError):
    pass


class SideaBusy(SideaError):
    pass


def _sidea_prod_lock_key(
    account_key: str,
) -> str:
    return (
        "provider16_sidea:"
        "account_lock:"
        f"{account_key}"
    )


def _sidea_prod_acquire_lock(
    pool: SideaPool,
    account_key: str,
    ttl_sec: int = 900,
) -> str | None:
    """
    Lock exclusivo por cuenta SIDEA.

    Evita que dos workers hagan simultáneamente:
      buscar -> submit -> monitoreo
    usando la misma sesión/cuenta.
    """

    import uuid

    token = uuid.uuid4().hex

    ok = pool.redis.set(
        _sidea_prod_lock_key(
            account_key
        ),
        token,
        nx=True,
        ex=int(ttl_sec),
    )

    if not ok:
        return None

    return token


def _sidea_prod_release_lock(
    pool: SideaPool,
    account_key: str,
    token: str,
) -> None:
    """
    Libera únicamente si seguimos siendo
    propietarios del lock.
    """

    lua = """
    if redis.call(
        'GET',
        KEYS[1]
    ) == ARGV[1] then

        return redis.call(
            'DEL',
            KEYS[1]
        )
    end

    return 0
    """

    try:
        pool.redis.eval(
            lua,
            1,
            _sidea_prod_lock_key(
                account_key
            ),
            token,
        )
    except Exception:
        pass


def _sidea_prod_extract_dataset(
    html: str,
    variable_name: str,
) -> list:
    """
    Extrae data:[...] de:
      var dsOption
      var dsOption2
    """

    variable_name = (
        variable_name
        or ""
    ).strip()

    if variable_name not in {
        "dsOption",
        "dsOption2",
    }:
        raise SideaError(
            "SIDEA_MONITOR_BAD_DATASET_NAME"
        )

    match = re.search(
        rf"""
        \bvar\s+
        {re.escape(variable_name)}
        \s*=
        """,
        html or "",
        flags=re.I | re.X,
    )

    if not match:
        return []

    if variable_name == "dsOption":
        next_pattern = (
            r"\bvar\s+colsOption\s*="
        )
    else:
        next_pattern = (
            r"\bvar\s+colsOption2\s*="
        )

    after = (
        html or ""
    )[match.end():]

    next_match = re.search(
        next_pattern,
        after,
        flags=re.I,
    )

    if next_match:
        block = after[
            :next_match.start()
        ]
    else:
        block = after[:30000]

    data_match = re.search(
        r"\bdata\s*:",
        block,
        flags=re.I,
    )

    if not data_match:
        return []

    start = block.find(
        "[",
        data_match.end(),
    )

    if start < 0:
        return []

    raw, _ = (
        _sidea_extract_balanced_array(
            block,
            start,
        )
    )

    rows = _sidea_parse_js_array(
        raw
    )

    if not isinstance(
        rows,
        list,
    ):
        return []

    return rows


def _sidea_prod_parse_monitor(
    html: str,
) -> dict:
    return {
        "petitions": (
            _sidea_prod_extract_dataset(
                html,
                "dsOption",
            )
        ),
        "responses": (
            _sidea_prod_extract_dataset(
                html,
                "dsOption2",
            )
        ),
    }


def _sidea_prod_petition_oids(
    rows: list,
    wanted_curp: str,
    wanted_chain: str,
) -> set[str]:

    wanted_curp = (
        wanted_curp
        or ""
    ).strip().upper()

    wanted_chain = (
        wanted_chain
        or ""
    ).strip()

    result: set[str] = set()

    # dsOption:
    # 0  TA07_E_OID
    # 1  TA07_E_CURP
    # ...
    # 11 TA07_C_CADENA

    for row in rows:

        if not isinstance(
            row,
            (list, tuple),
        ):
            continue

        if len(row) < 12:
            continue

        oid = str(
            row[0]
            or ""
        ).strip()

        row_curp = str(
            row[1]
            or ""
        ).strip().upper()

        row_chain = str(
            row[11]
            or ""
        ).strip()

        if (
            oid
            and row_curp == wanted_curp
            and row_chain == wanted_chain
        ):
            result.add(oid)

    return result


def _sidea_prod_response_oids(
    rows: list,
    wanted_curp: str,
    wanted_chain: str,
) -> set[str]:

    wanted_curp = (
        wanted_curp
        or ""
    ).strip().upper()

    wanted_chain = (
        wanted_chain
        or ""
    ).strip()

    result: set[str] = set()

    # dsOption2:
    # 0  CADENA
    # 1  CURP
    # ...
    # 10 TA10_E_OIDORIGEN

    for row in rows:

        if not isinstance(
            row,
            (list, tuple),
        ):
            continue

        if len(row) < 13:
            continue

        row_chain = str(
            row[0]
            or ""
        ).strip()

        row_curp = str(
            row[1]
            or ""
        ).strip().upper()

        petition_oid = str(
            row[10]
            or ""
        ).strip()

        if (
            petition_oid
            and row_curp == wanted_curp
            and row_chain == wanted_chain
        ):
            result.add(
                petition_oid
            )

    return result


def _sidea_prod_response_for_oid(
    rows: list,
    petition_oid: str,
) -> list | None:

    wanted = str(
        petition_oid
        or ""
    ).strip()

    if not wanted:
        return None

    for row in rows:

        if not isinstance(
            row,
            (list, tuple),
        ):
            continue

        if len(row) < 13:
            continue

        current = str(
            row[10]
            or ""
        ).strip()

        if current == wanted:
            return list(row)

    return None


def _sidea_prod_monitor_get(
    session,
) -> str:

    response = session.get(
        (
            f"{SIDEA_BASE_URL}"
            "/monitoreo.do"
        ),
        timeout=(
            SIDEA_HTTP_CONNECT_TIMEOUT,
            SIDEA_HTTP_READ_TIMEOUT,
        ),
        allow_redirects=True,
    )

    response.raise_for_status()

    html = response.text or ""

    if not _sidea_html_is_authenticated(
        html
    ):
        raise SideaNeedLogin(
            "SIDEA_NEED_LOGIN:"
            "MONITOR"
        )

    return html


def _sidea_prod_candidate_accounts(
    pool: SideaPool,
    accounts: list[SideaAccount],
) -> list[SideaAccount]:
    """
    READY/UNKNOWN + sesión presente + menor uso.
    Todavía NO reserva cuota.
    """

    candidates = []

    for account in accounts:

        if not account.enabled:
            continue

        status = pool.get_status(
            account.key
        )

        if status not in {
            "READY",
            "UNKNOWN",
        }:
            continue

        try:
            pool.get_session(
                account.key
            )
        except SideaNeedLogin:
            continue

        usage = pool.usage(
            account.key
        )

        if usage >= int(
            account.daily_limit
        ):
            continue

        candidates.append(
            (
                usage,
                account.key,
                account,
            )
        )

    candidates.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )

    return [
        item[2]
        for item in candidates
    ]



# ============================================================
# SIDEA_AUTO_ENTITY_RESOLUTION_V1
# ============================================================

SIDEA_CURP_ENTITY_TO_SIDEA = {
    "AS": "01",
    "BC": "02",
    "BS": "03",
    "CC": "04",
    "CL": "05",
    "CM": "06",
    "CS": "07",
    "CH": "08",
    "DF": "09",
    "DG": "10",
    "GT": "11",
    "GR": "12",
    "HG": "13",
    "JC": "14",
    "MC": "15",
    "MN": "16",
    "MS": "17",
    "NT": "18",
    "NL": "19",
    "OC": "20",
    "PL": "21",
    "QT": "22",
    "QR": "23",
    "SP": "24",
    "SL": "25",
    "SR": "26",
    "TC": "27",
    "TS": "28",
    "TL": "29",
    "VZ": "30",
    "YN": "31",
    "ZS": "32",
}

SIDEA_ALL_ENTITY_CODES = tuple(
    f"{number:02d}"
    for number in range(1, 33)
)


def sidea_curp_birth_entity(
    curp: str,
) -> str | None:
    """
    Primer candidato de búsqueda SIDEA
    inferido desde la entidad contenida
    en la CURP.

    NO se usa como entidad definitiva
    del reverso.
    """

    value = (
        curp
        or ""
    ).strip().upper()

    if len(value) != 18:
        return None

    entity_code = value[11:13]

    if entity_code == "NE":
        return None

    return (
        SIDEA_CURP_ENTITY_TO_SIDEA.get(
            entity_code
        )
    )


def _sidea_normalize_entity_candidate(
    value,
) -> str | None:

    raw = str(
        value
        or ""
    ).strip()

    if not raw:
        return None

    if not raw.isdigit():
        return None

    try:
        number = int(raw)
    except Exception:
        return None

    if number < 1 or number > 32:
        return None

    return f"{number:02d}"


def sidea_entity_candidates_for_curp(
    curp: str,
    preferred_entidad=None,
) -> list[str]:
    """
    Estrategia conservadora de producción:

    1. Si se conoce entidad registral explícita,
       usar únicamente esa.
    2. Si no, usar únicamente la entidad contenida
       en la CURP como primer intento.
    3. NUNCA barrer automáticamente las 32 entidades.

    Si no se encuentra, PROVIDER16 debe permitir
    fallback hacia otro proveedor.
    """

    preferred = (
        _sidea_normalize_entity_candidate(
            preferred_entidad
        )
    )

    if preferred:
        return [preferred]

    inferred = sidea_curp_birth_entity(
        curp
    )

    if inferred:
        return [inferred]

    return []



def sidea_search_curp_auto(
    pool: SideaPool,
    account_key: str,
    curp: str,
    acto: str | int = "1",
    tipo: str | int = "1",
    preferred_entidad=None,
) -> dict:
    """
    Localiza el registro probando entidades.

    IMPORTANTE:
    - NO reserva cuota.
    - NO crea impresión.
    - NO llama solicitudImpresion.do.
    """

    candidates = (
        sidea_entity_candidates_for_curp(
            curp,
            preferred_entidad=(
                preferred_entidad
            ),
        )
    )

    last_no_record = None

    for candidate in candidates:

        try:
            result = sidea_search_curp(
                pool=pool,
                account_key=account_key,
                curp=curp,
                entidad=candidate,
                acto=str(acto),
                tipo=str(tipo),
            )

        except SideaNoRecord as exc:
            last_no_record = exc
            continue

        result = dict(result)

        result[
            "resolved_search_entity"
        ] = candidate

        if not str(
            result.get("entidad")
            or ""
        ).strip():
            result["entidad"] = candidate

        print(
            "PROVIDER16_SIDEA_ENTITY_RESOLVED =",
            {
                "account": account_key,
                "entity": candidate,
                "birth_candidate": (
                    sidea_curp_birth_entity(
                        curp
                    )
                ),
            },
            flush=True,
        )

        return result

    raise SideaNoRecord(
        "SIDEA_NO_RECORD:"
        "SEARCH_ENTITY_NOT_FOUND"
    ) from last_no_record


def sidea_generate_pdf(
    pool: SideaPool,
    curp: str,
    entidad: str | int | None = None,
    acto: str | int = "1",
    tipo: str | int = "1",
    accounts: list[SideaAccount] | None = None,
    oid_poll_attempts: int = 20,
    oid_poll_delay_sec: float = 2.0,
    response_poll_attempts: int = 45,
    response_poll_delay_sec: float = 4.0,
) -> dict:
    """
    PROVIDER16 SIDEA - flujo completo de producción.

    Retorna:
      {
        pdf_bytes,
        account_key,
        usage_reserved,
        peticion_oid,
        respuesta_oid,
        registration_entity,
        cadena,
      }

    REGLA DE CUOTA:
    - búsqueda y baseline ocurren ANTES de reservar.
    - reserva justo antes de solicitudImpresion.do.
    - si todavía NO se llamó al POST y falla algo:
        no se consume.
    - una vez intentado solicitudImpresion.do:
        NUNCA se libera automáticamente la reserva,
        aunque haya timeout/error incierto.
    """

    from io import BytesIO
    from pypdf import PdfReader
    import time

    curp = (
        curp
        or ""
    ).strip().upper()

    entidad = (
        _sidea_normalize_entity_candidate(
            entidad
        )
    )

    acto = str(
        acto
        or "1"
    ).strip()

    tipo = str(
        tipo
        or "1"
    ).strip()

    if not curp:
        raise SideaError(
            "SIDEA_EMPTY_CURP"
        )

    if accounts is None:
        accounts = (
            load_sidea_accounts()
        )

    if not accounts:
        raise SideaNoReadyAccount(
            "SIDEA_ACCOUNTS_NOT_CONFIGURED"
        )

    candidates = (
        _sidea_prod_candidate_accounts(
            pool,
            accounts,
        )
    )

    if not candidates:
        raise SideaNoReadyAccount(
            "SIDEA_NO_READY_ACCOUNT"
        )

    last_need_login = None

    for account in candidates:

        lock_token = (
            _sidea_prod_acquire_lock(
                pool,
                account.key,
            )
        )

        if not lock_token:
            continue

        try:

            # =================================================
            # 1. SEARCH - todavía SIN reservar cuota
            # =================================================

            try:
                search = sidea_search_curp_auto(
                    pool=pool,
                    account_key=(
                        account.key
                    ),
                    curp=curp,
                    acto=acto,
                    tipo=tipo,
                    preferred_entidad=entidad,
                )

            except SideaNeedLogin as exc:
                last_need_login = exc
                continue

            chain = (
                search.get("cadena")
                or ""
            ).strip()

            if not chain:
                raise SideaNoRecord(
                    "SIDEA_NO_RECORD:"
                    "EMPTY_CHAIN"
                )

            payload = dict(
                search.get(
                    "print_payload"
                )
                or {}
            )

            # Valores ya confirmados mediante
            # prueba real SIDEA.
            payload["tipo"] = tipo
            payload["acto"] = acto
            payload["formato"] = "1"
            payload["impresiones"] = "1"
            payload["dialecto"] = "1"
            payload["folioHacienda"] = ""
            payload["folioControl"] = ""

            if payload.get(
                "sexo"
            ) not in {
                "M",
                "F",
            }:
                raise SideaError(
                    "SIDEA_BAD_PRINT_SEX"
                )

            # =================================================
            # 2. SESIÓN HTTP + BASELINE
            #    todavía SIN reservar cuota
            # =================================================

            session, state = (
                pool.build_http_session(
                    account.key
                )
            )

            baseline_html = (
                _sidea_prod_monitor_get(
                    session
                )
            )

            baseline = (
                _sidea_prod_parse_monitor(
                    baseline_html
                )
            )

            baseline_petitions = (
                _sidea_prod_petition_oids(
                    baseline[
                        "petitions"
                    ],
                    curp,
                    chain,
                )
            )

            baseline_responses = (
                _sidea_prod_response_oids(
                    baseline[
                        "responses"
                    ],
                    curp,
                    chain,
                )
            )

            baseline_all = (
                baseline_petitions
                | baseline_responses
            )

            # =================================================
            # 3. RESERVA ATÓMICA
            #    Desde aquí hay consumo local.
            # =================================================

            reserved = pool.reserve_one(
                account
            )

            if reserved is None:
                continue

            print(
                "PROVIDER16_SIDEA_RESERVED =",
                {
                    "account": (
                        account.key
                    ),
                    "usage": reserved,
                },
                flush=True,
            )

            # =================================================
            # 4. POST DE IMPRESIÓN
            #
            # A PARTIR DE AQUÍ NO LIBERAR RESERVA
            # automáticamente.
            # =================================================

            submit_html = ""
            submit_uncertain = False

            try:

                submit = session.post(
                    (
                        f"{SIDEA_BASE_URL}"
                        "/solicitudImpresion.do"
                    ),
                    data=payload,
                    headers={
                        "Referer": (
                            f"{SIDEA_BASE_URL}"
                            "/solicitudXCURP.do"
                        ),
                        "Origin": (
                            "https://csidea."
                            "registrocivil.gob.mx"
                        ),
                    },
                    timeout=(
                        SIDEA_HTTP_CONNECT_TIMEOUT,
                        SIDEA_HTTP_READ_TIMEOUT,
                    ),
                    allow_redirects=True,
                )

                submit.raise_for_status()

                submit_html = (
                    submit.text
                    or ""
                )

            except requests.RequestException:
                # El servidor pudo haber creado
                # la petición aunque el cliente
                # haya sufrido timeout/corte.
                submit_uncertain = True

            # =================================================
            # 5. RESOLVER peticionOID
            #    Busca tanto Peticiones como Respuestas.
            # =================================================

            peticion_oid = None
            response_row = None

            total_oid_attempts = max(
                1,
                int(oid_poll_attempts),
            )

            for attempt in range(
                total_oid_attempts + 1
            ):

                if (
                    attempt == 0
                    and submit_html
                ):
                    current_html = (
                        submit_html
                    )
                else:

                    if attempt > 0:
                        time.sleep(
                            float(
                                oid_poll_delay_sec
                            )
                        )

                    current_html = (
                        _sidea_prod_monitor_get(
                            session
                        )
                    )

                parsed = (
                    _sidea_prod_parse_monitor(
                        current_html
                    )
                )

                petition_ids = (
                    _sidea_prod_petition_oids(
                        parsed[
                            "petitions"
                        ],
                        curp,
                        chain,
                    )
                    - baseline_all
                )

                response_ids = (
                    _sidea_prod_response_oids(
                        parsed[
                            "responses"
                        ],
                        curp,
                        chain,
                    )
                    - baseline_all
                )

                candidates_oid = (
                    petition_ids
                    | response_ids
                )

                if len(
                    candidates_oid
                ) == 1:

                    peticion_oid = next(
                        iter(
                            candidates_oid
                        )
                    )

                    response_row = (
                        _sidea_prod_response_for_oid(
                            parsed[
                                "responses"
                            ],
                            peticion_oid,
                        )
                    )

                    break

                if len(
                    candidates_oid
                ) > 1:
                    raise SideaSubmitUncertain(
                        "SIDEA_MULTIPLE_NEW_PETITIONS:"
                        f"{account.key}:"
                        f"{reserved}"
                    )

            if not peticion_oid:

                reason = (
                    "SUBMIT_TIMEOUT_OR_NETWORK"
                    if submit_uncertain
                    else "POST_200_WITHOUT_OID"
                )

                raise SideaSubmitUncertain(
                    "SIDEA_SUBMIT_UNCERTAIN:"
                    f"{reason}:"
                    f"{account.key}:"
                    f"{reserved}"
                )

            print(
                "PROVIDER16_SIDEA_PETITION_RESOLVED =",
                {
                    "account": (
                        account.key
                    ),
                    "usage": reserved,
                },
                flush=True,
            )

            # =================================================
            # 6. ESPERAR RESPUESTA
            # =================================================

            if response_row is None:

                for _ in range(
                    max(
                        1,
                        int(
                            response_poll_attempts
                        ),
                    )
                ):

                    current_html = (
                        _sidea_prod_monitor_get(
                            session
                        )
                    )

                    parsed = (
                        _sidea_prod_parse_monitor(
                            current_html
                        )
                    )

                    response_row = (
                        _sidea_prod_response_for_oid(
                            parsed[
                                "responses"
                            ],
                            peticion_oid,
                        )
                    )

                    if response_row:
                        break

                    time.sleep(
                        float(
                            response_poll_delay_sec
                        )
                    )

            if response_row is None:
                raise SideaResponseTimeout(
                    "SIDEA_RESPONSE_TIMEOUT:"
                    f"{account.key}:"
                    f"{peticion_oid}"
                )

            # =================================================
            # 7. VALIDAR RESPUESTA
            # =================================================

            cadena_respuesta = str(
                response_row[0]
                or ""
            ).strip()

            if (
                not cadena_respuesta
                or cadena_respuesta.lower()
                == "no existe acto."
            ):
                raise SideaNoRecord(
                    "SIDEA_NO_RECORD:"
                    "REMOTE_NO_ACT"
                )

            if len(response_row) < 13:
                raise SideaError(
                    "SIDEA_RESPONSE_ROW_SHORT"
                )

            respuesta_oid = str(
                response_row[12]
                or ""
            ).strip()

            respuesta_fecha = str(
                response_row[11]
                or ""
            ).strip()

            if (
                not respuesta_oid
                or not respuesta_fecha
            ):
                raise SideaError(
                    "SIDEA_RESPONSE_INCOMPLETE"
                )

            # =================================================
            # 8. URL PDF
            #
            # Mantener exactamente la relación
            # usada por el JavaScript SIDEA:
            # origen = record[8]
            # destino = record[9]
            # =================================================

            pdf_params = {
                "acto": str(
                    response_row[7]
                    or ""
                ).strip(),

                "cadena": (
                    cadena_respuesta
                ),

                "origen": str(
                    response_row[8]
                    or ""
                ).strip(),

                "destino": str(
                    response_row[9]
                    or ""
                ).strip(),

                "peticionOID": str(
                    response_row[10]
                    or ""
                ).strip(),

                "respuestaFecha": (
                    respuesta_fecha
                ),

                "respuestaOID": (
                    respuesta_oid
                ),
            }

            if not all(
                pdf_params.values()
            ):
                raise SideaPdfError(
                    "SIDEA_PDF_PARAMS_INCOMPLETE"
                )

            # =================================================
            # 9. DESCARGAR PDF
            # =================================================

            pdf_response = session.get(
                (
                    f"{SIDEA_BASE_URL}"
                    "/actaPDF.do"
                ),
                params=pdf_params,
                timeout=(
                    SIDEA_HTTP_CONNECT_TIMEOUT,
                    SIDEA_HTTP_READ_TIMEOUT,
                ),
                allow_redirects=True,
            )

            pdf_response.raise_for_status()

            pdf_bytes = (
                pdf_response.content
                or b""
            )

            content_type = (
                pdf_response.headers.get(
                    "Content-Type"
                )
                or ""
            ).lower()

            if not pdf_bytes.startswith(
                b"%PDF-"
            ):
                raise SideaPdfError(
                    "SIDEA_INVALID_PDF_SIGNATURE"
                )

            if "pdf" not in content_type:
                raise SideaPdfError(
                    "SIDEA_INVALID_PDF_CONTENT_TYPE"
                )

            try:
                front_pages = len(
                    PdfReader(
                        BytesIO(
                            pdf_bytes
                        )
                    ).pages
                )
            except Exception as exc:
                raise SideaPdfError(
                    "SIDEA_FRONT_PDF_PARSE_ERROR"
                ) from exc

            if front_pages != 1:
                raise SideaPdfError(
                    "SIDEA_FRONT_PAGE_COUNT:"
                    f"{front_pages}"
                )

            # =================================================
            # 10. ENTIDAD REGISTRAL + REVERSO
            # =================================================

            registration_entity = ""

            if len(response_row) > 16:
                registration_entity = str(
                    response_row[16]
                    or ""
                ).strip()

            if not registration_entity:
                registration_entity = str(
                    search.get(
                        "entidad"
                    )
                    or entidad
                ).strip()

            final_pdf = append_sidea_rear(
                pdf_bytes,
                registration_entity,
            )

            try:
                final_pages = len(
                    PdfReader(
                        BytesIO(
                            final_pdf
                        )
                    ).pages
                )
            except Exception as exc:
                raise SideaPdfError(
                    "SIDEA_FINAL_PDF_PARSE_ERROR"
                ) from exc

            if final_pages != 2:
                raise SideaPdfError(
                    "SIDEA_FINAL_PAGE_COUNT:"
                    f"{final_pages}"
                )

            # =================================================
            # 11. GUARDAR COOKIES ACTUALIZADAS
            # =================================================

            pool.save_session(
                account_key=(
                    account.key
                ),
                cookies=(
                    _sidea_safe_cookie_dict(
                        session
                    )
                ),
                session_id=(
                    search.get(
                        "session_id"
                    )
                    or ""
                ),
                usuario=(
                    search.get(
                        "usuario"
                    )
                    or ""
                ),
                usuario_rol=(
                    search.get(
                        "usuario_rol"
                    )
                    or ""
                ),
                usuario_entidad=(
                    search.get(
                        "usuario_entidad"
                    )
                    or ""
                ),
            )

            pool.set_status(
                account.key,
                "READY",
            )

            print(
                "PROVIDER16_SIDEA_SUCCESS =",
                {
                    "account": (
                        account.key
                    ),
                    "usage": reserved,
                    "front_pages": (
                        front_pages
                    ),
                    "final_pages": (
                        final_pages
                    ),
                },
                flush=True,
            )

            return {
                "pdf_bytes": (
                    final_pdf
                ),
                "account_key": (
                    account.key
                ),
                "usage_reserved": (
                    reserved
                ),
                "peticion_oid": (
                    peticion_oid
                ),
                "respuesta_oid": (
                    respuesta_oid
                ),
                "registration_entity": (
                    registration_entity
                ),
                "cadena": (
                    cadena_respuesta
                ),
                "front_pages": (
                    front_pages
                ),
                "final_pages": (
                    final_pages
                ),
            }

        finally:
            _sidea_prod_release_lock(
                pool,
                account.key,
                lock_token,
            )

    if last_need_login is not None:
        raise SideaNeedLogin(
            "SIDEA_ALL_READY_ACCOUNTS_NEED_LOGIN"
        ) from last_need_login

    raise SideaBusy(
        "SIDEA_ALL_READY_ACCOUNTS_BUSY"
    )
