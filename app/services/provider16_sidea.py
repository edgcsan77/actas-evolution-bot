from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
import fitz
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


# ============================================================
# PROVIDER16_OPERATING_WINDOW_V1
#
# SIDEA solamente puede iniciar trabajo nuevo entre:
#
#   07:00 <= hora CDMX < 23:00
#
# A las 23:00 ya se considera cerrado.
# ============================================================

SIDEA_OPERATING_START_MINUTE = 7 * 60
SIDEA_OPERATING_END_MINUTE = 23 * 60


def sidea_operating_window(
    now: datetime | None = None,
) -> dict:

    tz = ZoneInfo(
        SIDEA_TIMEZONE
    )

    if now is None:
        local_now = datetime.now(
            tz
        )

    elif now.tzinfo is None:
        local_now = now.replace(
            tzinfo=tz
        )

    else:
        local_now = now.astimezone(
            tz
        )

    minute_of_day = (
        local_now.hour * 60
        + local_now.minute
    )

    is_open = (
        SIDEA_OPERATING_START_MINUTE
        <= minute_of_day
        < SIDEA_OPERATING_END_MINUTE
    )

    return {
        "is_open": is_open,
        "timezone": SIDEA_TIMEZONE,
        "local_iso": local_now.isoformat(),
        "start": "07:00",
        "end": "23:00",
    }


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

    # SIDEA_DB_ACCOUNTS_V1
    #
    # Configuración central del panel:
    # SIDEA1..SIDEA10 en PostgreSQL.
    #
    # Si todavía no existe configuración DB,
    # conserva compatibilidad con
    # SIDEA_ACCOUNTS_JSON del .sidea.env.
    try:
        from app.services.provider16_accounts import (
            load_sidea_account_dicts,
        )

        data = (
            load_sidea_account_dicts()
        )

    except Exception as exc:
        raise SideaError(
            "SIDEA_ACCOUNTS_DB_LOAD_ERROR:"
            f"{type(exc).__name__}:{exc}"
        ) from exc

    if not data:
        raw = os.getenv(
            "SIDEA_ACCOUNTS_JSON",
            "[]",
        ).strip()

        try:
            data = json.loads(raw)
        except Exception as exc:
            raise SideaError(
                f"SIDEA_ACCOUNTS_JSON_INVALID:{exc}"
            ) from exc

    if not isinstance(data, list):
        raise SideaError(
            "SIDEA_ACCOUNTS_MUST_BE_LIST"
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

    @staticmethod
    def _usage_bucket_date(
        local_now: datetime,
    ) -> str:
        """
        Define el bucket de consumo SIDEA.

        Lunes-viernes usan su fecha normal.
        Sabado usa su propia fecha.
        Domingo reutiliza la fecha del sabado.

        De esta forma sabado + domingo comparten
        exactamente el mismo limite por cuenta.
        """

        # WEEKEND_USAGE_POOL_V1
        if local_now.weekday() == 6:
            local_now = (
                local_now
                - timedelta(days=1)
            )

        return local_now.strftime(
            "%Y-%m-%d"
        )

    def _today(self) -> str:
        now = datetime.now(
            ZoneInfo(SIDEA_TIMEZONE)
        )

        return self._usage_bucket_date(
            now
        )

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

    # PROVIDER16_REQUEST_GUARD_V2
    def reserve_one_for_request(
        self,
        account: SideaAccount,
        request_id: int,
    ) -> int | None:
        """
        Reserva cuota SIDEA de manera atomica
        junto con un guard por RequestLog.id.

        Si el request ya habia reservado SIDEA,
        JAMAS incrementa nuevamente una cuenta.
        """

        request_id = int(
            request_id
        )

        usage_key = self._usage_key(
            account.key
        )

        guard_key = (
            "provider16:sidea:"
            "request_guard:v2:"
            f"{request_id}"
        )

        lua = """
        local previous = redis.call(
            'GET',
            KEYS[2]
        )

        if previous then
            local current = tonumber(
                redis.call(
                    'GET',
                    KEYS[1]
                ) or '0'
            )

            return {
                -1,
                current
            }
        end

        local current = tonumber(
            redis.call(
                'GET',
                KEYS[1]
            ) or '0'
        )

        local hard_limit = tonumber(
            ARGV[1]
        )

        if current >= hard_limit then
            return {
                0,
                current
            }
        end

        local new_value = current + 1

        redis.call(
            'SET',
            KEYS[1],
            new_value,
            'EX',
            172800
        )

        redis.call(
            'SET',
            KEYS[2],
            ARGV[2] .. ':' ..
            tostring(new_value),
            'EX',
            2592000
        )

        return {
            1,
            new_value
        }
        """

        result = self.redis.eval(
            lua,
            2,
            usage_key,
            guard_key,
            int(
                account.daily_limit
            ),
            account.key,
        )

        code = int(
            result[0]
        )

        value = int(
            result[1]
        )

        if code == -1:
            raise SideaSubmitUncertain(
                "SIDEA_REQUEST_ALREADY_"
                "RESERVED_OR_SUBMITTED:"
                f"{request_id}"
            )

        if code == 0:
            return None

        return value


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



# ============================================================
# PROVIDER16_INTERNAL_REFERENCE_V1
#
# Referencia DERIVADA / INTERNA.
# NO representa un folio emitido por SIDEA / Registro Civil.
# ============================================================

_SIDEA_CODE128_PATTERNS = (
    "212222","222122","222221","121223","121322",
    "131222","122213","122312","132212","221213",
    "221312","231212","112232","122132","122231",
    "113222","123122","123221","223211","221132",
    "221231","213212","223112","312131","311222",
    "321122","321221","312212","322112","322211",
    "212123","212321","232121","111323","131123",
    "131321","112313","132113","132311","211313",
    "231113","231311","112133","112331","132131",
    "113123","113321","133121","313121","211331",
    "231131","213113","213311","213131","311123",
    "311321","331121","312113","312311","332111",
    "314111","221411","431111","111224","111422",
    "121124","121421","141122","141221","112214",
    "112412","122114","122411","142112","142211",
    "241211","221114","413111","241112","134111",
    "111242","121142","121241","114212","124112",
    "124211","411212","421112","421211","212141",
    "214121","412121","111143","111341","131141",
    "114113","114311","411113","411311","113141",
    "114131","311141","411131","211412","211214",
    "211232","2331112",
)


def _sidea_extract_electronic_identifier(
    pdf_bytes: bytes,
) -> str:
    """
    Extrae SOLO el Identificador Electrónico de la
    primera página.

    PROVIDER16_IDENTIFIER_SPATIAL_V2

    Algunos formatos SIDEA (confirmado DEFUNCION)
    almacenan internamente el texto en este orden:

        05030000220260101391
        Identificador Electrónico

    aunque visualmente el número se encuentre debajo
    de la etiqueta.

    Por eso la fuente primaria es la POSICION física
    del texto, no únicamente el orden de extracción.
    """

    if not pdf_bytes:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_EMPTY_PDF"
        )

    try:
        doc = fitz.open(
            stream=pdf_bytes,
            filetype="pdf",
        )

    except Exception as exc:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_PDF_OPEN_ERROR"
        ) from exc

    try:

        if doc.page_count != 1:
            raise SideaPdfError(
                "SIDEA_INTERNAL_REF_EXPECTED_ONE_PAGE:"
                f"{doc.page_count}"
            )

        page = doc[0]

        text = (
            page.get_text(
                "text"
            )
            or ""
        )

        words = (
            page.get_text(
                "words"
            )
            or []
        )

        # ====================================================
        # 1. ESTRATEGIA PRIMARIA:
        #    POSICION REAL DEL LABEL + NUMERO
        # ====================================================

        identifier_words = []

        label_words = []

        for word in words:

            if (
                not isinstance(
                    word,
                    (list, tuple),
                )
                or len(word) < 5
            ):
                continue

            value = str(
                word[4]
                or ""
            ).strip()

            normalized = (
                value
                .lower()
                .replace("ó", "o")
            )

            if (
                "identificador"
                in normalized
                or "electronico"
                in normalized
            ):
                label_words.append(
                    word
                )

        # Normalmente son dos palabras:
        # Identificador + Electrónico.
        #
        # Formamos el rectángulo completo de esas palabras.
        if label_words:

            label_x0 = min(
                float(w[0])
                for w in label_words
            )

            label_y0 = min(
                float(w[1])
                for w in label_words
            )

            label_x1 = max(
                float(w[2])
                for w in label_words
            )

            label_y1 = max(
                float(w[3])
                for w in label_words
            )

            label_center_x = (
                label_x0
                + label_x1
            ) / 2.0

            for word in words:

                if (
                    not isinstance(
                        word,
                        (list, tuple),
                    )
                    or len(word) < 5
                ):
                    continue

                value = str(
                    word[4]
                    or ""
                ).strip()

                # El valor oficial que buscamos tiene
                # exactamente 20 dígitos.
                if not re.fullmatch(
                    r"\d{20}",
                    value,
                ):
                    continue

                x0 = float(
                    word[0]
                )

                y0 = float(
                    word[1]
                )

                x1 = float(
                    word[2]
                )

                y1 = float(
                    word[3]
                )

                center_x = (
                    x0 + x1
                ) / 2.0

                # --------------------------------------------
                # Debe encontrarse físicamente muy cerca del
                # label.
                #
                # Permitimos pequeño solapamiento vertical
                # porque las cajas de fuente del PDF pueden
                # tocarse.
                # --------------------------------------------

                vertical_ok = (
                    y0
                    >= (
                        label_y0
                        - 5.0
                    )
                    and y0
                    <= (
                        label_y1
                        + 35.0
                    )
                )

                horizontal_ok = (
                    abs(
                        center_x
                        - label_center_x
                    )
                    <= 100.0
                )

                # También exigir que el número esté en la
                # misma zona horizontal general del label.
                overlap_ok = (
                    x1
                    >= (
                        label_x0
                        - 50.0
                    )
                    and x0
                    <= (
                        label_x1
                        + 50.0
                    )
                )

                if (
                    vertical_ok
                    and horizontal_ok
                    and overlap_ok
                ):
                    identifier_words.append(
                        {
                            "value": value,
                            "distance_y": abs(
                                y0
                                - label_y1
                            ),
                            "distance_x": abs(
                                center_x
                                - label_center_x
                            ),
                            "bbox": (
                                x0,
                                y0,
                                x1,
                                y1,
                            ),
                        }
                    )

        # Eliminar duplicados conservando el mejor candidato.
        best_by_value = {}

        for item in identifier_words:

            value = item[
                "value"
            ]

            score = (
                item[
                    "distance_y"
                ],
                item[
                    "distance_x"
                ],
            )

            current = (
                best_by_value.get(
                    value
                )
            )

            if (
                current is None
                or score
                < current[
                    "score"
                ]
            ):
                best_by_value[
                    value
                ] = {
                    "score": score,
                    "item": item,
                }

        spatial_values = list(
            best_by_value.keys()
        )

        if len(
            spatial_values
        ) == 1:

            identifier = (
                spatial_values[0]
            )

            print(
                "PROVIDER16_IDENTIFIER_"
                "SPATIAL_OK =",
                {
                    "identifier_last7": (
                        identifier[-7:]
                    ),
                    "bbox": (
                        best_by_value[
                            identifier
                        ][
                            "item"
                        ][
                            "bbox"
                        ]
                    ),
                },
                flush=True,
            )

            return identifier

        if len(
            spatial_values
        ) > 1:

            print(
                "PROVIDER16_IDENTIFIER_"
                "SPATIAL_AMBIGUOUS =",
                spatial_values,
                flush=True,
            )

        # ====================================================
        # 2. FALLBACK TEXTO
        #
        # Mantener compatibilidad con formatos NACIMIENTO
        # que ya funcionaban.
        #
        # A diferencia del código anterior buscamos tanto
        # ANTES como DESPUES de la etiqueta.
        # ====================================================

        label = re.search(
            r"Identificador\s+"
            r"Electr[oó]nico",
            text,
            flags=re.I,
        )

        if not label:
            raise SideaPdfError(
                "SIDEA_INTERNAL_REF_IDENTIFIER_LABEL_NOT_FOUND"
            )

        window_start = max(
            0,
            label.start() - 180,
        )

        window_end = min(
            len(text),
            label.end() + 320,
        )

        nearby = text[
            window_start:
            window_end
        ]

        # Separar Código de Verificación si aparece
        # DESPUÉS del label.
        verification_label = re.search(
            r"C[oó]digo\s+(?:de\s+)?"
            r"Verificaci[oó]n",
            nearby,
            flags=re.I,
        )

        if verification_label:

            local_label_offset = (
                label.start()
                - window_start
            )

            if (
                verification_label.start()
                > local_label_offset
            ):
                nearby = nearby[
                    :verification_label.start()
                ]

        values = re.findall(
            r"(?<!\d)"
            r"(\d{20})"
            r"(?!\d)",
            nearby,
        )

        if not values:

            loose_values = re.findall(
                r"(?<!\d)"
                r"((?:\d[\s]*){20})"
                r"(?!\d)",
                nearby,
            )

            for raw_value in loose_values:

                normalized = re.sub(
                    r"\s+",
                    "",
                    raw_value,
                )

                if re.fullmatch(
                    r"\d{20}",
                    normalized,
                ):
                    values.append(
                        normalized
                    )

        values = list(
            dict.fromkeys(
                values
            )
        )

        if len(values) == 1:

            print(
                "PROVIDER16_IDENTIFIER_"
                "TEXT_FALLBACK_OK =",
                {
                    "identifier_last7": (
                        values[0][-7:]
                    ),
                },
                flush=True,
            )

            return values[0]

        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_IDENTIFIER_AMBIGUOUS:"
            f"{values}"
        )

    finally:
        doc.close()

def _sidea_resolve_entity_code(
    registration_entity: Any,
) -> str:
    """
    Convierte la entidad registral final al código
    de dos dígitos usado por el mapa SIDEA.
    """

    asset = sidea_entity_to_asset(
        registration_entity
    )

    codes = [
        str(code).zfill(2)
        for code, value
        in SIDEA_ENTITY_CODE_TO_ASSET.items()
        if value == asset
    ]

    if len(codes) != 1:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_ENTITY_CODE_AMBIGUOUS:"
            f"{registration_entity}:{asset}:{codes}"
        )

    return codes[0]


def _sidea_build_internal_reference(
    registration_entity: Any,
    identifier: str,
) -> tuple[str, str]:
    """
    Retorna:
      visible: A16 0010799-A
      barcode: A160010799-A
    """

    identifier = str(
        identifier or ""
    ).strip()

    if not re.fullmatch(
        r"\d{20}",
        identifier,
    ):
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_BAD_IDENTIFIER:"
            f"{identifier}"
        )

    entity_code = (
        _sidea_resolve_entity_code(
            registration_entity
        )
    )

    last7 = identifier[-7:]

    visible = (
        f"A{entity_code} "
        f"{last7}-A"
    )

    barcode_value = (
        f"A{entity_code}"
        f"{last7}-A"
    )

    return visible, barcode_value


def _sidea_draw_code128b(
    page,
    value: str,
    rect,
) -> None:
    """
    Code128-B puro con vectores PyMuPDF.
    Sin dependencias externas.
    """

    value = str(
        value or ""
    )

    if not value:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_BARCODE_EMPTY"
        )

    data_codes = []

    for ch in value:
        n = ord(ch)

        if n < 32 or n > 126:
            raise SideaPdfError(
                "SIDEA_INTERNAL_REF_BARCODE_BAD_CHAR:"
                f"{repr(ch)}"
            )

        data_codes.append(
            n - 32
        )

    # Code128-B start = 104
    checksum = (
        104
        + sum(
            index * code
            for index, code
            in enumerate(
                data_codes,
                start=1,
            )
        )
    ) % 103

    symbols = [
        104,
        *data_codes,
        checksum,
        106,
    ]

    quiet_modules = 10

    total_modules = (
        quiet_modules * 2
        + sum(
            sum(
                int(x)
                for x in
                _SIDEA_CODE128_PATTERNS[
                    symbol
                ]
            )
            for symbol in symbols
        )
    )

    module_width = (
        rect.width
        / total_modules
    )

    x = (
        rect.x0
        + quiet_modules
        * module_width
    )

    for symbol in symbols:

        pattern = (
            _SIDEA_CODE128_PATTERNS[
                symbol
            ]
        )

        black = True

        for digit in pattern:

            width = (
                int(digit)
                * module_width
            )

            if black:
                page.draw_rect(
                    fitz.Rect(
                        x,
                        rect.y0,
                        x + width,
                        rect.y1,
                    ),
                    color=None,
                    fill=(0, 0, 0),
                    width=0,
                    overlay=True,
                )

            x += width
            black = not black


def add_sidea_internal_reference(
    pdf_bytes: bytes,
    registration_entity: Any,
) -> bytes:
    """
    Agrega REFERENCIA INTERNA en la primera página.

    Posición calibrada contra formato SIDEA carta 612x792:
      centro X = 115.5
      título   baseline y = 49
      valor    baseline y = 61
      barcode  x=54..177 / y=64..79
    """

    identifier = (
        _sidea_extract_electronic_identifier(
            pdf_bytes
        )
    )

    visible, barcode_value = (
        _sidea_build_internal_reference(
            registration_entity,
            identifier,
        )
    )

    try:
        doc = fitz.open(
            stream=pdf_bytes,
            filetype="pdf",
        )
    except Exception as exc:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_PDF_OPEN_ERROR"
        ) from exc

    try:
        if doc.page_count != 1:
            raise SideaPdfError(
                "SIDEA_INTERNAL_REF_EXPECTED_ONE_PAGE:"
                f"{doc.page_count}"
            )

        page = doc[0]

        if (
            abs(page.rect.width - 612.0)
            > 2.0
            or
            abs(page.rect.height - 792.0)
            > 2.0
        ):
            raise SideaPdfError(
                "SIDEA_INTERNAL_REF_UNEXPECTED_PAGE_SIZE:"
                f"{page.rect.width}x"
                f"{page.rect.height}"
            )

        center_x = 115.5

        title = (
            "FOLIO"
        )

        title_size = 8.5

        title_width = (
            fitz.get_text_length(
                title,
                fontname="helv",
                fontsize=title_size,
            )
        )

        page.insert_text(
            (
                center_x
                - title_width / 2,
                49.0,
            ),
            title,
            fontname="helv",
            fontsize=title_size,
            color=(0, 0, 0),
            overlay=True,
        )

        ref_size = 10.5

        ref_width = (
            fitz.get_text_length(
                visible,
                fontname="helv",
                fontsize=ref_size,
            )
        )

        page.insert_text(
            (
                center_x
                - ref_width / 2,
                61.0,
            ),
            visible,
            fontname="helv",
            fontsize=ref_size,
            color=(0, 0, 0),
            overlay=True,
        )

        _sidea_draw_code128b(
            page,
            barcode_value,
            fitz.Rect(
                54.0,
                64.0,
                177.0,
                79.0,
            ),
        )

        result = doc.tobytes(
            garbage=4,
            deflate=True,
        )

    finally:
        doc.close()

    try:
        check = fitz.open(
            stream=result,
            filetype="pdf",
        )

        try:
            if check.page_count != 1:
                raise SideaPdfError(
                    "SIDEA_INTERNAL_REF_OUTPUT_PAGE_COUNT:"
                    f"{check.page_count}"
                )
        finally:
            check.close()

    except SideaPdfError:
        raise

    except Exception as exc:
        raise SideaPdfError(
            "SIDEA_INTERNAL_REF_OUTPUT_INVALID"
        ) from exc

    print(
        "PROVIDER16_INTERNAL_REFERENCE_OK =",
        {
            "registration_entity": (
                str(
                    registration_entity
                    or ""
                )
            ),
            "identifier_last7": (
                identifier[-7:]
            ),
            "reference": visible,
        },
        flush=True,
    )

    return result


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


def _sidea_prod_monitor_has_structure(
    html: str,
) -> bool:
    """
    Confirma que el HTML realmente corresponde
    al monitor SIDEA que nuestro parser entiende.

    Un monitor válido puede no incluir logoutAction.do,
    pero sí debe declarar ambos datasets.
    """

    html = html or ""

    has_petitions = bool(
        re.search(
            r"\bvar\s+dsOption\s*=",
            html,
            flags=re.I,
        )
    )

    has_responses = bool(
        re.search(
            r"\bvar\s+dsOption2\s*=",
            html,
            flags=re.I,
        )
    )

    return bool(
        has_petitions
        and has_responses
    )


def _sidea_prod_monitor_get(
    session,
) -> str:
    """
    Obtiene monitoreo.do evitando falsos NEED_LOGIN.

    La estructura real del monitor es la señal primaria.
    Si la respuesta no parece un monitor válido,
    se confirma la sesión mediante /solicitudes.do
    y se reintenta MONITOR una sola vez.
    """

    monitor_url = (
        f"{SIDEA_BASE_URL}"
        "/monitoreo.do"
    )

    response = session.get(
        monitor_url,
        timeout=(
            SIDEA_HTTP_CONNECT_TIMEOUT,
            SIDEA_HTTP_READ_TIMEOUT,
        ),
        allow_redirects=True,
    )

    response.raise_for_status()

    html = response.text or ""

    # ========================================================
    # Un MONITOR real es suficiente aunque no contenga
    # logoutAction.do.
    # ========================================================

    if _sidea_prod_monitor_has_structure(
        html
    ):
        return html

    # ========================================================
    # La primera respuesta NO parece un monitor real.
    #
    # Confirmar si la sesión sigue viva utilizando una
    # página autenticada conocida y LA MISMA Session.
    # ========================================================

    verify_response = session.get(
        (
            f"{SIDEA_BASE_URL}"
            "/solicitudes.do"
        ),
        timeout=(
            SIDEA_HTTP_CONNECT_TIMEOUT,
            SIDEA_HTTP_READ_TIMEOUT,
        ),
        allow_redirects=True,
    )

    verify_response.raise_for_status()

    verify_html = (
        verify_response.text
        or ""
    )

    if not _sidea_html_is_authenticated(
        verify_html
    ):
        raise SideaNeedLogin(
            "SIDEA_NEED_LOGIN:"
            "MONITOR_CONFIRMED"
        )

    # ========================================================
    # Sesión confirmada viva.
    # Reintentar MONITOR exactamente una vez.
    # ========================================================

    retry_response = session.get(
        monitor_url,
        timeout=(
            SIDEA_HTTP_CONNECT_TIMEOUT,
            SIDEA_HTTP_READ_TIMEOUT,
        ),
        allow_redirects=True,
    )

    retry_response.raise_for_status()

    retry_html = (
        retry_response.text
        or ""
    )

    if _sidea_prod_monitor_has_structure(
        retry_html
    ):
        print(
            "SIDEA_MONITOR_RECOVERED =",
            {
                "http_status": (
                    retry_response.status_code
                ),
            },
            flush=True,
        )

        return retry_html

    # ========================================================
    # IMPORTANTE:
    #
    # La sesión está viva, pero MONITOR respondió dos veces
    # sin la estructura que nuestro parser necesita.
    #
    # NO destruir sesión.
    # NO enviar HTML inválido al parser.
    # ========================================================

    print(
        "SIDEA_MONITOR_INVALID_RESPONSE_"
        "SESSION_VALID =",
        {
            "http_status": (
                retry_response.status_code
            ),
            "html_length": len(
                retry_html
            ),
        },
        flush=True,
    )

    raise SideaError(
        "SIDEA_MONITOR_INVALID_RESPONSE"
    )


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

        # PROVIDER16_SIDEA8_PRODUCTION_EXCLUDE_V1
        # SIDEA8 no tiene credencial productiva válida todavía.
        # Puede existir en panel, pero jamás participar en requests.
        if (
            (account.key or "")
            .strip()
            .lower()
            == "sidea8"
        ):
            continue

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
    request_id: int | None = None,
    oid_poll_attempts: int = 20,
    oid_poll_delay_sec: float = 2.0,
    response_poll_attempts: int = 45,
    response_poll_delay_sec: float = 4.0,
    add_internal_folio: bool = False,
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

    # PROVIDER16_REQUEST_GUARD_V2
    request_id_int = None
    request_guard_key = None
    request_audit_key = None
    request_has_guard = False

    if request_id is not None:

        request_id_int = int(
            request_id
        )

        request_guard_key = (
            "provider16:sidea:"
            "request_guard:v2:"
            f"{request_id_int}"
        )

        request_audit_key = (
            "provider16:sidea:"
            "request_audit:v2:"
            f"{request_id_int}"
        )

        # PROVIDER16_RECOVERY_EXISTING_V1
        #
        # Si ya existe guard NO volvemos a reservar ni hacemos
        # otro POST. Más abajo entraremos al flujo de recuperación.
        request_has_guard = bool(
            pool.redis.exists(
                request_guard_key
            )
        )

    def _write_request_audit(
        state_name: str,
        account_key: str = "",
        usage_value: int | None = None,
        **extra,
    ) -> None:

        if not request_audit_key:
            return

        payload = {}

        # ----------------------------------------------------
        # MERGE: conservar todo lo conocido anteriormente.
        # ----------------------------------------------------
        try:
            previous_raw = pool.redis.get(
                request_audit_key
            )

            if previous_raw:
                if isinstance(
                    previous_raw,
                    bytes,
                ):
                    previous_raw = (
                        previous_raw.decode(
                            "utf-8",
                            errors="replace",
                        )
                    )

                previous = json.loads(
                    str(previous_raw)
                )

                if isinstance(
                    previous,
                    dict,
                ):
                    payload.update(
                        previous
                    )

        except Exception as audit_read_exc:
            print(
                "PROVIDER16_SIDEA_"
                "AUDIT_READ_ERROR =",
                {
                    "request_id": (
                        request_id_int
                    ),
                    "error": str(
                        audit_read_exc
                    )[:200],
                },
                flush=True,
            )

        payload[
            "request_id"
        ] = request_id_int

        if account_key:
            payload[
                "account"
            ] = str(
                account_key
            )

        elif "account" not in payload:
            payload[
                "account"
            ] = ""

        if usage_value is not None:
            payload[
                "usage"
            ] = int(
                usage_value
            )

        elif "usage" not in payload:
            payload[
                "usage"
            ] = None

        payload[
            "state"
        ] = (
            str(
                state_name
            )
            .strip()
            .upper()
        )

        payload[
            "updated_at"
        ] = (
            datetime.now(
                ZoneInfo(
                    SIDEA_TIMEZONE
                )
            ).isoformat()
        )

        for key, value in (
            extra.items()
        ):
            if value is None:
                continue

            payload[
                str(key)
            ] = value

        try:
            pool.redis.setex(
                request_audit_key,
                2592000,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                ),
            )

        except Exception as audit_exc:
            print(
                "PROVIDER16_SIDEA_"
                "AUDIT_WRITE_ERROR =",
                {
                    "request_id": (
                        request_id_int
                    ),
                    "state": (
                        state_name
                    ),
                    "error": str(
                        audit_exc
                    )[:200],
                },
                flush=True,
            )



    # ============================================================
    # PROVIDER16_RECOVERY_EXISTING_V1
    #
    # REGLA:
    #   guard existente = JAMAS reserve_one()
    #   guard existente = JAMAS solicitudImpresion.do
    #
    # Solamente:
    #   misma cuenta -> monitor -> misma petición/respuesta
    #   -> actaPDF.do -> folio/reverso -> return.
    # ============================================================

    def _read_request_audit() -> dict:

        if not request_audit_key:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_AUDIT_KEY_MISSING:"
                f"{request_id_int}"
            )

        raw = pool.redis.get(
            request_audit_key
        )

        if not raw:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_AUDIT_MISSING:"
                f"{request_id_int}"
            )

        if isinstance(
            raw,
            bytes,
        ):
            raw = raw.decode(
                "utf-8",
                errors="replace",
            )

        try:
            payload = json.loads(
                str(raw)
            )

        except Exception as exc:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_AUDIT_INVALID_JSON:"
                f"{request_id_int}"
            ) from exc

        if not isinstance(
            payload,
            dict,
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_AUDIT_NOT_OBJECT:"
                f"{request_id_int}"
            )

        audit_request_id = (
            payload.get(
                "request_id"
            )
        )

        if (
            audit_request_id is not None
            and int(audit_request_id)
            != int(request_id_int)
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_AUDIT_REQUEST_MISMATCH:"
                f"{request_id_int}:"
                f"{audit_request_id}"
            )

        return payload


    def _read_guard_reservation() -> tuple[str, int]:

        raw = pool.redis.get(
            request_guard_key
        )

        if not raw:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_GUARD_DISAPPEARED:"
                f"{request_id_int}"
            )

        if isinstance(
            raw,
            bytes,
        ):
            raw = raw.decode(
                "utf-8",
                errors="replace",
            )

        value = str(raw).strip()

        try:
            account_key, usage_text = (
                value.rsplit(
                    ":",
                    1,
                )
            )

            account_key = (
                account_key.strip()
            )

            usage_value = int(
                usage_text
            )

        except Exception as exc:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_BAD_GUARD:"
                f"{request_id_int}:"
                f"{value[:100]}"
            ) from exc

        if (
            not account_key
            or usage_value <= 0
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_BAD_GUARD_VALUES:"
                f"{request_id_int}"
            )

        return (
            account_key,
            usage_value,
        )


    def _recover_existing_request(
        recovery_accounts,
    ) -> dict:

        audit = (
            _read_request_audit()
        )

        (
            guard_account,
            guard_usage,
        ) = _read_guard_reservation()

        audit_account = str(
            audit.get(
                "account"
            )
            or ""
        ).strip()

        if (
            audit_account
            and audit_account
            != guard_account
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_ACCOUNT_MISMATCH:"
                f"{request_id_int}:"
                f"{audit_account}:"
                f"{guard_account}"
            )

        account_key = (
            audit_account
            or guard_account
        )

        usage_value = (
            audit.get(
                "usage"
            )
        )

        if usage_value is None:
            usage_value = (
                guard_usage
            )

        try:
            usage_value = int(
                usage_value
            )
        except Exception:
            usage_value = (
                guard_usage
            )

        account = next(
            (
                item
                for item
                in recovery_accounts
                if str(
                    item.key
                ).strip()
                == account_key
            ),
            None,
        )

        if account is None:
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_ACCOUNT_NOT_CONFIGURED:"
                f"{request_id_int}:"
                f"{account_key}"
            )

        audit_curp = str(
            audit.get(
                "curp"
            )
            or ""
        ).strip().upper()

        if (
            audit_curp
            and audit_curp != curp
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_CURP_MISMATCH:"
                f"{request_id_int}:"
                f"{audit_curp}:"
                f"{curp}"
            )

        if (
            "add_internal_folio"
            in audit
            and bool(
                audit.get(
                    "add_internal_folio"
                )
            )
            != bool(
                add_internal_folio
            )
        ):
            raise SideaSubmitUncertain(
                "SIDEA_RECOVERY_FOLIO_MODE_MISMATCH:"
                f"{request_id_int}"
            )

        lock_token = (
            _sidea_prod_acquire_lock(
                pool,
                account_key,
            )
        )

        if not lock_token:
            raise SideaBusy(
                "SIDEA_RECOVERY_ACCOUNT_BUSY:"
                f"{account_key}"
            )

        print(
            "PROVIDER16_SIDEA_RECOVERY_BEGIN =",
            {
                "request_id": (
                    request_id_int
                ),
                "account": (
                    account_key
                ),
                "usage": (
                    usage_value
                ),
                "audit_state": (
                    audit.get(
                        "state"
                    )
                ),
            },
            flush=True,
        )

        try:

            # ----------------------------------------------------
            # MISMA CUENTA / MISMA SESIÓN
            # ----------------------------------------------------

            session, state = (
                pool.build_http_session(
                    account_key
                )
            )

            pdf_params = (
                audit.get(
                    "pdf_params"
                )
            )

            required_pdf_keys = (
                "acto",
                "cadena",
                "origen",
                "destino",
                "peticionOID",
                "respuestaFecha",
                "respuestaOID",
            )

            has_complete_pdf_params = (
                isinstance(
                    pdf_params,
                    dict,
                )
                and all(
                    str(
                        pdf_params.get(
                            key
                        )
                        or ""
                    ).strip()
                    for key
                    in required_pdf_keys
                )
            )

            response_row = None

            peticion_oid = str(
                audit.get(
                    "peticion_oid"
                )
                or ""
            ).strip()

            respuesta_oid = str(
                audit.get(
                    "respuesta_oid"
                )
                or ""
            ).strip()

            cadena_respuesta = str(
                audit.get(
                    "cadena"
                )
                or ""
            ).strip()

            # ----------------------------------------------------
            # Si todavía no habíamos llegado a RESPONSE_RESOLVED,
            # encontrar la petición YA EXISTENTE en monitoreo.
            # ----------------------------------------------------

            if not has_complete_pdf_params:

                chain = cadena_respuesta

                if not chain:
                    chain = str(
                        audit.get(
                            "chain"
                        )
                        or ""
                    ).strip()

                if not chain:
                    raise SideaSubmitUncertain(
                        "SIDEA_RECOVERY_CHAIN_MISSING:"
                        f"{request_id_int}"
                    )

                baseline_petitions = {
                    str(value).strip()
                    for value
                    in (
                        audit.get(
                            "baseline_petitions"
                        )
                        or []
                    )
                    if str(
                        value
                    ).strip()
                }

                baseline_responses = {
                    str(value).strip()
                    for value
                    in (
                        audit.get(
                            "baseline_responses"
                        )
                        or []
                    )
                    if str(
                        value
                    ).strip()
                }

                baseline_all = (
                    baseline_petitions
                    | baseline_responses
                )

                parsed = None

                # ----------------------------------------------
                # PETICIÓN
                # ----------------------------------------------

                if not peticion_oid:

                    total_attempts = max(
                        1,
                        int(
                            oid_poll_attempts
                        ),
                    )

                    for attempt in range(
                        total_attempts + 1
                    ):

                        if attempt > 0:
                            time.sleep(
                                float(
                                    oid_poll_delay_sec
                                )
                            )

                        monitor_html = (
                            _sidea_prod_monitor_get(
                                session
                            )
                        )

                        parsed = (
                            _sidea_prod_parse_monitor(
                                monitor_html
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

                            break

                        if len(
                            candidates_oid
                        ) > 1:
                            raise SideaSubmitUncertain(
                                "SIDEA_RECOVERY_MULTIPLE_"
                                "NEW_PETITIONS:"
                                f"{request_id_int}:"
                                f"{account_key}"
                            )

                    if not peticion_oid:
                        raise SideaResponseTimeout(
                            "SIDEA_RECOVERY_PETITION_"
                            "NOT_VISIBLE:"
                            f"{request_id_int}:"
                            f"{account_key}"
                        )

                    _write_request_audit(
                        "PETITION_RESOLVED",
                        account_key,
                        usage_value,
                        peticion_oid=(
                            peticion_oid
                        ),
                        recovered=True,
                    )

                # ----------------------------------------------
                # RESPUESTA
                # ----------------------------------------------

                if parsed is None:

                    monitor_html = (
                        _sidea_prod_monitor_get(
                            session
                        )
                    )

                    parsed = (
                        _sidea_prod_parse_monitor(
                            monitor_html
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

                if response_row is None:

                    for _ in range(
                        max(
                            1,
                            int(
                                response_poll_attempts
                            ),
                        )
                    ):

                        monitor_html = (
                            _sidea_prod_monitor_get(
                                session
                            )
                        )

                        parsed = (
                            _sidea_prod_parse_monitor(
                                monitor_html
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
                        "SIDEA_RECOVERY_RESPONSE_TIMEOUT:"
                        f"{request_id_int}:"
                        f"{account_key}:"
                        f"{peticion_oid}"
                    )

                if len(
                    response_row
                ) < 13:
                    raise SideaError(
                        "SIDEA_RECOVERY_RESPONSE_ROW_SHORT"
                    )

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
                        "SIDEA_RECOVERY_RESPONSE_INCOMPLETE"
                    )

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
                        "SIDEA_RECOVERY_PDF_PARAMS_INCOMPLETE"
                    )

                response_entity = ""

                if len(
                    response_row
                ) > 16:
                    response_entity = str(
                        response_row[16]
                        or ""
                    ).strip()

                _write_request_audit(
                    "RESPONSE_RESOLVED",
                    account_key,
                    usage_value,
                    peticion_oid=(
                        peticion_oid
                    ),
                    respuesta_oid=(
                        respuesta_oid
                    ),
                    respuesta_fecha=(
                        respuesta_fecha
                    ),
                    cadena=(
                        cadena_respuesta
                    ),
                    registration_entity_hint=(
                        response_entity
                    ),
                    pdf_params=(
                        pdf_params
                    ),
                    recovered=True,
                )

            else:

                # Ya teníamos todos los datos exactos:
                # no necesitamos correlacionar monitor.
                pdf_params = {
                    key: str(
                        pdf_params.get(
                            key
                        )
                        or ""
                    ).strip()
                    for key
                    in required_pdf_keys
                }

                peticion_oid = (
                    pdf_params[
                        "peticionOID"
                    ]
                )

                respuesta_oid = (
                    pdf_params[
                        "respuestaOID"
                    ]
                )

                cadena_respuesta = (
                    pdf_params[
                        "cadena"
                    ]
                )

            # ----------------------------------------------------
            # DESCARGAR ESA MISMA ACTA.
            #
            # IMPORTANTE:
            # aquí NO existe solicitudImpresion.do.
            # ----------------------------------------------------

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
                    "SIDEA_RECOVERY_INVALID_PDF_SIGNATURE"
                )

            if "pdf" not in content_type:
                raise SideaPdfError(
                    "SIDEA_RECOVERY_INVALID_PDF_CONTENT_TYPE"
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
                    "SIDEA_RECOVERY_FRONT_PDF_PARSE_ERROR"
                ) from exc

            if front_pages != 1:
                raise SideaPdfError(
                    "SIDEA_RECOVERY_FRONT_PAGE_COUNT:"
                    f"{front_pages}"
                )

            _write_request_audit(
                "PDF_DOWNLOADED",
                account_key,
                usage_value,
                peticion_oid=(
                    peticion_oid
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                front_pages=(
                    front_pages
                ),
                remote_pdf_bytes=(
                    len(pdf_bytes)
                ),
                recovered=True,
            )

            # ----------------------------------------------------
            # ENTIDAD
            # ----------------------------------------------------

            registration_entity = str(
                audit.get(
                    "registration_entity"
                )
                or audit.get(
                    "registration_entity_hint"
                )
                or audit.get(
                    "preflight_entity"
                )
                or audit.get(
                    "search_entity"
                )
                or audit.get(
                    "preferred_entity"
                )
                or entidad
                or ""
            ).strip()

            if (
                response_row is not None
                and len(
                    response_row
                ) > 16
                and str(
                    response_row[16]
                    or ""
                ).strip()
            ):
                registration_entity = str(
                    response_row[16]
                    or ""
                ).strip()

            if not registration_entity:
                raise SideaPdfError(
                    "SIDEA_RECOVERY_REGISTRATION_ENTITY_MISSING"
                )

            # ----------------------------------------------------
            # MISMO POSTPROCESO LOCAL
            # ----------------------------------------------------

            if add_internal_folio:
                pdf_bytes = (
                    add_sidea_internal_reference(
                        pdf_bytes,
                        registration_entity,
                    )
                )

            final_pdf = (
                append_sidea_rear(
                    pdf_bytes,
                    registration_entity,
                )
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
                    "SIDEA_RECOVERY_FINAL_PDF_PARSE_ERROR"
                ) from exc

            if final_pages != 2:
                raise SideaPdfError(
                    "SIDEA_RECOVERY_FINAL_PAGE_COUNT:"
                    f"{final_pages}"
                )

            _write_request_audit(
                "LOCAL_PDF_READY",
                account_key,
                usage_value,
                peticion_oid=(
                    peticion_oid
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                registration_entity=(
                    registration_entity
                ),
                front_pages=(
                    front_pages
                ),
                final_pages=(
                    final_pages
                ),
                final_pdf_bytes=(
                    len(final_pdf)
                ),
                folio_applied=bool(
                    add_internal_folio
                ),
                recovered=True,
            )

            pool.set_status(
                account_key,
                "READY",
            )

            _write_request_audit(
                "SUCCESS",
                account_key,
                usage_value,
                peticion_oid=(
                    peticion_oid
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                registration_entity=(
                    registration_entity
                ),
                cadena=(
                    cadena_respuesta
                ),
                front_pages=(
                    front_pages
                ),
                final_pages=(
                    final_pages
                ),
                recovered=True,
            )

            print(
                "PROVIDER16_SIDEA_RECOVERY_SUCCESS =",
                {
                    "request_id": (
                        request_id_int
                    ),
                    "account": (
                        account_key
                    ),
                    "usage": (
                        usage_value
                    ),
                    "peticion_oid": (
                        peticion_oid
                    ),
                    "respuesta_oid": (
                        respuesta_oid
                    ),
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
                    account_key
                ),
                "usage_reserved": (
                    usage_value
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
                "recovered": True,
            }

        finally:
            _sidea_prod_release_lock(
                pool,
                account_key,
                lock_token,
            )

    if accounts is None:
        accounts = (
            load_sidea_accounts()
        )

    if not accounts:
        raise SideaNoReadyAccount(
            "SIDEA_ACCOUNTS_NOT_CONFIGURED"
        )

    if request_has_guard:

        print(
            "PROVIDER16_SIDEA_RECOVERY_GUARD_FOUND =",
            {
                "request_id": (
                    request_id_int
                ),
            },
            flush=True,
        )

        return _recover_existing_request(
            accounts
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
            # PREFLIGHT REVERSO
            #
            # search["entidad"] proviene del registro
            # localizado por SIDEA.
            #
            # Validamos existencia + PDF de una pagina
            # ANTES de reserve_one().
            # =================================================

            preflight_entity = str(
                search.get(
                    "entidad"
                )
                or payload.get(
                    "entidad"
                )
                or search.get(
                    "resolved_search_entity"
                )
                or entidad
                or ""
            ).strip()

            preflight_rear_path = (
                sidea_rear_path(
                    preflight_entity
                )
            )

            try:
                preflight_rear_reader = (
                    PdfReader(
                        str(
                            preflight_rear_path
                        )
                    )
                )

            except Exception as exc:
                raise SideaPdfError(
                    "SIDEA_REAR_INVALID:"
                    f"{preflight_rear_path}:"
                    f"{exc}"
                ) from exc

            if len(
                preflight_rear_reader.pages
            ) != 1:
                raise SideaPdfError(
                    "SIDEA_REAR_UNEXPECTED_"
                    "PAGE_COUNT:"
                    f"{preflight_rear_path}:"
                    f"{len(preflight_rear_reader.pages)}"
                )

            print(
                "PROVIDER16_SIDEA_"
                "REAR_PREFLIGHT_OK =",
                {
                    "request_id": (
                        request_id_int
                    ),
                    "entity": (
                        preflight_entity
                    ),
                    "asset": (
                        preflight_rear_path.name
                    ),
                },
                flush=True,
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
            # RECOVERY CHECKPOINT: PREPARED
            #
            # Ocurre ANTES de reservar cuota.
            # Deja persistido el baseline necesario para
            # recuperar una impresion incierta sin repetir POST.
            # =================================================

            _write_request_audit(
                "PREPARED",
                account.key,
                None,
                curp=curp,
                chain=chain,
                acto=acto,
                tipo=tipo,
                add_internal_folio=bool(
                    add_internal_folio
                ),
                preferred_entity=str(
                    entidad
                    or ""
                ),
                preflight_entity=str(
                    preflight_entity
                    or ""
                ),
                search_entity=str(
                    search.get(
                        "entidad"
                    )
                    or ""
                ),
                baseline_petitions=sorted(
                    str(value)
                    for value
                    in baseline_petitions
                ),
                baseline_responses=sorted(
                    str(value)
                    for value
                    in baseline_responses
                ),
            )

            # =================================================
            # 3. RESERVA ATÓMICA
            #    Desde aquí hay consumo local.
            # =================================================

            if request_id_int is None:

                # Compatibilidad con herramientas
                # manuales antiguas que no pasan
                # RequestLog.id.
                reserved = pool.reserve_one(
                    account
                )

            else:

                reserved = (
                    pool.reserve_one_for_request(
                        account,
                        request_id_int,
                    )
                )

            if reserved is None:
                continue

            _write_request_audit(
                "RESERVED",
                account.key,
                reserved,
            )

            print(
                "PROVIDER16_SIDEA_RESERVED =",
                {
                    "request_id": (
                        request_id_int
                    ),
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

                _write_request_audit(
                    "POST_ATTEMPTED",
                    account.key,
                    reserved,
                )

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

            _write_request_audit(
                "PETITION_RESOLVED",
                account.key,
                reserved,
                peticion_oid=str(
                    peticion_oid
                    or ""
                ),
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
            # RECOVERY CHECKPOINT: RESPONSE_RESOLVED
            #
            # Desde aqui SIDEA ya tiene una respuesta concreta.
            # Guardamos exactamente los parametros necesarios
            # para volver a descargar ESA MISMA acta.
            # =================================================

            response_registration_entity = ""

            if len(response_row) > 16:
                response_registration_entity = str(
                    response_row[16]
                    or ""
                ).strip()

            recovery_pdf_params = {
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

            _write_request_audit(
                "RESPONSE_RESOLVED",
                account.key,
                reserved,
                peticion_oid=str(
                    peticion_oid
                    or ""
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                respuesta_fecha=(
                    respuesta_fecha
                ),
                cadena=(
                    cadena_respuesta
                ),
                registration_entity_hint=(
                    response_registration_entity
                ),
                pdf_params=(
                    recovery_pdf_params
                ),
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

            _write_request_audit(
                "PDF_DOWNLOADED",
                account.key,
                reserved,
                peticion_oid=str(
                    peticion_oid
                    or ""
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                front_pages=(
                    front_pages
                ),
                remote_pdf_bytes=(
                    len(pdf_bytes)
                ),
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

            # =================================================
            # P16 FOLIO
            # Identificador electronico + entidad registral final
            # =================================================

            if add_internal_folio:
                pdf_bytes = add_sidea_internal_reference(
                    pdf_bytes,
                    registration_entity,
                )

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

            _write_request_audit(
                "LOCAL_PDF_READY",
                account.key,
                reserved,
                peticion_oid=str(
                    peticion_oid
                    or ""
                ),
                respuesta_oid=(
                    respuesta_oid
                ),
                registration_entity=(
                    registration_entity
                ),
                front_pages=(
                    front_pages
                ),
                final_pages=(
                    final_pages
                ),
                final_pdf_bytes=(
                    len(final_pdf)
                ),
                folio_applied=bool(
                    add_internal_folio
                ),
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

            _write_request_audit(
                "SUCCESS",
                account.key,
                reserved,
            )

            print(
                "PROVIDER16_SIDEA_SUCCESS =",
                {
                    "request_id": (
                        request_id_int
                    ),
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


# ============================================================
# SIDEA_MULTIACT_CHAIN_SPECIAL_V2
# ============================================================


# ============================================================
# PROVIDER16_DIRECT_RECOVERY_ENTRY_V1
# ============================================================

def sidea_recover_request(
    pool: SideaPool,
    request_id: int,
    accounts: list[SideaAccount] | None = None,
    add_internal_folio: bool = False,
) -> dict:
    """
    Recupera una impresion YA reservada/creada.

    REGLAS:
      - requiere request_guard:v2
      - usa contexto persistido por PREPARED
      - NO busca cadena nuevamente
      - NO resuelve CURP especial nuevamente
      - NO selecciona cuenta por saldo
      - NO reserva cuota
      - NO hace solicitudImpresion.do

    sidea_generate_pdf() detectará el guard y entrará
    inmediatamente al recovery interno.
    """

    request_id_int = int(
        request_id
    )

    guard_key = (
        "provider16:sidea:"
        "request_guard:v2:"
        f"{request_id_int}"
    )

    audit_key = (
        "provider16:sidea:"
        "request_audit:v2:"
        f"{request_id_int}"
    )

    if not pool.redis.exists(
        guard_key
    ):
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_GUARD_MISSING:"
            f"{request_id_int}"
        )

    raw = pool.redis.get(
        audit_key
    )

    if not raw:
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_AUDIT_MISSING:"
            f"{request_id_int}"
        )

    if isinstance(
        raw,
        bytes,
    ):
        raw = raw.decode(
            "utf-8",
            errors="replace",
        )

    try:
        audit = json.loads(
            str(raw)
        )

    except Exception as exc:
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_AUDIT_INVALID:"
            f"{request_id_int}"
        ) from exc

    if not isinstance(
        audit,
        dict,
    ):
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_AUDIT_NOT_OBJECT:"
            f"{request_id_int}"
        )

    audit_request_id = (
        audit.get(
            "request_id"
        )
    )

    if (
        audit_request_id is not None
        and int(
            audit_request_id
        ) != request_id_int
    ):
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_REQUEST_MISMATCH:"
            f"{request_id_int}:"
            f"{audit_request_id}"
        )

    curp = str(
        audit.get(
            "curp"
        )
        or ""
    ).strip().upper()

    acto = str(
        audit.get(
            "acto"
        )
        or ""
    ).strip()

    tipo = str(
        audit.get(
            "tipo"
        )
        or "1"
    ).strip()

    entidad = str(
        audit.get(
            "preflight_entity"
        )
        or audit.get(
            "search_entity"
        )
        or audit.get(
            "preferred_entity"
        )
        or ""
    ).strip()

    if not curp:
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_CURP_MISSING:"
            f"{request_id_int}"
        )

    if acto not in {
        "1",
        "2",
        "3",
        "4",
    }:
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_ACTO_INVALID:"
            f"{request_id_int}:"
            f"{acto}"
        )

    if tipo not in {
        "1",
        "2",
    }:
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_TIPO_INVALID:"
            f"{request_id_int}:"
            f"{tipo}"
        )

    if (
        "add_internal_folio"
        in audit
        and bool(
            audit.get(
                "add_internal_folio"
            )
        )
        != bool(
            add_internal_folio
        )
    ):
        raise SideaSubmitUncertain(
            "SIDEA_DIRECT_RECOVERY_FOLIO_MISMATCH:"
            f"{request_id_int}"
        )

    print(
        "PROVIDER16_DIRECT_RECOVERY_ENTRY =",
        {
            "request_id": (
                request_id_int
            ),
            "curp": curp,
            "acto": acto,
            "tipo": tipo,
            "entity": entidad,
            "audit_state": (
                audit.get(
                    "state"
                )
            ),
        },
        flush=True,
    )

    # IMPORTANTE:
    # sidea_generate_pdf() verá request_guard:v2
    # antes de candidate_accounts y hará RETURN recovery.
    return sidea_generate_pdf(
        pool=pool,
        curp=curp,
        entidad=(
            entidad
            or None
        ),
        acto=acto,
        tipo=tipo,
        accounts=accounts,
        request_id=request_id_int,
        add_internal_folio=(
            add_internal_folio
        ),
    )


def sidea_resolve_chain(
    pool: SideaPool,
    cadena: str,
    accounts: list[SideaAccount] | None = None,
) -> dict:
    """
    Resuelve CADENA DIGITAL en SIDEA.

    SOLO BUSCA:
    - no imprime
    - no reserva cuota
    - obtiene acto, entidad y CURP canónica
    """

    cadena = (
        cadena
        or ""
    ).strip()

    if not cadena:
        raise SideaError(
            "SIDEA_EMPTY_CHAIN"
        )

    if accounts is None:
        accounts = load_sidea_accounts()

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
    last_no_record = None
    obtained_lock = False

    for account in candidates:

        lock_token = (
            _sidea_prod_acquire_lock(
                pool,
                account.key,
            )
        )

        if not lock_token:
            continue

        obtained_lock = True

        try:

            try:
                session, state = (
                    pool.build_http_session(
                        account.key
                    )
                )

            except SideaNeedLogin as exc:
                last_need_login = exc
                continue

            try:
                response = session.post(
                    (
                        f"{SIDEA_BASE_URL}"
                        "/solicitudXCadena.do"
                    ),
                    data={
                        "cadena": cadena,
                    },
                    headers={
                        "Referer": (
                            f"{SIDEA_BASE_URL}"
                            "/solicitudes.do"
                        ),
                    },
                    timeout=(
                        SIDEA_HTTP_CONNECT_TIMEOUT,
                        SIDEA_HTTP_READ_TIMEOUT,
                    ),
                    allow_redirects=True,
                )

            except requests.RequestException as exc:
                raise SideaError(
                    "SIDEA_SEARCH_CHAIN_HTTP_ERROR:"
                    f"{type(exc).__name__}"
                ) from exc

            html = response.text or ""

            if not _sidea_html_is_authenticated(
                html
            ):
                pool.clear_session(
                    account.key,
                    reason="NEED_LOGIN",
                )

                last_need_login = (
                    SideaNeedLogin(
                        "SIDEA_NEED_LOGIN:"
                        f"{account.key}"
                    )
                )

                continue

            try:
                row_html = (
                    _sidea_find_matching_row_html(
                        html,
                        cadena,
                    )
                )

            except SideaNoRecord as exc:
                last_no_record = exc
                continue

            hidden = (
                _sidea_hidden_values_from_row(
                    row_html
                )
            )

            real_chain = (
                hidden.get("cadena")
                or ""
            ).strip()

            if (
                real_chain
                and real_chain != cadena
            ):
                raise SideaError(
                    "SIDEA_CHAIN_MISMATCH"
                )

            acto = (
                hidden.get("acto")
                or ""
            ).strip()

            if acto not in {
                "1",
                "2",
                "3",
                "4",
            }:
                raise SideaError(
                    "SIDEA_CHAIN_ACT_NOT_SUPPORTED:"
                    f"{acto}"
                )

            entidad = (
                hidden.get("entidad")
                or ""
            ).strip()

            if not entidad:
                raise SideaError(
                    "SIDEA_CHAIN_MISSING_ENTITY"
                )

            canonical_curp = (
                hidden.get("curp")
                or hidden.get("curp_1")
                or hidden.get("curp_2")
                or ""
            ).strip().upper()

            if not canonical_curp:
                raise SideaError(
                    "SIDEA_CHAIN_MISSING_CURP"
                )

            pool.save_session(
                account_key=account.key,
                cookies=(
                    _sidea_safe_cookie_dict(
                        session
                    )
                ),
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

            print(
                "PROVIDER16_SIDEA_CHAIN_RESOLVED =",
                {
                    "account": account.key,
                    "acto": acto,
                    "entity": entidad,
                },
                flush=True,
            )

            return {
                "account_key": account.key,
                "cadena": (
                    real_chain
                    or cadena
                ),
                "curp": canonical_curp,
                "entidad": entidad,
                "acto": acto,
                "municipio": (
                    hidden.get("municipio")
                    or ""
                ).strip(),
                "oficialia": (
                    hidden.get("oficialia")
                    or ""
                ).strip(),
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

    if last_no_record is not None:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "CHAIN_NOT_FOUND"
        ) from last_no_record

    if not obtained_lock:
        raise SideaBusy(
            "SIDEA_ALL_READY_ACCOUNTS_BUSY"
        )

    raise SideaNoRecord(
        "SIDEA_NO_RECORD:"
        "CHAIN_NOT_FOUND"
    )


def sidea_resolve_special_curp_to_chain(
    pool: SideaPool,
    curp: str,
    acto: str | int,
    accounts: list[SideaAccount] | None = None,
) -> dict:
    """
    DEFUNCION / MATRIMONIO / DIVORCIO por CURP.

    Busca únicamente en la entidad contenida en la CURP.

    Acepta que SIDEA coloque la CURP solicitada en:
      curp
      curp_1
      curp_2

    NO imprime.
    NO reserva cuota.
    """

    curp = (
        curp
        or ""
    ).strip().upper()

    acto = str(
        acto
        or ""
    ).strip()

    if not curp:
        raise SideaError(
            "SIDEA_EMPTY_CURP"
        )

    if acto not in {
        "2",
        "3",
        "4",
    }:
        raise SideaError(
            "SIDEA_SPECIAL_ACT_INVALID:"
            f"{acto}"
        )

    entidad = (
        sidea_curp_birth_entity(
            curp
        )
    )

    if not entidad:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "SPECIAL_ENTITY_UNKNOWN"
        )

    if accounts is None:
        accounts = load_sidea_accounts()

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
    last_no_record = None
    obtained_lock = False

    for account in candidates:

        lock_token = (
            _sidea_prod_acquire_lock(
                pool,
                account.key,
            )
        )

        if not lock_token:
            continue

        obtained_lock = True

        try:

            try:
                session, state = (
                    pool.build_http_session(
                        account.key
                    )
                )

            except SideaNeedLogin as exc:
                last_need_login = exc
                continue

            try:
                response = session.post(
                    (
                        f"{SIDEA_BASE_URL}"
                        "/solicitudXCURP.do"
                    ),
                    data={
                        "tipo": "1",
                        "acto": acto,
                        "entidad": entidad,
                        "curp": curp,
                    },
                    headers={
                        "Referer": (
                            f"{SIDEA_BASE_URL}"
                            "/solicitudes.do"
                        ),
                    },
                    timeout=(
                        SIDEA_HTTP_CONNECT_TIMEOUT,
                        SIDEA_HTTP_READ_TIMEOUT,
                    ),
                    allow_redirects=True,
                )

            except requests.RequestException as exc:
                raise SideaError(
                    "SIDEA_SPECIAL_SEARCH_HTTP_ERROR:"
                    f"{type(exc).__name__}"
                ) from exc

            html = response.text or ""

            if not _sidea_html_is_authenticated(
                html
            ):
                pool.clear_session(
                    account.key,
                    reason="NEED_LOGIN",
                )

                last_need_login = (
                    SideaNeedLogin(
                        "SIDEA_NEED_LOGIN:"
                        f"{account.key}"
                    )
                )

                continue

            try:
                row_html = (
                    _sidea_find_matching_row_html(
                        html,
                        curp,
                    )
                )

            except SideaNoRecord as exc:
                last_no_record = exc
                continue

            hidden = (
                _sidea_hidden_values_from_row(
                    row_html
                )
            )

            main_curp = (
                hidden.get("curp")
                or ""
            ).strip().upper()

            curp_1 = (
                hidden.get("curp_1")
                or ""
            ).strip().upper()

            curp_2 = (
                hidden.get("curp_2")
                or ""
            ).strip().upper()

            if curp not in {
                main_curp,
                curp_1,
                curp_2,
            }:
                raise SideaError(
                    "SIDEA_SPECIAL_CURP_MISMATCH"
                )

            real_acto = (
                hidden.get("acto")
                or ""
            ).strip()

            if (
                real_acto
                and real_acto != acto
            ):
                raise SideaError(
                    "SIDEA_SPECIAL_ACT_MISMATCH:"
                    f"EXPECTED_{acto}:"
                    f"REAL_{real_acto}"
                )

            cadena = (
                hidden.get("cadena")
                or ""
            ).strip()

            if not cadena:
                raise SideaNoRecord(
                    "SIDEA_NO_RECORD:"
                    "SPECIAL_EMPTY_CHAIN"
                )

            real_entity = (
                hidden.get("entidad")
                or entidad
            ).strip()

            canonical_curp = (
                main_curp
                or curp_1
                or curp_2
            )

            if not canonical_curp:
                raise SideaError(
                    "SIDEA_SPECIAL_MISSING_CANONICAL_CURP"
                )

            pool.save_session(
                account_key=account.key,
                cookies=(
                    _sidea_safe_cookie_dict(
                        session
                    )
                ),
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

            print(
                "PROVIDER16_SPECIAL_CURP_RESOLVED =",
                {
                    "account": account.key,
                    "acto": acto,
                    "entity": real_entity,
                    "matched_main":
                        main_curp == curp,
                    "matched_person1":
                        curp_1 == curp,
                    "matched_person2":
                        curp_2 == curp,
                },
                flush=True,
            )

            return {
                "account_key": account.key,
                "requested_curp": curp,
                "canonical_curp":
                    canonical_curp,
                "cadena": cadena,
                "entidad": real_entity,
                "acto": acto,
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

    if last_no_record is not None:
        raise SideaNoRecord(
            "SIDEA_NO_RECORD:"
            "SPECIAL_CURP_NOT_FOUND"
        ) from last_no_record

    if not obtained_lock:
        raise SideaBusy(
            "SIDEA_ALL_READY_ACCOUNTS_BUSY"
        )

    raise SideaNoRecord(
        "SIDEA_NO_RECORD:"
        "SPECIAL_CURP_NOT_FOUND"
    )


def sidea_generate_pdf_from_chain(
    pool: SideaPool,
    cadena: str,
    accounts: list[SideaAccount] | None = None,
    expected_acto: str | int | None = None,
    request_id: int | None = None,
    add_internal_folio: bool = False,
) -> dict:
    """
    Cadena Digital -> registro real -> flujo normal SIDEA.

    expected_acto evita imprimir una cadena perteneciente
    a un acto diferente al solicitado.
    """

    resolved = sidea_resolve_chain(
        pool=pool,
        cadena=cadena,
        accounts=accounts,
    )

    expected = str(
        expected_acto
        or ""
    ).strip()

    real_acto = str(
        resolved.get("acto")
        or ""
    ).strip()

    if (
        expected
        and real_acto != expected
    ):
        raise SideaError(
            "SIDEA_CHAIN_ACT_MISMATCH:"
            f"EXPECTED_{expected}:"
            f"REAL_{real_acto}"
        )

    result = sidea_generate_pdf(
        pool=pool,
        curp=resolved["curp"],
        entidad=resolved["entidad"],
        acto=real_acto,
        tipo="1",
        accounts=accounts,
        request_id=request_id,
        add_internal_folio=add_internal_folio,
    )

    result = dict(result)

    result[
        "resolved_from_chain"
    ] = True

    result[
        "input_chain"
    ] = cadena

    result[
        "resolved_acto"
    ] = real_acto

    return result
