from __future__ import annotations

import base64
import json
import random
import string
import time
import os
import uuid
import redis
import threading

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from hashlib import pbkdf2_hmac
from typing import Any, Optional

import requests
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


# Lock compartido entre TODAS las instancias del cliente
# dentro del mismo proceso del agente.
#
# Solo protege:
#   1. cambiar FOLIADO / REVERSADO
#   2. crear la solicitud
#
# NO protege polling ni descarga del PDF.
_PROVIDER15_LOCAL_FORMAT_LOCK = threading.Lock()

_PROVIDER15_LOCAL_LOCK_TOKEN = "__LOCAL_FORMAT_LOCK__"


class ProviderSidEmilianoClient:
    """Cliente para https://tramitesonlinemx.com.

    El frontend cifra nombres y valores de cada propiedad antes de enviarlos.
    Esquema reproducido del bundle web:
      - PBKDF2-HMAC-SHA1, 1000 iteraciones, clave de 16 bytes
      - AES-128-CBC
      - PKCS7
      - IV fijo hexadecimal
      - salida Base64
    """

    API_BASE_URL = "https://api.tramitesonlinemx.com"
    WEB_BASE_URL = "https://tramitesonlinemx.com"

    CRYPTO_PASSWORD = "0BY#c2CSiT24a"
    CRYPTO_SALT = "V@wnIEV$4N5f4XGnU19"
    CRYPTO_IV_HEX = "e84ad660c4721ae0e84ad660c4721ae0"
    PBKDF2_ITERATIONS = 1000
    AES_KEY_BYTES = 16

    def __init__(
        self,
        *,
        username: str,
        password: str,
        timeout_login: int = 30,
        timeout_query: int = 45,
        redis_url: Optional[str] = None,
    ) -> None:
        self.username = (username or "").strip()
        self.password = password or ""
        self.timeout_login = int(timeout_login)
        self.timeout_query = int(timeout_query)

        self.redis_url = (
            redis_url
            or os.getenv("REDIS_URL", "")
        ).strip()

        self.redis_client = None

        if self.redis_url:
            self.redis_client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
            )

        self.login_url = f"{self.API_BASE_URL}/api/users/login"
        self.balance_url = f"{self.API_BASE_URL}/api/users/balance"

        self.token: str = ""
        self.user_info: dict[str, Any] = {}

        self._session_ready: bool = False
        self._socket_sid: str = ""

        self._key = pbkdf2_hmac(
            "sha1",
            self.CRYPTO_PASSWORD.encode("utf-8"),
            self.CRYPTO_SALT.encode("utf-8"),
            self.PBKDF2_ITERATIONS,
            dklen=self.AES_KEY_BYTES,
        )
        self._iv = bytes.fromhex(self.CRYPTO_IV_HEX)

        self.session = requests.Session()
        self._configure_session()

    # =========================================================
    # HTTP
    # =========================================================
    def _configure_session(self) -> None:
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "PUT"]),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(self._public_headers())

    def _public_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9",
            "Content-Type": "application/json",
            "Origin": self.WEB_BASE_URL,
            "Referer": f"{self.WEB_BASE_URL}/",
            "User-Agent": DEFAULT_UA,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    def _auth_headers(self, face: str = "/app/home/dashboard") -> dict[str, str]:
        if not self.token:
            raise RuntimeError("PROVIDER15_TOKEN_MISSING")

        return {
            **self._public_headers(),
            "Authorization": self.token,
            "Face": face,
            "System-Origin": "Web",
            "Priority": "u=1, i",
            "Sec-CH-UA": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }

    def _sleep_jitter(self, low: float = 0.15, high: float = 0.7) -> None:
        if high > 0:
            time.sleep(random.uniform(low, high))

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        merged = dict(self.session.headers)
        if headers:
            merged.update(headers)

        return self.session.request(
            method=method.upper(),
            url=url,
            json=json_body,
            headers=merged,
            timeout=timeout or self.timeout_query,
        )

    # =========================================================
    # CIFRADO COMPATIBLE CON EL FRONTEND
    # =========================================================
    def encrypt_value(self, plaintext: str) -> str:
        raw = (plaintext or "").encode("utf-8")

        padder = padding.PKCS7(128).padder()
        padded = padder.update(raw) + padder.finalize()

        encryptor = Cipher(
            algorithms.AES(self._key),
            modes.CBC(self._iv),
        ).encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(encrypted).decode("ascii")

    def decrypt_value(self, ciphertext_b64: str) -> str:
        try:
            encrypted = base64.b64decode(ciphertext_b64, validate=True)
        except Exception as exc:
            raise RuntimeError("PROVIDER15_BAD_BASE64") from exc

        decryptor = Cipher(
            algorithms.AES(self._key),
            modes.CBC(self._iv),
        ).decryptor()
        padded = decryptor.update(encrypted) + decryptor.finalize()

        try:
            unpadder = padding.PKCS7(128).unpadder()
            raw = unpadder.update(padded) + unpadder.finalize()
        except Exception as exc:
            raise RuntimeError("PROVIDER15_BAD_PADDING_OR_CRYPTO_KEY") from exc

        return raw.decode("utf-8")

    @staticmethod
    def _json_stringify(value: Any) -> str:
        # Equivalente práctico a JSON.stringify para los tipos usados por esta API.
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def encrypt_object(self, obj: dict[str, Any]) -> dict[str, str]:
        encrypted: dict[str, str] = {}
        for key, value in obj.items():
            encrypted_key = self.encrypt_value(str(key))
            encrypted_value = self.encrypt_value(self._json_stringify(value))
            encrypted[encrypted_key] = encrypted_value
        return encrypted

    def decrypt_object(self, obj: dict[str, Any]) -> dict[str, Any]:
        decrypted: dict[str, Any] = {}

        for encrypted_key, encrypted_value in obj.items():
            key = self.decrypt_value(str(encrypted_key))
            raw_value = self.decrypt_value(str(encrypted_value))

            try:
                value = json.loads(raw_value)
            except Exception:
                value = raw_value

            decrypted[key] = value

        return decrypted

    # =========================================================
    # AUTENTICACIÓN
    # =========================================================
    def login(self) -> dict[str, Any]:
        if not self.username:
            raise RuntimeError("PROVIDER15_USERNAME_MISSING")
        if not self.password:
            raise RuntimeError("PROVIDER15_PASSWORD_MISSING")

        payload = self.encrypt_object(
            {
                "username": self.username,
                "password": self.password,
            }
        )

        print(
            "PROVIDER15_LOGIN_START =",
            {"url": self.login_url, "username": self.username},
            flush=True,
        )

        response = self._request(
            "POST",
            self.login_url,
            json_body=payload,
            headers=self._public_headers(),
            timeout=self.timeout_login,
        )

        print("PROVIDER15_LOGIN_STATUS =", response.status_code, flush=True)

        if response.status_code in (401, 403):
            raise RuntimeError("PROVIDER15_LOGIN_UNAUTHORIZED")
        if response.status_code == 429:
            raise RuntimeError("PROVIDER15_LOGIN_RATE_LIMIT")
        if response.status_code >= 400:
            raise RuntimeError(
                f"PROVIDER15_LOGIN_HTTP_{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            encrypted_response = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"PROVIDER15_LOGIN_NO_JSON: {(response.text or '')[:500]}"
            ) from exc

        if not isinstance(encrypted_response, dict):
            raise RuntimeError("PROVIDER15_LOGIN_BAD_RESPONSE_TYPE")

        decoded = self.decrypt_object(encrypted_response)
        data = decoded.get("data")

        if not isinstance(data, dict):
            raise RuntimeError(
                f"PROVIDER15_LOGIN_DATA_MISSING: keys={list(decoded.keys())}"
            )

        token = str(data.get("token") or "").strip()
        if not token:
            raise RuntimeError("PROVIDER15_LOGIN_TOKEN_MISSING")

        self.token = token
        self.user_info = data
        self._session_ready = False
        self._socket_sid = ""

        print(
            "PROVIDER15_LOGIN_OK =",
            {
                "id": data.get("id"),
                "username": data.get("username"),
                "rol": data.get("rol"),
                "premium": data.get("premium"),
                "token_present": True,
            },
            flush=True,
        )

        return {
            "ok": True,
            "user": {
                "id": data.get("id"),
                "username": data.get("username"),
                "rol": data.get("rol"),
                "premium": data.get("premium"),
                "modules": data.get("modules") or [],
                "systems": data.get("systems") or [],
                "preferences": data.get("preferences") or [],
            },
            "raw": {
                "data": {
                    k: v
                    for k, v in data.items()
                    if k != "token"
                },
                "error": decoded.get("error", ""),
                "modal": decoded.get("modal"),
                "redirect": decoded.get("redirect", ""),
            },
        }

    def ensure_login(self) -> None:
        if not self.token:
            self.login()

    def ensure_session(self) -> None:
        self.ensure_login()

        if self._session_ready:
            return

        self.initialize_session()

    @staticmethod
    def _socket_random_t(length: int = 8) -> str:
        return "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(length)
        )

    def initialize_session(self) -> None:
        if not self.token:
            raise RuntimeError("PROVIDER15_TOKEN_MISSING")

        socket_url = f"{self.API_BASE_URL}/socket.io/"

        socket_headers = {
            "Accept": "*/*",
            "Accept-Language": "es-ES,es;q=0.9",
            "Origin": self.WEB_BASE_URL,
            "Referer": f"{self.WEB_BASE_URL}/",
            "Priority": "u=1, i",
            "Sec-CH-UA": (
                '"Not;A=Brand";v="8", '
                '"Chromium";v="150", '
                '"Google Chrome";v="150"'
            ),
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": DEFAULT_UA,
        }

        # =====================================================
        # 1. ENGINE.IO HANDSHAKE
        # =====================================================

        handshake_response = self.session.get(
            socket_url,
            params={
                "EIO": "4",
                "transport": "polling",
                "t": self._socket_random_t(),
            },
            headers=socket_headers,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_SOCKET_HANDSHAKE_STATUS =",
            handshake_response.status_code,
            flush=True,
        )

        if handshake_response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_SOCKET_HANDSHAKE_HTTP_"
                f"{handshake_response.status_code}: "
                f"{(handshake_response.text or '')[:500]}"
            )

        handshake_body = handshake_response.text or ""

        if not handshake_body.startswith("0"):
            raise RuntimeError(
                "PROVIDER15_SOCKET_BAD_HANDSHAKE: "
                f"{handshake_body[:500]}"
            )

        try:
            handshake_data = json.loads(handshake_body[1:])
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_SOCKET_HANDSHAKE_BAD_JSON"
            ) from exc

        sid = str(handshake_data.get("sid") or "").strip()

        if not sid:
            raise RuntimeError("PROVIDER15_SOCKET_SID_MISSING")

        self._socket_sid = sid

        print(
            "PROVIDER15_SOCKET_HANDSHAKE_OK =",
            {
                "sid_present": True,
                "pingInterval": handshake_data.get("pingInterval"),
                "pingTimeout": handshake_data.get("pingTimeout"),
            },
            flush=True,
        )

        # =====================================================
        # 2. SOCKET.IO AUTH
        # =====================================================

        auth_packet = "40" + json.dumps(
            {
                "authorization": self.token,
                "username": self.username,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        socket_post_headers = {
            **socket_headers,
            "Content-Type": "text/plain;charset=UTF-8",
        }

        auth_response = self.session.post(
            socket_url,
            params={
                "EIO": "4",
                "transport": "polling",
                "t": self._socket_random_t(),
                "sid": sid,
            },
            headers=socket_post_headers,
            data=auth_packet,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_SOCKET_AUTH_STATUS =",
            auth_response.status_code,
            flush=True,
        )

        if auth_response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_SOCKET_AUTH_HTTP_"
                f"{auth_response.status_code}: "
                f"{(auth_response.text or '')[:500]}"
            )

        # =====================================================
        # 3. LEER CONFIRMACIÓN DEL NAMESPACE
        # =====================================================

        auth_poll = self.session.get(
            socket_url,
            params={
                "EIO": "4",
                "transport": "polling",
                "t": self._socket_random_t(),
                "sid": sid,
            },
            headers=socket_headers,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_SOCKET_AUTH_POLL_STATUS =",
            auth_poll.status_code,
            flush=True,
        )

        if auth_poll.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_SOCKET_AUTH_POLL_HTTP_"
                f"{auth_poll.status_code}: "
                f"{(auth_poll.text or '')[:500]}"
            )

        auth_poll_body = auth_poll.text or ""

        if not auth_poll_body.startswith("40"):
            raise RuntimeError(
                "PROVIDER15_SOCKET_NAMESPACE_NOT_CONNECTED: "
                f"{auth_poll_body[:500]}"
            )

        # =====================================================
        # 4. user:connect CIFRADO
        # =====================================================

        user_id = self.user_info.get("id")
        username = self.user_info.get("username")
        rol = self.user_info.get("rol")

        if user_id is None:
            raise RuntimeError("PROVIDER15_USER_ID_MISSING")

        if not username:
            raise RuntimeError("PROVIDER15_USER_USERNAME_MISSING")

        if not rol:
            raise RuntimeError("PROVIDER15_USER_ROLE_MISSING")

        face = "/app/home/dashboard"

        user_connect_payload = [
            user_id,
            username,
            rol,
            face,
        ]

        encrypted_event = self.encrypt_value("user:connect")

        encrypted_payload = self.encrypt_value(
            json.dumps(
                user_connect_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        user_connect_packet = "42" + json.dumps(
            [
                encrypted_event,
                encrypted_payload,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

        user_connect_response = self.session.post(
            socket_url,
            params={
                "EIO": "4",
                "transport": "polling",
                "t": self._socket_random_t(),
                "sid": sid,
            },
            headers=socket_post_headers,
            data=user_connect_packet,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_USER_CONNECT_STATUS =",
            user_connect_response.status_code,
            flush=True,
        )

        if user_connect_response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_USER_CONNECT_HTTP_"
                f"{user_connect_response.status_code}: "
                f"{(user_connect_response.text or '')[:500]}"
            )

        # =====================================================
        # 5. LEER PING Y CONTESTAR PONG
        # =====================================================

        ping_response = self.session.get(
            socket_url,
            params={
                "EIO": "4",
                "transport": "polling",
                "t": self._socket_random_t(),
                "sid": sid,
            },
            headers=socket_headers,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_SOCKET_POST_CONNECT_POLL_STATUS =",
            ping_response.status_code,
            flush=True,
        )

        if ping_response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_SOCKET_POST_CONNECT_POLL_HTTP_"
                f"{ping_response.status_code}: "
                f"{(ping_response.text or '')[:500]}"
            )

        ping_body = ping_response.text or ""

        print(
            "PROVIDER15_SOCKET_POST_CONNECT_BODY =",
            ping_body[:200],
            flush=True,
        )

        # Engine.IO:
        # servidor -> "2" = ping
        # cliente  -> "3" = pong
        if ping_body.startswith("2"):
            pong_response = self.session.post(
                socket_url,
                params={
                    "EIO": "4",
                    "transport": "polling",
                    "t": self._socket_random_t(),
                    "sid": sid,
                },
                headers=socket_post_headers,
                data="3",
                timeout=self.timeout_query,
            )

            print(
                "PROVIDER15_SOCKET_PONG_STATUS =",
                pong_response.status_code,
                flush=True,
            )

            if pong_response.status_code != 200:
                raise RuntimeError(
                    "PROVIDER15_SOCKET_PONG_HTTP_"
                    f"{pong_response.status_code}: "
                    f"{(pong_response.text or '')[:500]}"
                )

        # =====================================================
        # 6. VALIDAR QUE EL BACKEND YA NOS RECONOCE
        # =====================================================

        validation_response = self.session.get(
            f"{self.API_BASE_URL}/api/users/queue/list",
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
            allow_redirects=False,
        )

        print(
            "PROVIDER15_SESSION_INIT_STATUS =",
            validation_response.status_code,
            flush=True,
        )

        if validation_response.status_code != 200:
            try:
                decoded = self.decrypt_object(
                    validation_response.json()
                )
            except Exception:
                decoded = (
                    validation_response.text or ""
                )[:500]

            raise RuntimeError(
                "PROVIDER15_SESSION_INIT_HTTP_"
                f"{validation_response.status_code}: "
                f"{decoded}"
            )

        self._session_ready = True

        print(
            "PROVIDER15_SESSION_READY = True",
            flush=True,
        )

    # =========================================================
    # LOCK DISTRIBUIDO PARA FORMATO + REQUEST
    # =========================================================

    PROVIDER15_FORMAT_LOCK_KEY = (
        "provider15:actas:format_request_lock"
    )

    def _acquire_format_request_lock(
        self,
        *,
        wait_seconds: float = 60.0,
        lock_seconds: int = 90,
    ) -> Optional[str]:

        # Para tests locales sin Redis:
        # simplemente no se aplica lock.
        if self.redis_client is None:
            acquired = _PROVIDER15_LOCAL_FORMAT_LOCK.acquire(
                timeout=float(wait_seconds)
            )
        
            if not acquired:
                raise RuntimeError(
                    "PROVIDER15_LOCAL_FORMAT_LOCK_TIMEOUT"
                )
        
            print(
                "PROVIDER15_LOCAL_FORMAT_LOCK_ACQUIRED = True",
                flush=True,
            )
        
            return _PROVIDER15_LOCAL_LOCK_TOKEN

        token = uuid.uuid4().hex

        deadline = (
            time.monotonic()
            + float(wait_seconds)
        )

        while time.monotonic() < deadline:

            acquired = self.redis_client.set(
                self.PROVIDER15_FORMAT_LOCK_KEY,
                token,
                nx=True,
                ex=int(lock_seconds),
            )

            if acquired:
                print(
                    "PROVIDER15_FORMAT_LOCK_ACQUIRED =",
                    {
                        "key": self.PROVIDER15_FORMAT_LOCK_KEY,
                    },
                    flush=True,
                )

                return token

            time.sleep(0.1)

        raise RuntimeError(
            "PROVIDER15_FORMAT_LOCK_TIMEOUT"
        )


    def _release_format_request_lock(
        self,
        token: Optional[str],
    ) -> None:

        if not token:
            return
        
        if token == _PROVIDER15_LOCAL_LOCK_TOKEN:
            try:
                _PROVIDER15_LOCAL_FORMAT_LOCK.release()
        
                print(
                    "PROVIDER15_LOCAL_FORMAT_LOCK_RELEASED = True",
                    flush=True,
                )
        
            except RuntimeError as exc:
                print(
                    "PROVIDER15_LOCAL_FORMAT_LOCK_RELEASE_ERROR =",
                    repr(exc),
                    flush=True,
                )
        
            return
        
        if self.redis_client is None:
            return

        # Solo elimina el lock si todavía
        # pertenece a esta ejecución.
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

        try:
            released = self.redis_client.eval(
                release_script,
                1,
                self.PROVIDER15_FORMAT_LOCK_KEY,
                token,
            )

            print(
                "PROVIDER15_FORMAT_LOCK_RELEASED =",
                {
                    "released": bool(released),
                },
                flush=True,
            )

        except Exception as exc:
            print(
                "PROVIDER15_FORMAT_LOCK_RELEASE_ERROR =",
                repr(exc),
                flush=True,
            )

    def set_acta_format(
        self,
        acta_format: str,
    ) -> dict[str, Any]:

        acta_format = str(
            acta_format or ""
        ).strip().upper()

        preferences = {
            "REVERSADO": 3,
            "FOLIADO": 4,
        }

        if acta_format not in preferences:
            raise RuntimeError(
                "PROVIDER15_ACTA_FORMAT_INVALID: "
                f"{acta_format}"
            )

        preference = preferences[
            acta_format
        ]

        self.ensure_session()

        face = "/app/docs/registro-civil"

        payload = self.encrypt_object(
            {
                "section": "ACTAS",
                "preference": preference,
            }
        )

        response = self._request(
            "PUT",
            f"{self.API_BASE_URL}"
            f"/api/users/preferences/set",
            json_body=payload,
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_ACTA_FORMAT_STATUS =",
            response.status_code,
            flush=True,
        )

        if response.status_code in (
            307,
            401,
            403,
        ):
            print(
                "PROVIDER15_ACTA_FORMAT_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "PUT",
                f"{self.API_BASE_URL}"
                f"/api/users/preferences/set",
                json_body=payload,
                headers=self._auth_headers(face),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError(
                "PROVIDER15_ACTA_FORMAT_RATE_LIMIT"
            )

        if response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_ACTA_FORMAT_HTTP_"
                f"{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            decoded = self.decrypt_object(
                response.json()
            )

        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_ACTA_FORMAT_DECRYPT_FAILED"
            ) from exc

        error = str(
            decoded.get("error")
            or ""
        ).strip()

        if error:
            raise RuntimeError(
                "PROVIDER15_ACTA_FORMAT_ERROR: "
                f"{error}"
            )

        print(
            "PROVIDER15_ACTA_FORMAT_SET =",
            {
                "format": acta_format,
                "preference": preference,
            },
            flush=True,
        )

        return {
            "ok": True,
            "format": acta_format,
            "preference": preference,
            "raw": decoded,
        }

    def request_certificate(
        self,
        curp: str,
        certificate_type: str,
    ) -> dict[str, Any]:

        curp = (curp or "").strip().upper()
        certificate_type = (
            certificate_type
            or ""
        ).strip().upper()

        if not curp:
            raise RuntimeError(
                "PROVIDER15_CURP_MISSING"
            )

        allowed_types = {
            "NACIMIENTO",
            "MATRIMONIO",
            "DEFUNCION",
            "DIVORCIO",
        }

        if certificate_type not in allowed_types:
            raise RuntimeError(
                "PROVIDER15_CERTIFICATE_TYPE_INVALID: "
                f"{certificate_type}"
            )

        self.ensure_session()

        face = "/app/docs/registro-civil"

        payload = self.encrypt_object(
            {
                "search": "CURP",
                "type": certificate_type,
                "data": {
                    "curp": curp,
                },
            }
        )

        response = self._request(
            "POST",
            f"{self.API_BASE_URL}/api/registro-civil/actas/request",
            json_body=payload,
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_ACTA_REQUEST_STATUS =",
            response.status_code,
            flush=True,
        )

        if response.status_code in (307, 401, 403):
            print(
                "PROVIDER15_ACTA_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "POST",
                f"{self.API_BASE_URL}/api/registro-civil/actas/request",
                json_body=payload,
                headers=self._auth_headers(face),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError(
                "PROVIDER15_ACTA_REQUEST_RATE_LIMIT"
            )

        if response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_ACTA_REQUEST_HTTP_"
                f"{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            body = response.json()
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_ACTA_REQUEST_NO_JSON: "
                f"{(response.text or '')[:500]}"
            ) from exc

        try:
            decoded = self.decrypt_object(
                body
            )
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_ACTA_REQUEST_DECRYPT_FAILED"
            ) from exc

        print(
            "PROVIDER15_ACTA_REQUEST_RESULT =",
            decoded,
            flush=True,
        )

        data = decoded.get("data")

        request_id = None
        estimated_value = None

        if isinstance(data, list):
            if len(data) >= 1:
                request_id = data[0]

            if len(data) >= 2:
                estimated_value = data[1]

        return {
            "ok": True,
            "request_id": request_id,
            "estimated_value": estimated_value,
            "certificate_type": certificate_type,
            "raw": decoded,
        }

    def request_by_chain(
        self,
        cadena: str,
    ) -> dict[str, Any]:

        cadena = str(
            cadena or ""
        ).strip()

        if not cadena:
            raise RuntimeError(
                "PROVIDER15_CHAIN_MISSING"
            )

        self.ensure_session()

        face = "/app/docs/registro-civil"

        # La web envía exactamente:
        # search = CADENA
        # type = null
        # data = {"cadena": "..."}
        payload = self.encrypt_object(
            {
                "search": "CADENA",
                "type": None,
                "data": {
                    "cadena": cadena,
                },
            }
        )

        response = self._request(
            "POST",
            f"{self.API_BASE_URL}"
            f"/api/registro-civil/actas/request",
            json_body=payload,
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_CHAIN_REQUEST_STATUS =",
            response.status_code,
            flush=True,
        )

        if response.status_code in (
            307,
            401,
            403,
        ):
            print(
                "PROVIDER15_CHAIN_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "POST",
                f"{self.API_BASE_URL}"
                f"/api/registro-civil/actas/request",
                json_body=payload,
                headers=self._auth_headers(face),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError(
                "PROVIDER15_CHAIN_RATE_LIMIT"
            )

        if response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_CHAIN_HTTP_"
                f"{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            body = response.json()

        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_CHAIN_NO_JSON: "
                f"{(response.text or '')[:500]}"
            ) from exc

        try:
            decoded = self.decrypt_object(
                body
            )

        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_CHAIN_DECRYPT_FAILED"
            ) from exc

        print(
            "PROVIDER15_CHAIN_REQUEST_RESULT =",
            decoded,
            flush=True,
        )

        data = decoded.get("data")

        provider_uuid = None
        estimated_value = None

        if isinstance(
            data,
            list,
        ):
            if len(data) >= 1:
                provider_uuid = data[0]

            if len(data) >= 2:
                estimated_value = data[1]

        return {
            "ok": True,
            "search": "CADENA",
            "cadena": cadena,
            "request_id": provider_uuid,
            "estimated_value": (
                estimated_value
            ),
            "raw": decoded,
        }

    def get_requests(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:

        start_date = (start_date or "").strip()
        end_date = (end_date or "").strip()

        if not start_date:
            raise RuntimeError("PROVIDER15_START_DATE_MISSING")

        if not end_date:
            raise RuntimeError("PROVIDER15_END_DATE_MISSING")

        self.ensure_session()

        face = "/app/docs/registro-civil"

        url = (
            f"{self.API_BASE_URL}"
            f"/api/registro-civil/list/get/"
            f"{start_date}/{end_date}"
        )

        response = self._request(
            "GET",
            url,
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_REQUESTS_LIST_STATUS =",
            response.status_code,
            flush=True,
        )

        if response.status_code in (307, 401, 403):
            print(
                "PROVIDER15_REQUESTS_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "GET",
                url,
                headers=self._auth_headers(face),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError(
                "PROVIDER15_REQUESTS_RATE_LIMIT"
            )

        if response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_REQUESTS_HTTP_"
                f"{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            body = response.json()
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_REQUESTS_NO_JSON: "
                f"{(response.text or '')[:500]}"
            ) from exc

        try:
            decoded = self.decrypt_object(body)
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_REQUESTS_DECRYPT_FAILED"
            ) from exc

        rows = decoded.get("data")

        print(
            "PROVIDER15_REQUESTS_RESULT =",
            {
                "count": (
                    len(rows)
                    if isinstance(rows, list)
                    else 0
                ),
                "error": decoded.get("error"),
                "redirect": decoded.get("redirect"),
            },
            flush=True,
        )

        return decoded

    def download_acta(
        self,
        request_id: str,
        *,
        output_path: Optional[str] = None,
    ) -> dict[str, Any]:

        request_id = str(request_id or "").strip()

        if not request_id:
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_REQUEST_ID_MISSING"
            )

        self.ensure_session()

        face = "/app/docs/registro-civil"

        download_api_url = (
            f"{self.API_BASE_URL}"
            f"/api/registro-civil/actas/download/"
            f"{request_id}"
        )

        # =====================================================
        # 1. PEDIR URL FIRMADA DEL PDF
        # =====================================================

        response = self._request(
            "GET",
            download_api_url,
            headers=self._auth_headers(face),
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_DOWNLOAD_URL_STATUS =",
            response.status_code,
            flush=True,
        )

        if response.status_code in (307, 401, 403):
            print(
                "PROVIDER15_DOWNLOAD_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "GET",
                download_api_url,
                headers=self._auth_headers(face),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_RATE_LIMIT"
            )

        # =====================================================
        # EL REGISTRO PUEDE ESTAR "OK" ANTES DE QUE EL PDF
        # TERMINE DE ESTAR DISPONIBLE EN STORAGE.
        # =====================================================

        if response.status_code == 404:

            decoded_404 = None

            try:
                body_404 = response.json()

                if isinstance(
                    body_404,
                    dict,
                ):
                    decoded_404 = (
                        self.decrypt_object(
                            body_404
                        )
                    )

            except Exception:
                decoded_404 = None

            error_404 = ""

            if isinstance(
                decoded_404,
                dict,
            ):
                error_404 = str(
                    decoded_404.get(
                        "error"
                    )
                    or ""
                ).strip()

            print(
                "PROVIDER15_DOWNLOAD_NOT_READY =",
                {
                    "request_id": request_id,
                    "error": (
                        error_404
                        or "HTTP 404"
                    ),
                },
                flush=True,
            )

            if (
                "acta no encontrada"
                in error_404.lower()
            ):
                raise RuntimeError(
                    "PROVIDER15_DOWNLOAD_NOT_READY"
                )

            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_HTTP_404: "
                f"{decoded_404 or (response.text or '')[:500]}"
            )

        if response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_HTTP_"
                f"{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            body = response.json()
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_NO_JSON: "
                f"{(response.text or '')[:500]}"
            ) from exc

        try:
            decoded = self.decrypt_object(body)
        except Exception as exc:
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_DECRYPT_FAILED"
            ) from exc

        print(
            "PROVIDER15_DOWNLOAD_RESULT =",
            {
                "data_present": bool(decoded.get("data")),
                "error": decoded.get("error"),
                "redirect": decoded.get("redirect"),
            },
            flush=True,
        )

        signed_url = decoded.get("data")

        if not isinstance(signed_url, str):
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_URL_MISSING: "
                f"{decoded}"
            )

        signed_url = signed_url.strip()

        if not signed_url.startswith("http"):
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_BAD_URL"
            )

        # =====================================================
        # 2. DESCARGAR PDF DESDE R2
        # =====================================================

        pdf_headers = {
            "Accept": "*/*",
            "Origin": self.WEB_BASE_URL,
            "Referer": f"{self.WEB_BASE_URL}/",
            "User-Agent": DEFAULT_UA,
        }

        pdf_response = requests.get(
            signed_url,
            headers=pdf_headers,
            timeout=self.timeout_query,
        )

        print(
            "PROVIDER15_PDF_STATUS =",
            pdf_response.status_code,
            flush=True,
        )

        if pdf_response.status_code != 200:
            raise RuntimeError(
                "PROVIDER15_PDF_HTTP_"
                f"{pdf_response.status_code}"
            )

        pdf_bytes = pdf_response.content

        if not pdf_bytes:
            raise RuntimeError(
                "PROVIDER15_PDF_EMPTY"
            )

        content_type = (
            pdf_response.headers.get(
                "Content-Type",
                "",
            )
            .lower()
            .strip()
        )

        print(
            "PROVIDER15_PDF_INFO =",
            {
                "bytes": len(pdf_bytes),
                "content_type": content_type,
                "starts_pdf": pdf_bytes.startswith(b"%PDF"),
            },
            flush=True,
        )

        if (
            "application/pdf" not in content_type
            and not pdf_bytes.startswith(b"%PDF")
        ):
            raise RuntimeError(
                "PROVIDER15_DOWNLOAD_NOT_PDF"
            )

        # =====================================================
        # 3. GUARDAR OPCIONALMENTE
        # =====================================================

        if output_path:
            with open(output_path, "wb") as f:
                f.write(pdf_bytes)

            print(
                "PROVIDER15_PDF_SAVED =",
                output_path,
                flush=True,
            )

        return {
            "ok": True,
            "request_id": request_id,
            "signed_url": signed_url,
            "pdf_bytes": pdf_bytes,
            "size": len(pdf_bytes),
            "content_type": content_type,
            "output_path": output_path,
        }

    def process_certificate(
        self,
        curp: str,
        certificate_type: str,
        *,
        acta_format: str = "REVERSADO",
        timeout_seconds: int = 120,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:

        curp = (curp or "").strip().upper()

        certificate_type = (
            certificate_type
            or ""
        ).strip().upper()

        if not curp:
            raise RuntimeError(
                "PROVIDER15_PROCESS_CURP_MISSING"
            )

        allowed_types = {
            "NACIMIENTO",
            "MATRIMONIO",
            "DEFUNCION",
            "DIVORCIO",
        }

        if certificate_type not in allowed_types:
            raise RuntimeError(
                "PROVIDER15_PROCESS_TYPE_INVALID: "
                f"{certificate_type}"
            )

        timeout_seconds = max(
            10,
            int(timeout_seconds),
        )

        poll_interval = max(
            0.5,
            float(poll_interval),
        )

        print(
            "PROVIDER15_PROCESS_START =",
            {
                "curp": curp,
                "certificate_type": (
                    certificate_type
                ),
                "timeout_seconds": (
                    timeout_seconds
                ),
                "poll_interval": (
                    poll_interval
                ),
            },
            flush=True,
        )

        try:
            provider_tz = ZoneInfo(
                "America/Matamoros"
            )

            now_provider = datetime.now(
                provider_tz
            )

        except Exception:
            now_provider = datetime.now()

        start_date = (
            now_provider.strftime(
                "%Y-%m-%d"
            )
        )

        end_date = (
            now_provider
            + timedelta(days=1)
        ).strftime(
            "%Y-%m-%d"
        )

        # =====================================================
        # IDS EXISTENTES
        # =====================================================

        before_result = self.get_requests(
            start_date,
            end_date,
        )

        before_rows = (
            before_result.get("data")
        )

        if not isinstance(
            before_rows,
            list,
        ):
            before_rows = []

        existing_ids = {
            str(
                row.get("id")
                or ""
            ).strip()

            for row in before_rows

            if isinstance(
                row,
                dict,
            )
        }

        existing_ids.discard("")

        print(
            "PROVIDER15_PROCESS_EXISTING_IDS =",
            len(existing_ids),
            flush=True,
        )

        # =====================================================
        # CREAR SOLICITUD
        # =====================================================

        format_lock_token = (
            self._acquire_format_request_lock()
        )

        try:

            self.set_acta_format(
                acta_format
            )

            request_result = (
                self.request_certificate(
                    curp,
                    certificate_type,
                )
            )

        finally:

            self._release_format_request_lock(
                format_lock_token
            )

        provider_uuid = str(
            request_result.get(
                "request_id"
            )
            or ""
        ).strip()

        print(
            "PROVIDER15_PROCESS_REQUEST_CREATED =",
            {
                "curp": curp,
                "certificate_type": (
                    certificate_type
                ),
                "provider_uuid": (
                    provider_uuid
                    or None
                ),
            },
            flush=True,
        )

        # =====================================================
        # POLLING
        # =====================================================

        deadline = (
            time.monotonic()
            + timeout_seconds
        )

        last_matching_row = None
        poll_number = 0

        while (
            time.monotonic()
            < deadline
        ):
            poll_number += 1

            time.sleep(
                poll_interval
            )

            try:
                result = self.get_requests(
                    start_date,
                    end_date,
                )

            except Exception as exc:
                print(
                    "PROVIDER15_PROCESS_POLL_ERROR =",
                    {
                        "poll": poll_number,
                        "error": repr(exc),
                    },
                    flush=True,
                )

                continue

            rows = result.get("data")

            if not isinstance(
                rows,
                list,
            ):
                rows = []

            matching_rows = []

            for row in rows:
                if not isinstance(
                    row,
                    dict,
                ):
                    continue

                row_id = str(
                    row.get("id")
                    or ""
                ).strip()

                row_curp = str(
                    row.get("curp")
                    or ""
                ).strip().upper()

                row_type = str(
                    row.get("type")
                    or ""
                ).strip().upper()

                row_search = str(
                    row.get("search")
                    or ""
                ).strip().upper()

                row_source = str(
                    row.get("systemsource")
                    or ""
                ).strip().upper()

                if row_curp != curp:
                    continue

                if (
                    row_type
                    != certificate_type
                ):
                    continue

                if row_search != "CURP":
                    continue

                # Las solicitudes que hacemos desde este cliente
                # salen como System-Origin: Web.
                if row_source != "WEB":
                    continue

                if (
                    row_id
                    and row_id
                    in existing_ids
                ):
                    continue

                matching_rows.append(
                    row
                )

            if not matching_rows:
                print(
                    "PROVIDER15_PROCESS_WAITING =",
                    {
                        "curp": curp,
                        "certificate_type": (
                            certificate_type
                        ),
                        "poll": poll_number,
                    },
                    flush=True,
                )

                continue

            def row_sort_key(
                row: dict[str, Any],
            ) -> int:
                try:
                    return int(
                        str(
                            row.get("id")
                            or "0"
                        )
                    )
                except Exception:
                    return 0

            matching_rows.sort(
                key=row_sort_key,
                reverse=True,
            )

            row = matching_rows[0]
            last_matching_row = row

            row_id = str(
                row.get("id")
                or ""
            ).strip()

            status = str(
                row.get("status")
                or ""
            ).strip()

            status_upper = (
                status.upper()
            )

            print(
                "PROVIDER15_PROCESS_RESULT_FOUND =",
                {
                    "curp": curp,
                    "certificate_type": (
                        certificate_type
                    ),
                    "id": row_id,
                    "status": status,
                    "poll": poll_number,
                },
                flush=True,
            )

            # =================================================
            # OK
            # =================================================

            if status_upper == "OK":

                if not row_id:
                    raise RuntimeError(
                        "PROVIDER15_RESULT_ID_MISSING"
                    )

                # =============================================
                # LA FILA PUEDE MARCAR OK ANTES DE QUE STORAGE
                # TENGA DISPONIBLE EL PDF.
                # =============================================

                pdf_wait_deadline = (
                    time.monotonic()
                    + 120
                )

                pdf_attempt = 0
                pdf_result = None

                while (
                    time.monotonic()
                    < pdf_wait_deadline
                ):
                    pdf_attempt += 1

                    try:
                        pdf_result = (
                            self.download_acta(
                                row_id
                            )
                        )

                        break

                    except RuntimeError as exc:

                        error_text = str(
                            exc
                        )

                        if (
                            "PROVIDER15_DOWNLOAD_NOT_READY"
                            not in error_text
                        ):
                            raise

                        print(
                            "PROVIDER15_PROCESS_PDF_WAITING =",
                            {
                                "curp": curp,
                                "certificate_type": (
                                    certificate_type
                                ),
                                "provider_request_id": (
                                    row_id
                                ),
                                "attempt": (
                                    pdf_attempt
                                ),
                            },
                            flush=True,
                        )

                        time.sleep(
                            2.0
                        )

                if pdf_result is None:

                    print(
                        "PROVIDER15_PROCESS_PDF_TIMEOUT =",
                        {
                            "curp": curp,
                            "certificate_type": (
                                certificate_type
                            ),
                            "provider_request_id": (
                                row_id
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": False,
                        "status": (
                            "download_timeout"
                        ),
                        "curp": curp,
                        "certificate_type": (
                            certificate_type
                        ),
                        "provider_uuid": (
                            provider_uuid
                            or None
                        ),
                        "provider_request_id": (
                            row_id
                        ),
                        "message": (
                            "El proveedor marcó "
                            "la solicitud como OK, "
                            "pero el PDF no estuvo "
                            "disponible a tiempo."
                        ),
                        "raw_row": row,
                    }

                pdf_bytes = (
                    pdf_result.get(
                        "pdf_bytes"
                    )
                )

                if not isinstance(
                    pdf_bytes,
                    bytes,
                ):
                    raise RuntimeError(
                        "PROVIDER15_RESULT_PDF_BYTES_MISSING"
                    )

                if not pdf_bytes.startswith(
                    b"%PDF"
                ):
                    raise RuntimeError(
                        "PROVIDER15_RESULT_INVALID_PDF"
                    )

                final_result = {
                    "ok": True,
                    "status": "ok",

                    "curp": curp,

                    "certificate_type": (
                        certificate_type
                    ),

                    "provider_uuid": (
                        provider_uuid
                        or None
                    ),

                    "provider_request_id": (
                        row_id
                    ),

                    "type": row.get(
                        "type"
                    ),

                    "search": row.get(
                        "search"
                    ),

                    "cadena": row.get(
                        "cadena"
                    ),

                    "nombre": row.get(
                        "nombre"
                    ),

                    "apellidos": row.get(
                        "apellidos"
                    ),

                    "fechanac": row.get(
                        "fechanac"
                    ),

                    "price": row.get(
                        "price"
                    ),

                    "systemsource": row.get(
                        "systemsource"
                    ),

                    "created_at": row.get(
                        "createdAt"
                    ),

                    "pdf_bytes": (
                        pdf_bytes
                    ),

                    "pdf_size": len(
                        pdf_bytes
                    ),

                    "raw_row": row,
                }

                print(
                    "PROVIDER15_PROCESS_SUCCESS =",
                    {
                        "curp": curp,
                        "certificate_type": (
                            certificate_type
                        ),
                        "provider_request_id": (
                            row_id
                        ),
                        "provider_uuid": (
                            provider_uuid
                            or None
                        ),
                        "pdf_size": len(
                            pdf_bytes
                        ),
                    },
                    flush=True,
                )

                return final_result

            # =================================================
            # NO ENCONTRADO
            # =================================================

            status_lower = (
                status.lower()
            )

            if (
                "no encontrado"
                in status_lower
            ):
                print(
                    "PROVIDER15_PROCESS_NOT_FOUND =",
                    {
                        "curp": curp,
                        "certificate_type": (
                            certificate_type
                        ),
                        "provider_request_id": (
                            row_id
                        ),
                        "message": status,
                    },
                    flush=True,
                )

                return {
                    "ok": False,
                    "status": "not_found",

                    "curp": curp,

                    "certificate_type": (
                        certificate_type
                    ),

                    "provider_uuid": (
                        provider_uuid
                        or None
                    ),

                    "provider_request_id": (
                        row_id
                        or None
                    ),

                    "message": status,

                    "raw_row": row,
                }

            # =================================================
            # OTRO ESTADO
            # =================================================

            print(
                "PROVIDER15_PROCESS_PENDING_STATUS =",
                {
                    "curp": curp,
                    "certificate_type": (
                        certificate_type
                    ),
                    "provider_request_id": (
                        row_id
                    ),
                    "status": status,
                    "poll": poll_number,
                },
                flush=True,
            )

        # =====================================================
        # TIMEOUT
        # =====================================================

        print(
            "PROVIDER15_PROCESS_TIMEOUT =",
            {
                "curp": curp,
                "certificate_type": (
                    certificate_type
                ),
                "provider_uuid": (
                    provider_uuid
                    or None
                ),
                "last_row": (
                    last_matching_row
                ),
            },
            flush=True,
        )

        return {
            "ok": False,
            "status": "timeout",

            "curp": curp,

            "certificate_type": (
                certificate_type
            ),

            "provider_uuid": (
                provider_uuid
                or None
            ),

            "message": (
                "Provider 15 no terminó "
                "la solicitud dentro del "
                "tiempo configurado."
            ),

            "raw_row": (
                last_matching_row
            ),
        }

    def process_by_chain(
        self,
        cadena: str,
        *,
        acta_format: str = "REVERSADO",
        timeout_seconds: int = 120,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:

        cadena = str(
            cadena or ""
        ).strip()

        if not cadena:
            raise RuntimeError(
                "PROVIDER15_PROCESS_CHAIN_MISSING"
            )

        timeout_seconds = max(
            10,
            int(timeout_seconds),
        )

        poll_interval = max(
            0.5,
            float(poll_interval),
        )

        print(
            "PROVIDER15_CHAIN_PROCESS_START =",
            {
                "cadena": cadena,
                "timeout_seconds": (
                    timeout_seconds
                ),
                "poll_interval": (
                    poll_interval
                ),
            },
            flush=True,
        )

        # =====================================================
        # FECHAS PARA LISTADO
        # =====================================================

        try:
            provider_tz = ZoneInfo(
                "America/Matamoros"
            )

            now_provider = datetime.now(
                provider_tz
            )

        except Exception:
            now_provider = datetime.now()

        start_date = (
            now_provider.strftime(
                "%Y-%m-%d"
            )
        )

        end_date = (
            now_provider
            + timedelta(days=1)
        ).strftime(
            "%Y-%m-%d"
        )

        # =====================================================
        # IDS QUE YA EXISTÍAN ANTES DE CREAR LA SOLICITUD
        # =====================================================

        before_result = self.get_requests(
            start_date,
            end_date,
        )

        before_rows = (
            before_result.get("data")
        )

        if not isinstance(
            before_rows,
            list,
        ):
            before_rows = []

        existing_ids = {
            str(
                row.get("id")
                or ""
            ).strip()

            for row in before_rows

            if isinstance(
                row,
                dict,
            )
        }

        existing_ids.discard("")

        print(
            "PROVIDER15_CHAIN_EXISTING_IDS =",
            len(existing_ids),
            flush=True,
        )

        # =====================================================
        # CREAR SOLICITUD POR CADENA
        # =====================================================

        format_lock_token = (
            self._acquire_format_request_lock()
        )

        try:

            self.set_acta_format(
                acta_format
            )

            request_result = (
                self.request_by_chain(
                    cadena
                )
            )

        finally:

            self._release_format_request_lock(
                format_lock_token
            )

        provider_uuid = str(
            request_result.get(
                "request_id"
            )
            or ""
        ).strip()

        print(
            "PROVIDER15_CHAIN_REQUEST_CREATED =",
            {
                "cadena": cadena,
                "provider_uuid": (
                    provider_uuid
                    or None
                ),
            },
            flush=True,
        )

        # =====================================================
        # ESPERAR LA NUEVA FILA
        # =====================================================

        deadline = (
            time.monotonic()
            + timeout_seconds
        )

        poll_number = 0
        last_matching_row = None

        while (
            time.monotonic()
            < deadline
        ):
            poll_number += 1

            time.sleep(
                poll_interval
            )

            try:
                result = self.get_requests(
                    start_date,
                    end_date,
                )

            except Exception as exc:
                print(
                    "PROVIDER15_CHAIN_POLL_ERROR =",
                    {
                        "poll": poll_number,
                        "error": repr(exc),
                    },
                    flush=True,
                )

                continue

            rows = result.get("data")

            if not isinstance(
                rows,
                list,
            ):
                rows = []

            matching_rows = []

            for row in rows:

                if not isinstance(
                    row,
                    dict,
                ):
                    continue

                row_id = str(
                    row.get("id")
                    or ""
                ).strip()

                row_chain = str(
                    row.get("cadena")
                    or ""
                ).strip()

                row_search = str(
                    row.get("search")
                    or ""
                ).strip().upper()

                row_source = str(
                    row.get("systemsource")
                    or ""
                ).strip().upper()

                # CADENA EXACTA
                if row_chain != cadena:
                    continue

                # Solo solicitudes por CADENA
                if row_search != "CADENA":
                    continue

                # Nuestro cliente usa System-Origin: Web
                if row_source != "WEB":
                    continue

                # No aceptar registros que ya existían
                # antes de esta solicitud.
                if (
                    row_id
                    and row_id
                    in existing_ids
                ):
                    continue

                matching_rows.append(
                    row
                )

            if not matching_rows:

                print(
                    "PROVIDER15_CHAIN_WAITING =",
                    {
                        "cadena": cadena,
                        "poll": poll_number,
                    },
                    flush=True,
                )

                continue

            # Si por alguna razón llegan varias,
            # nos quedamos con el ID nuevo más alto.
            def row_sort_key(
                row: dict[str, Any],
            ) -> int:

                try:
                    return int(
                        str(
                            row.get("id")
                            or "0"
                        )
                    )

                except Exception:
                    return 0

            matching_rows.sort(
                key=row_sort_key,
                reverse=True,
            )

            row = matching_rows[0]
            last_matching_row = row

            row_id = str(
                row.get("id")
                or ""
            ).strip()

            status = str(
                row.get("status")
                or ""
            ).strip()

            status_upper = (
                status.upper()
            )

            print(
                "PROVIDER15_CHAIN_RESULT_FOUND =",
                {
                    "cadena": cadena,
                    "id": row_id,
                    "type": row.get(
                        "type"
                    ),
                    "status": status,
                    "poll": poll_number,
                },
                flush=True,
            )

            # =================================================
            # DOCUMENTO ENCONTRADO
            # =================================================

            if status_upper == "OK":

                if not row_id:
                    raise RuntimeError(
                        "PROVIDER15_CHAIN_RESULT_ID_MISSING"
                    )

                # =============================================
                # ESPERAR PDF
                #
                # Igual que CURP:
                # la fila puede aparecer OK unos segundos
                # antes de que el PDF esté en storage.
                # =============================================

                pdf_wait_deadline = (
                    time.monotonic()
                    + 120
                )

                pdf_attempt = 0
                pdf_result = None

                while (
                    time.monotonic()
                    < pdf_wait_deadline
                ):

                    pdf_attempt += 1

                    try:
                        pdf_result = (
                            self.download_acta(
                                row_id
                            )
                        )

                        break

                    except RuntimeError as exc:

                        error_text = str(
                            exc
                        )

                        if (
                            "PROVIDER15_DOWNLOAD_NOT_READY"
                            not in error_text
                        ):
                            raise

                        print(
                            "PROVIDER15_CHAIN_PDF_WAITING =",
                            {
                                "cadena": cadena,
                                "provider_request_id": (
                                    row_id
                                ),
                                "attempt": (
                                    pdf_attempt
                                ),
                            },
                            flush=True,
                        )

                        time.sleep(
                            2.0
                        )

                if pdf_result is None:

                    print(
                        "PROVIDER15_CHAIN_PDF_TIMEOUT =",
                        {
                            "cadena": cadena,
                            "provider_request_id": (
                                row_id
                            ),
                        },
                        flush=True,
                    )

                    return {
                        "ok": False,
                        "status": (
                            "download_timeout"
                        ),
                        "search": "CADENA",
                        "cadena": cadena,
                        "provider_uuid": (
                            provider_uuid
                            or None
                        ),
                        "provider_request_id": (
                            row_id
                        ),
                        "type": row.get(
                            "type"
                        ),
                        "message": (
                            "El proveedor marcó "
                            "la solicitud como OK, "
                            "pero el PDF no estuvo "
                            "disponible a tiempo."
                        ),
                        "raw_row": row,
                    }

                pdf_bytes = (
                    pdf_result.get(
                        "pdf_bytes"
                    )
                )

                if not isinstance(
                    pdf_bytes,
                    bytes,
                ):
                    raise RuntimeError(
                        "PROVIDER15_CHAIN_PDF_BYTES_MISSING"
                    )

                if not pdf_bytes.startswith(
                    b"%PDF"
                ):
                    raise RuntimeError(
                        "PROVIDER15_CHAIN_INVALID_PDF"
                    )

                print(
                    "PROVIDER15_CHAIN_SUCCESS =",
                    {
                        "cadena": cadena,
                        "provider_request_id": (
                            row_id
                        ),
                        "provider_uuid": (
                            provider_uuid
                            or None
                        ),
                        "type": row.get(
                            "type"
                        ),
                        "pdf_size": len(
                            pdf_bytes
                        ),
                    },
                    flush=True,
                )

                return {
                    "ok": True,
                    "status": "ok",

                    "search": "CADENA",
                    "cadena": cadena,

                    "provider_uuid": (
                        provider_uuid
                        or None
                    ),

                    "provider_request_id": (
                        row_id
                    ),

                    # El tipo lo determina el proveedor
                    # a partir de la cadena.
                    "certificate_type": (
                        row.get("type")
                    ),

                    "type": row.get(
                        "type"
                    ),

                    "curp": row.get(
                        "curp"
                    ),

                    "nombre": row.get(
                        "nombre"
                    ),

                    "apellidos": row.get(
                        "apellidos"
                    ),

                    "fechanac": row.get(
                        "fechanac"
                    ),

                    "price": row.get(
                        "price"
                    ),

                    "systemsource": row.get(
                        "systemsource"
                    ),

                    "created_at": row.get(
                        "createdAt"
                    ),

                    "pdf_bytes": (
                        pdf_bytes
                    ),

                    "pdf_size": len(
                        pdf_bytes
                    ),

                    "raw_row": row,
                }

            # =================================================
            # NO ENCONTRADO
            # =================================================

            status_lower = (
                status.lower()
            )

            if (
                "no encontrado"
                in status_lower
            ):

                print(
                    "PROVIDER15_CHAIN_NOT_FOUND =",
                    {
                        "cadena": cadena,
                        "provider_request_id": (
                            row_id
                        ),
                        "message": status,
                    },
                    flush=True,
                )

                return {
                    "ok": False,
                    "status": "not_found",

                    "search": "CADENA",
                    "cadena": cadena,

                    "provider_uuid": (
                        provider_uuid
                        or None
                    ),

                    "provider_request_id": (
                        row_id
                        or None
                    ),

                    "certificate_type": (
                        row.get("type")
                    ),

                    "message": status,

                    "raw_row": row,
                }

            # =================================================
            # OTRO ESTADO
            # =================================================

            print(
                "PROVIDER15_CHAIN_PENDING_STATUS =",
                {
                    "cadena": cadena,
                    "provider_request_id": (
                        row_id
                    ),
                    "status": status,
                    "poll": poll_number,
                },
                flush=True,
            )

        # =====================================================
        # TIMEOUT
        # =====================================================

        print(
            "PROVIDER15_CHAIN_TIMEOUT =",
            {
                "cadena": cadena,
                "provider_uuid": (
                    provider_uuid
                    or None
                ),
                "last_row": (
                    last_matching_row
                ),
            },
            flush=True,
        )

        return {
            "ok": False,
            "status": "timeout",

            "search": "CADENA",
            "cadena": cadena,

            "provider_uuid": (
                provider_uuid
                or None
            ),

            "message": (
                "Provider 15 no terminó "
                "la solicitud por CADENA "
                "dentro del tiempo configurado."
            ),

            "raw_row": (
                last_matching_row
            ),
        }

    def process_birth_certificate(
        self,
        curp: str,
        *,
        timeout_seconds: int = 120,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:

        return self.process_certificate(
            curp,
            "NACIMIENTO",
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )

    def request_birth_certificate(
        self,
        curp: str,
    ) -> dict[str, Any]:

        return self.request_certificate(
            curp,
            "NACIMIENTO",
        )

    def get_balance(self) -> dict[str, Any]:
        self.ensure_session()
        self._sleep_jitter()

        response = self._request(
            "GET",
            self.balance_url,
            headers=self._auth_headers("/app/home/dashboard"),
            timeout=self.timeout_query,
        )

        print("PROVIDER15_BALANCE_STATUS =", response.status_code, flush=True)

        if response.status_code in (307, 401, 403):
            print(
                "PROVIDER15_SESSION_EXPIRED_RECONNECTING =",
                response.status_code,
                flush=True,
            )

            self._session_ready = False
            self._socket_sid = ""
            self.token = ""
            self.user_info = {}

            self.login()
            self.initialize_session()

            response = self._request(
                "GET",
                self.balance_url,
                headers=self._auth_headers("/app/home/dashboard"),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError("PROVIDER15_BALANCE_RATE_LIMIT")
        if response.status_code != 200:
            raise RuntimeError(
                f"PROVIDER15_BALANCE_HTTP_{response.status_code}: "
                f"{(response.text or '')[:500]}"
            )

        try:
            body = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"PROVIDER15_BALANCE_NO_JSON: {(response.text or '')[:500]}"
            ) from exc

        # La API puede responder cifrada o en JSON normal.
        if isinstance(body, dict):
            try:
                decoded = self.decrypt_object(body)
                return decoded
            except Exception:
                return body

        return {"data": body}


if __name__ == "__main__":
    import os

    username = os.getenv(
        "PROVIDER15_USERNAME",
        "",
    ).strip()

    password = os.getenv(
        "PROVIDER15_PASSWORD",
        "",
    )

    client = ProviderSidEmilianoClient(
        username=username,
        password=password,
    )

    login_result = client.login()

    print(
        "LOGIN_OK =",
        login_result.get("ok"),
    )

    balance_result = (
        client.get_balance()
    )

    print(
        "BALANCE_RESULT =",
        balance_result,
    )
