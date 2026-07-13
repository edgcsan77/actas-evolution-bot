from __future__ import annotations

import base64
import json
import random
import time
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
    ) -> None:
        self.username = (username or "").strip()
        self.password = password or ""
        self.timeout_login = int(timeout_login)
        self.timeout_query = int(timeout_query)

        self.login_url = f"{self.API_BASE_URL}/api/users/login"
        self.balance_url = f"{self.API_BASE_URL}/api/users/balance"

        self.token: str = ""
        self.user_info: dict[str, Any] = {}

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
            allowed_methods=frozenset(["GET", "POST"]),
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
            "raw": decoded,
        }

    def ensure_login(self) -> None:
        if not self.token:
            self.login()

    def get_balance(self) -> dict[str, Any]:
        self.ensure_login()
        self._sleep_jitter()

        response = self._request(
            "GET",
            self.balance_url,
            headers=self._auth_headers("/app/home/dashboard"),
            timeout=self.timeout_query,
        )

        print("PROVIDER15_BALANCE_STATUS =", response.status_code, flush=True)

        if response.status_code in (401, 403):
            # Un único relogin automático.
            self.token = ""
            self.login()
            response = self._request(
                "GET",
                self.balance_url,
                headers=self._auth_headers("/app/home/dashboard"),
                timeout=self.timeout_query,
            )

        if response.status_code == 429:
            raise RuntimeError("PROVIDER15_BALANCE_RATE_LIMIT")
        if response.status_code >= 400:
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

    username = os.getenv("PROVIDER15_USERNAME", "").strip()
    password = os.getenv("PROVIDER15_PASSWORD", "")

    client = ProviderSidEmilianoClient(
        username=username,
        password=password,
    )

    login_result = client.login()
    print("LOGIN_RESULT =", login_result)

    balance_result = client.get_balance()
    print("BALANCE_RESULT =", balance_result)
