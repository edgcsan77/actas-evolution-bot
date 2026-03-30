import base64
import random
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.config import settings


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


class Provider3Client:
    def __init__(self, phpsessid: str | None = None) -> None:
        self.base_url = settings.PROVIDER3_BASE_URL.rstrip("/")
        self.login_url = f"{self.base_url}/login_proxy.php"
        self.user_url = f"{self.base_url}/user_proxy.php"
        self.auth_page_url = f"{self.base_url}/auth.php"
        self.home_url = f"{self.base_url}/"
        self.acta_curp_url = f"{self.base_url}/service_proxy.php?type=acta-curp"
        self.acta_cadena_url = f"{self.base_url}/service_proxy.php?type=acta-cadena"

        self.session = requests.Session()
        self._configure_session()

        cookie = (phpsessid or settings.PROVIDER3_PHPSESSID or "").strip()
        if cookie:
            self._set_phpsessid(cookie)

    # =========================================================
    # CONFIG
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

        self.session.headers.update(self._default_headers_json())

    def _default_headers_json(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": self.base_url,
            "Referer": self.auth_page_url,
            "User-Agent": DEFAULT_UA,
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }

    def _default_headers_html(self) -> dict[str, str]:
        return {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "User-Agent": DEFAULT_UA,
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "Referer": self.auth_page_url,
        }

    def _set_phpsessid(self, cookie_value: str) -> None:
        domain = (
            self.base_url
            .replace("https://", "")
            .replace("http://", "")
            .strip("/")
        )
        self.session.cookies.set(
            "PHPSESSID",
            cookie_value.strip(),
            domain=domain,
            path="/",
        )

    # =========================================================
    # HELPERS
    # =========================================================
    def _sleep_jitter(self, min_seconds: float = 0.15, max_seconds: float = 0.9) -> None:
        if max_seconds <= 0:
            return
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | float = 20,
        allow_retry_429: bool = True,
        retry_429_max: int = 1,
        jitter_before: tuple[float, float] | None = None,
    ) -> requests.Response:
        if jitter_before:
            self._sleep_jitter(*jitter_before)

        merged_headers = dict(self.session.headers)
        if headers:
            merged_headers.update(headers)

        attempt = 0
        while True:
            resp = self.session.request(
                method=method.upper(),
                url=url,
                json=json_body,
                headers=merged_headers,
                timeout=timeout,
            )

            if resp.status_code != 429 or not allow_retry_429 or attempt >= retry_429_max:
                return resp

            retry_after = resp.headers.get("Retry-After", "").strip()
            wait_seconds = 1.5

            try:
                if retry_after:
                    wait_seconds = max(1.0, float(retry_after))
            except Exception:
                wait_seconds = 1.5

            wait_seconds += random.uniform(0.4, 1.2)
            time.sleep(wait_seconds)
            attempt += 1

    def _parse_json_or_raise(self, resp: requests.Response, error_prefix: str) -> dict[str, Any]:
        try:
            return resp.json()
        except Exception as exc:
            raise RuntimeError(f"{error_prefix}: {resp.text[:500]}") from exc

    # =========================================================
    # NAVEGACIÓN MÍNIMA REAL
    # =========================================================
    def warm_session(self, with_user_check: bool = True) -> dict[str, Any]:
        """
        Navegación mínima y estable:
        1) home
        2) auth page
        3) user_proxy opcional

        No intenta simular humano. Solo calienta la sesión de forma consistente.
        """
        out: dict[str, Any] = {"ok": True, "steps": []}

        try:
            r1 = self._request(
                "GET",
                self.home_url,
                headers=self._default_headers_html(),
                timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
                allow_retry_429=True,
                retry_429_max=1,
                jitter_before=(0.2, 0.7),
            )
            out["steps"].append({"url": self.home_url, "status": r1.status_code})

            self._sleep_jitter(0.15, 0.6)

            r2 = self._request(
                "GET",
                self.auth_page_url,
                headers=self._default_headers_html(),
                timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
                allow_retry_429=True,
                retry_429_max=1,
                jitter_before=(0.2, 0.8),
            )
            out["steps"].append({"url": self.auth_page_url, "status": r2.status_code})

            if with_user_check:
                self._sleep_jitter(0.2, 0.8)
                r3 = self._request(
                    "GET",
                    self.user_url,
                    headers=self._default_headers_json(),
                    timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
                    allow_retry_429=True,
                    retry_429_max=1,
                    jitter_before=None,
                )
                out["steps"].append({"url": self.user_url, "status": r3.status_code})

                if r3.status_code == 401:
                    raise RuntimeError("PROVIDER3_WARM_SESSION_INVALID")
                if r3.status_code == 429:
                    raise RuntimeError("PROVIDER3_WARM_SESSION_RATE_LIMIT")

        except Exception as exc:
            out["ok"] = False
            out["error"] = str(exc)

        return out

    # =========================================================
    # ESTADO DE SESIÓN
    # =========================================================
    def keepalive(self, jitter_seconds: tuple[float, float] | None = (0.2, 1.0)) -> dict:
        resp = self._request(
            "GET",
            self.user_url,
            headers=self._default_headers_json(),
            timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
            allow_retry_429=True,
            retry_429_max=1,
            jitter_before=jitter_seconds,
        )

        if resp.status_code == 401:
            raise RuntimeError("PROVIDER3_KEEPALIVE_SESSION_INVALID")

        if resp.status_code == 429:
            raise RuntimeError("PROVIDER3_KEEPALIVE_RATE_LIMIT")

        resp.raise_for_status()

        try:
            return resp.json()
        except Exception:
            return {
                "ok": True,
                "status_code": resp.status_code,
            }

    def get_licenses(self) -> dict:
        resp = self._request(
            "GET",
            self.user_url,
            headers=self._default_headers_json(),
            timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
            allow_retry_429=True,
            retry_429_max=1,
            jitter_before=(0.15, 0.7),
        )

        if resp.status_code == 401:
            raise RuntimeError("PROVIDER3_LICENSES_SESSION_INVALID")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                "PROVIDER3_LICENSES_RATE_LIMIT"
                + (f": retry_after={retry_after}" if retry_after else "")
                + (f" | {detail}" if detail else "")
            )

        resp.raise_for_status()
        data = self._parse_json_or_raise(resp, "PROVIDER3_LICENSES_NO_JSON")
        licenses = data.get("licenses") or {}

        return {
            "acta_curp": licenses.get("acta_curp"),
            "acta_cadena": licenses.get("acta_cadena"),
            "email": data.get("email"),
            "username": data.get("username"),
            "raw": data,
        }

    # =========================================================
    # LOGIN
    # =========================================================
    def login(self, captcha: str) -> dict:
        payload = {
            "email": settings.PROVIDER3_EMAIL,
            "password": settings.PROVIDER3_PASSWORD,
            "captcha": (captcha or "").strip(),
        }

        resp = self._request(
            "POST",
            self.login_url,
            json_body=payload,
            headers=self._default_headers_json(),
            timeout=settings.PROVIDER3_TIMEOUT_LOGIN,
            allow_retry_429=False,
            jitter_before=(0.25, 0.9),
        )

        if resp.status_code == 401:
            raise RuntimeError(f"PROVIDER3_LOGIN_INVALID: {resp.text[:500]}")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                "PROVIDER3_LOGIN_RATE_LIMIT"
                + (f": retry_after={retry_after}" if retry_after else "")
                + (f" | {detail}" if detail else "")
            )

        resp.raise_for_status()
        return self._parse_json_or_raise(resp, "PROVIDER3_LOGIN_NO_JSON")

    # =========================================================
    # GENERACIÓN
    # =========================================================
    def generar_por_curp(
        self,
        curp: str,
        tipo_acta: str = "nacimiento",
        folio1: bool = False,
        folio2: bool = False,
        reverso: bool = False,
        margen: bool = False,
    ) -> dict:
        payload = {
            "curp": curp.strip().upper(),
            "tipo_acta": tipo_acta,
            "folio1": folio1,
            "folio2": folio2,
            "reverso": reverso,
            "margen": margen,
        }

        resp = self._request(
            "POST",
            self.acta_curp_url,
            json_body=payload,
            headers=self._default_headers_json(),
            timeout=settings.PROVIDER3_TIMEOUT_GENERATE,
            allow_retry_429=True,
            retry_429_max=1,
            jitter_before=(0.3, 1.1),
        )

        if resp.status_code == 401:
            raise RuntimeError(f"PROVIDER3_SESSION_INVALID_OR_EXPIRED: {resp.text[:500]}")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                "PROVIDER3_RATE_LIMIT"
                + (f": retry_after={retry_after}" if retry_after else "")
                + (f" | {detail}" if detail else "")
            )

        if resp.status_code == 400:
            body_text = (resp.text or "").strip()
            try:
                data = resp.json()
            except Exception:
                data = {}

            message = (
                data.get("message")
                or data.get("error")
                or body_text
                or "ACTA_NO_LOCALIZADA"
            )
            raise RuntimeError(f"PROVIDER3_NO_RECORD: {message[:500]}")

        resp.raise_for_status()
        return self._parse_json_or_raise(resp, "CURP_NO_JSON")

    def generar_por_cadena(
        self,
        cadena: str,
        folio1: bool = False,
        folio2: bool = False,
        reverso: bool = False,
        margen: bool = False,
    ) -> dict:
        payload = {
            "cadena": cadena.strip(),
            "folio1": folio1,
            "folio2": folio2,
            "reverso": reverso,
            "margen": margen,
        }

        resp = self._request(
            "POST",
            self.acta_cadena_url,
            json_body=payload,
            headers=self._default_headers_json(),
            timeout=settings.PROVIDER3_TIMEOUT_GENERATE,
            allow_retry_429=True,
            retry_429_max=1,
            jitter_before=(0.3, 1.1),
        )

        if resp.status_code == 401:
            raise RuntimeError(f"PROVIDER3_SESSION_INVALID_OR_EXPIRED: {resp.text[:500]}")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                "PROVIDER3_RATE_LIMIT"
                + (f": retry_after={retry_after}" if retry_after else "")
                + (f" | {detail}" if detail else "")
            )

        if resp.status_code == 400:
            body_text = (resp.text or "").strip()
            try:
                data = resp.json()
            except Exception:
                data = {}

            message = (
                data.get("message")
                or data.get("error")
                or body_text
                or "ACTA_NO_LOCALIZADA"
            )
            raise RuntimeError(f"PROVIDER3_NO_RECORD: {message[:500]}")

        resp.raise_for_status()
        return self._parse_json_or_raise(resp, "CADENA_NO_JSON")


def decode_pdf_base64(pdf_b64: str) -> bytes:
    raw = (pdf_b64 or "").strip()

    if raw.startswith("data:"):
        raw = raw.split(",", 1)[1]

    raw = raw.replace("\n", "").replace("\r", "").strip()

    missing_padding = len(raw) % 4
    if missing_padding:
        raw += "=" * (4 - missing_padding)

    pdf_bytes = base64.b64decode(raw, validate=False)

    if not pdf_bytes.startswith(b"%PDF"):
        raise ValueError("La respuesta no contiene un PDF válido.")

    return pdf_bytes
