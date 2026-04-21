import random
import time
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) "
    "Gecko/20100101 Firefox/149.0"
)


class SidOaxacaClient:
    def __init__(
        self,
        *,
        access_token: str,
        jsessionid: str,
        oficialia: int | str,
        rfc_usuario: str,
        base_url: str = "https://sid.oaxaca.gob.mx",
        timeout_login: int = 20,
        timeout_query: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = (access_token or "").strip()
        self.jsessionid = (jsessionid or "").strip()
        self.oficialia = str(oficialia).strip()
        self.rfc_usuario = (rfc_usuario or "").strip().upper()

        self.timeout_login = timeout_login
        self.timeout_query = timeout_query

        self.home_url = f"{self.base_url}/sirabi/"
        self.certificacion_url = f"{self.base_url}/sirabi/acto/certificacion.html?v=18082023"

        self.captcha_url = (
            f"{self.base_url}/sirabi-admin/usuario/captcha/{self.oficialia}"
            f"?access_token={self.access_token}"
        )
        self.param_local_url = (
            f"{self.base_url}/sirabi-consultas/consulta/parametrolocal/BOND"
            f"?access_token={self.access_token}"
        )

        self.session = requests.Session()
        self._configure_session()

        if self.jsessionid:
            self._set_jsessionid(self.jsessionid)

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
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json; charset=utf-8",
            "Origin": self.base_url,
            "Referer": self.certificacion_url,
            "User-Agent": DEFAULT_UA,
            "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "X-Requested-With": "XMLHttpRequest",
        }

    def _default_headers_html(self) -> dict[str, str]:
        return {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "User-Agent": DEFAULT_UA,
            "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "Referer": self.home_url,
        }

    def _set_jsessionid(self, cookie_value: str) -> None:
        self.session.cookies.set(
            "JSESSIONID",
            cookie_value.strip(),
            domain="sid.oaxaca.gob.mx",
            path="/",
        )

    # =========================================================
    # HELPERS
    # =========================================================
    def _sleep_jitter(self, min_seconds: float = 0.6, max_seconds: float = 2.2) -> None:
        if max_seconds <= 0:
            return
        time.sleep(random.uniform(min_seconds, max_seconds))

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
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
                data=data,
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
            raise RuntimeError(f"{error_prefix}: {resp.text[:800]}") from exc

    def _raise_common_errors(self, resp: requests.Response, prefix: str) -> None:
        if resp.status_code == 401:
            raise RuntimeError(f"{prefix}_SESSION_INVALID: {resp.text[:500]}")
        if resp.status_code == 403:
            raise RuntimeError(f"{prefix}_FORBIDDEN: {resp.text[:500]}")
        if resp.status_code == 404:
            raise RuntimeError(f"{prefix}_NOT_FOUND: {resp.text[:500]}")
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            raise RuntimeError(
                f"{prefix}_RATE_LIMIT"
                + (f": retry_after={retry_after}" if retry_after else "")
                + (f" | {resp.text[:500]}" if resp.text else "")
            )
        if resp.status_code >= 400:
            raise RuntimeError(f"{prefix}_HTTP_{resp.status_code}: {resp.text[:800]}")

    # =========================================================
    # WARM SESSION
    # =========================================================
    def warm_session(self) -> dict[str, Any]:
        out: dict[str, Any] = {"ok": True, "steps": []}

        try:
            r1 = self._request(
                "GET",
                self.home_url,
                headers=self._default_headers_html(),
                timeout=self.timeout_login,
                jitter_before=(0.2, 0.7),
            )
            out["steps"].append({"url": self.home_url, "status": r1.status_code})

            self._sleep_jitter(0.15, 0.5)

            r2 = self._request(
                "GET",
                self.certificacion_url,
                headers=self._default_headers_html(),
                timeout=self.timeout_login,
                jitter_before=(0.2, 0.7),
            )
            out["steps"].append({"url": self.certificacion_url, "status": r2.status_code})

            self._sleep_jitter(0.15, 0.5)

            r3 = self._request(
                "GET",
                self.param_local_url,
                headers=self._default_headers_json(),
                timeout=self.timeout_login,
            )
            out["steps"].append({"url": self.param_local_url, "status": r3.status_code})

            self._raise_common_errors(r3, "SID_WARM_PARAMLOCAL")

        except Exception as exc:
            out["ok"] = False
            out["error"] = str(exc)

        return out

    # =========================================================
    # CONTEXTO / VALIDACIÓN
    # =========================================================
    def get_param_local(self) -> dict[str, Any]:
        resp = self._request(
            "GET",
            self.param_local_url,
            headers=self._default_headers_json(),
            timeout=self.timeout_login,
            jitter_before=(0.1, 0.4),
        )
        self._raise_common_errors(resp, "SID_PARAMLOCAL")
        return self._parse_json_or_raise(resp, "SID_PARAMLOCAL_NO_JSON")

    def post_captcha_value(self, captcha_value: str) -> dict[str, Any]:
        payload = {"captcha": (captcha_value or "").strip()}

        resp = self._request(
            "POST",
            self.captcha_url,
            json_body=payload,
            headers=self._default_headers_json(),
            timeout=self.timeout_login,
            allow_retry_429=False,
            jitter_before=(0.2, 0.6),
        )
        self._raise_common_errors(resp, "SID_CAPTCHA")

        # A veces puede responder "true", "false", 1 byte, etc.
        try:
            return {
                "ok": True,
                "status_code": resp.status_code,
                "json": resp.json(),
                "text": resp.text,
            }
        except Exception:
            return {
                "ok": True,
                "status_code": resp.status_code,
                "text": (resp.text or "").strip(),
            }

    # =========================================================
    # CONSULTAS
    # =========================================================
    def consultar_nacimiento_por_curp(self, curp: str) -> dict[str, Any]:
        curp = (curp or "").strip().upper()

        url = (
            f"{self.base_url}/sirabi-consultas/nacimiento/curp/"
            f"{curp}/{self.access_token}/{self.oficialia}/{self.rfc_usuario}"
            f"?access_token={self.access_token}"
        )

        resp = self._request(
            "GET",
            url,
            headers=self._default_headers_json(),
            timeout=self.timeout_query,
            jitter_before=(0.2, 0.8),
        )

        self._raise_common_errors(resp, "SID_NACIMIENTO_CURP")
        return self._parse_json_or_raise(resp, "SID_NACIMIENTO_CURP_NO_JSON")

    def consultar_por_curp(self, curp: str, acto: str = "nacimiento") -> dict[str, Any]:
        """
        acto esperado: nacimiento, matrimonio, defuncion, divorcio
        """
        acto = (acto or "").strip().lower()
        curp = (curp or "").strip().upper()

        if acto not in {"nacimiento", "matrimonio", "defuncion", "divorcio"}:
            raise ValueError("acto inválido")

        url = (
            f"{self.base_url}/sirabi-consultas/{acto}/curp/"
            f"{curp}/{self.access_token}/{self.oficialia}/{self.rfc_usuario}"
            f"?access_token={self.access_token}"
        )

        resp = self._request(
            "GET",
            url,
            headers=self._default_headers_json(),
            timeout=self.timeout_query,
            jitter_before=(0.2, 0.8),
        )

        self._raise_common_errors(resp, f"SID_{acto.upper()}_CURP")
        return self._parse_json_or_raise(resp, f"SID_{acto.upper()}_CURP_NO_JSON")

    def consultar_por_cadena(self, cadena: str, acto: str = "nacimiento") -> dict[str, Any]:
        """
        OJO: esta ruta es tentativa.
        Necesita confirmarse en Network del flujo 'Por CADENA'.
        """
        acto = (acto or "").strip().lower()
        cadena = (cadena or "").strip()

        if acto not in {"nacimiento", "matrimonio", "defuncion", "divorcio"}:
            raise ValueError("acto inválido")

        url = (
            f"{self.base_url}/sirabi-consultas/{acto}/cadena/"
            f"{cadena}/{self.access_token}/{self.oficialia}/{self.rfc_usuario}"
            f"?access_token={self.access_token}"
        )

        resp = self._request(
            "GET",
            url,
            headers=self._default_headers_json(),
            timeout=self.timeout_query,
            jitter_before=(0.2, 0.8),
        )

        self._raise_common_errors(resp, f"SID_{acto.upper()}_CADENA")
        return self._parse_json_or_raise(resp, f"SID_{acto.upper()}_CADENA_NO_JSON")

    # =========================================================
    # CONSULTAS
    # =========================================================
    def get_folios_impresion(self) -> dict[str, Any]:
        url = (
            f"{self.base_url}/sirabi-admin/parametro/FOLIOS_IMPRESION"
            f"?access_token={self.access_token}"
        )

        resp = self._request(
            "GET",
            url,
            headers=self._default_headers_json(),
            timeout=self.timeout_login,
            jitter_before=(0.1, 0.4),
        )

        self._raise_common_errors(resp, "SID_FOLIOS_IMPRESION")
        return self._parse_json_or_raise(resp, "SID_FOLIOS_IMPRESION_NO_JSON")

    def descargar_pdf_acta(
        self,
        *,
        folio_impresion: str,
        referencia: str,
        formato: int | str = 1,
        sexo: str = "F",
    ) -> bytes:
        folio_impresion = str(folio_impresion).strip()
        referencia = str(referencia).strip()
        formato = str(formato).strip()
        sexo = str(sexo).strip().upper()

        url = (
            f"{self.base_url}/sirabi-consultas/acta/folio/"
            f"{folio_impresion}/{referencia}/{formato}/{self.oficialia}/{sexo}/"
            f"{self.access_token}/{self.oficialia}/{self.rfc_usuario}"
            f"?access_token={self.access_token}"
        )

        headers = self._default_headers_json()
        headers["Accept"] = "*/*"

        resp = self._request(
            "GET",
            url,
            headers=headers,
            timeout=self.timeout_query,
            jitter_before=(0.2, 0.8),
        )

        self._raise_common_errors(resp, "SID_DESCARGA_PDF")

        content_type = (resp.headers.get("Content-Type") or "").lower()
        raw = resp.content or b""

        if "application/pdf" in content_type:
            if not raw.startswith(b"%PDF"):
                # por si alguna vez viene base64 en vez de binario
                try:
                    import base64
                    decoded = base64.b64decode(raw, validate=False)
                    if decoded.startswith(b"%PDF"):
                        return decoded
                except Exception:
                    pass
                raise RuntimeError("SID_DESCARGA_PDF_INVALIDO: no inicia con %PDF")
            return raw

        # fallback: a veces puede venir texto/base64
        try:
            import base64
            text = resp.text.strip()
            decoded = base64.b64decode(text, validate=False)
            if decoded.startswith(b"%PDF"):
                return decoded
        except Exception:
            pass

        raise RuntimeError(
            f"SID_DESCARGA_PDF_NO_PDF: content_type={content_type} body={resp.text[:500]}"
        )
