import base64
import requests

from app.config import settings


class Provider3Client:
    def __init__(self, phpsessid: str | None = None) -> None:
        self.base_url = settings.PROVIDER3_BASE_URL.rstrip("/")
        self.login_url = f"{self.base_url}/login_proxy.php"
        self.acta_curp_url = f"{self.base_url}/service_proxy.php?type=acta-curp"
        self.acta_cadena_url = f"{self.base_url}/service_proxy.php?type=acta-cadena"

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/auth.php",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        })

        cookie = (phpsessid or settings.PROVIDER3_PHPSESSID or "").strip()
        if cookie:
            domain = (
                self.base_url
                .replace("https://", "")
                .replace("http://", "")
                .strip("/")
            )
            self.session.cookies.set(
                "PHPSESSID",
                cookie,
                domain=domain,
                path="/",
            )

    def keepalive(self) -> dict:
        url = f"{self.base_url}/user_proxy.php"
    
        resp = self.session.get(
            url,
            timeout=settings.PROVIDER3_TIMEOUT_LOGIN
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
                "status_code": resp.status_code
            }

    def login(self, captcha: str) -> dict:
        payload = {
            "email": settings.PROVIDER3_EMAIL,
            "password": settings.PROVIDER3_PASSWORD,
            "captcha": (captcha or "").strip(),
        }

        resp = self.session.post(
            self.login_url,
            json=payload,
            timeout=settings.PROVIDER3_TIMEOUT_LOGIN
        )
        resp.raise_for_status()
        return resp.json()

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
    
        resp = self.session.post(
            self.acta_curp_url,
            json=payload,
            timeout=settings.PROVIDER3_TIMEOUT_GENERATE
        )
    
        if resp.status_code == 401:
            raise RuntimeError(f"PROVIDER3_SESSION_INVALID_OR_EXPIRED: {resp.text[:500]}")
    
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                f"PROVIDER3_RATE_LIMIT"
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
    
        try:
            return resp.json()
        except Exception as exc:
            raise RuntimeError(f"CURP_NO_JSON: {resp.text[:500]}") from exc

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
    
        resp = self.session.post(
            self.acta_cadena_url,
            json=payload,
            timeout=settings.PROVIDER3_TIMEOUT_GENERATE
        )
    
        if resp.status_code == 401:
            raise RuntimeError(f"PROVIDER3_SESSION_INVALID_OR_EXPIRED: {resp.text[:500]}")
    
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "").strip()
            detail = resp.text[:500]
            raise RuntimeError(
                f"PROVIDER3_RATE_LIMIT"
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
    
        try:
            return resp.json()
        except Exception as exc:
            raise RuntimeError(f"CADENA_NO_JSON: {resp.text[:500]}") from exc


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
