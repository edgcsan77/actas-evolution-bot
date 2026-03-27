import base64
from pathlib import Path
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

        cookie = phpsessid or settings.PROVIDER3_PHPSESSID
        if cookie:
            self.session.cookies.set(
                "PHPSESSID",
                cookie,
                domain=self.base_url.replace("https://", "").replace("http://", ""),
                path="/",
            )

    def login(self, captcha: str) -> dict:
        payload = {
            "email": settings.PROVIDER3_EMAIL,
            "password": settings.PROVIDER3_PASSWORD,
            "captcha": captcha,
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
        resp.raise_for_status()
        return resp.json()

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
        resp.raise_for_status()
        return resp.json()


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
