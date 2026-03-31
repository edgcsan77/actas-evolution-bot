import re
from urllib.parse import urljoin

import requests


class Provider4Client:
    BASE_URL = "https://www.tramitanet.org"
    MANUAL_PAGE_URL = "https://www.tramitanet.org/servicio/manual.php?HID=Isa4cLz"
    MANUAL_ENDPOINT = "https://www.tramitanet.org/servicio/backend-manualCVL.php"
    HISTORY_URL = "https://www.tramitanet.org/servicio/vHistory.php?HID=Isa4cLz"
    HID = "Isa4cLz"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.MANUAL_PAGE_URL,
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Mobile Safari/537.36"
            ),
        })

    def consultar(
        self,
        curp: str = "",
        tipoa: str = "nacimiento",
        inc_folio: bool = False,
        cadena: str = "",
        trami_ine: bool = True,
    ) -> str:
        params = {
            "curp": curp,
            "tipoa": tipoa,
            "hidU": self.HID,
            "incFolio": "true" if inc_folio else "false",
            "tramiINE": "true" if trami_ine else "false",
            "cadenaA": cadena,
        }

        resp = self.session.get(self.MANUAL_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        return resp.text

    def consultar_por_curp(
        self,
        curp: str,
        tipoa: str,
        inc_folio: bool = False,
    ) -> str:
        return self.consultar(
            curp=curp,
            tipoa=tipoa,
            inc_folio=inc_folio,
            cadena="",
        )

    def consultar_por_cadena(
        self,
        cadena: str,
        tipoa: str,
        inc_folio: bool = False,
    ) -> str:
        return self.consultar(
            curp="",
            tipoa=tipoa,
            inc_folio=inc_folio,
            cadena=cadena,
        )

    def get_history_html(self) -> str:
        resp = self.session.get(self.HISTORY_URL, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _extract_pdf_link(self, history_html: str, term: str) -> str | None:
        """
        Busca el enlace de descarga normal tipo ./d.php?f=CURP_NAC
        """
        term = (term or "").strip().upper()

        pattern = rf'href="(\./d\.php\?f={re.escape(term)}[^"]*)"'
        m = re.search(pattern, history_html, flags=re.IGNORECASE)
        if m:
            return urljoin("https://www.tramitanet.org/servicio/", m.group(1))

        # fallback: primer d.php relacionado con el término
        pattern_fallback = rf'href="(\./d\.php\?f=[^"]*{re.escape(term)}[^"]*)"'
        m = re.search(pattern_fallback, history_html, flags=re.IGNORECASE)
        if m:
            return urljoin("https://www.tramitanet.org/servicio/", m.group(1))

        return None

    def _extract_folio_link(self, history_html: str, term: str) -> str | None:
        """
        Busca el enlace de foliado del registro reciente del término.
        """
        term = (term or "").strip().upper()

        # Tomar el bloque de fila donde aparece el término
        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        row_match = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)
        if not row_match:
            return None

        row_html = row_match.group(0)

        folio_match = re.search(
            r'href="(\./ActasN/addFol\.php\?[^"]+)"',
            row_html,
            flags=re.IGNORECASE
        )
        if folio_match:
            return urljoin("https://www.tramitanet.org/servicio/", folio_match.group(1))

        return None

    def download_pdf_bytes(self, term: str, inc_folio: bool = False) -> bytes:
        history_html = self.get_history_html()

        if inc_folio:
            link = self._extract_folio_link(history_html, term)
            if not link:
                raise RuntimeError(f"PROVIDER4_NO_FOLIO_LINK_FOR:{term}")
        else:
            link = self._extract_pdf_link(history_html, term)
            if not link:
                raise RuntimeError(f"PROVIDER4_NO_PDF_LINK_FOR:{term}")

        resp = self.session.get(link, timeout=60)
        resp.raise_for_status()

        content_type = (resp.headers.get("content-type") or "").lower()
        if "pdf" not in content_type and not resp.content.startswith(b"%PDF"):
            raise RuntimeError(f"PROVIDER4_INVALID_PDF_RESPONSE:{content_type}")

        return resp.content
