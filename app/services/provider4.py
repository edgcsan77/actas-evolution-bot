import re
import time
from html import unescape
from urllib.parse import urljoin

import requests


class Provider4Client:
    BASE_URL = "https://www.tramitanet.org"
    HID = "Isa4cLz"

    MANUAL_PAGE_URL = f"{BASE_URL}/servicio/manual.php?HID={HID}"
    MANUAL_ENDPOINT = f"{BASE_URL}/servicio/backend-manualCVL.php"
    VGET_URL = f"{BASE_URL}/servicio/vGetOfi.php"
    HISTORY_URL = f"{BASE_URL}/servicio/vHistory.php?HID={HID}"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Mobile Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "es-ES,es;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.MANUAL_PAGE_URL,
        })

    def warm(self) -> None:
        resp = self.session.get(self.MANUAL_PAGE_URL, timeout=(15, 60))
        resp.raise_for_status()

    def consultar(
        self,
        curp: str = "",
        tipoa: str = "nacimiento",
        inc_folio: bool = False,
        cadena: str = "",
        trami_ine: bool = True,
    ) -> str:
        self.warm()

        params = {
            "curp": curp,
            "tipoa": tipoa,
            "hidU": self.HID,
            "incFolio": "true" if inc_folio else "false",
            "tramiINE": "true" if trami_ine else "false",
            "cadenaA": cadena,
        }

        last_error = None

        for attempt in range(3):
            try:
                print("PROVIDER4_REQUEST_PARAMS =", params, flush=True)
                print(f"PROVIDER4_BACKEND_ATTEMPT_{attempt+1}_START", flush=True)

                resp = self.session.get(
                    self.MANUAL_ENDPOINT,
                    params=params,
                    timeout=(15, 240),
                )
                resp.raise_for_status()

                print(
                    f"PROVIDER4_BACKEND_ATTEMPT_{attempt+1}_STATUS = {resp.status_code}",
                    flush=True,
                )

                return resp.text

            except requests.exceptions.RequestException as e:
                last_error = e
                print(
                    f"PROVIDER4_BACKEND_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
                    flush=True,
                )
                if attempt < 2:
                    time.sleep(5 + attempt * 3)

        raise RuntimeError(f"PROVIDER4_BACKEND_FAILED: {last_error}")

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

    def _parse_hidden_form(self, html: str) -> tuple[str, dict]:
        form_action_match = re.search(
            r'<form[^>]+action="([^"]+)"',
            html,
            flags=re.IGNORECASE,
        )
        if not form_action_match:
            raise RuntimeError("PROVIDER4_NO_FORM_ACTION")

        action = form_action_match.group(1).strip()
        action_url = urljoin(f"{self.BASE_URL}/servicio/", action)

        inputs = {}
        for name, value in re.findall(
            r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"',
            html,
            flags=re.IGNORECASE,
        ):
            inputs[unescape(name)] = unescape(value)

        if not inputs:
            raise RuntimeError("PROVIDER4_NO_FORM_INPUTS")

        print("PROVIDER4_FORM_ACTION =", action_url, flush=True)
        print("PROVIDER4_FORM_INPUT_KEYS =", list(inputs.keys()), flush=True)

        return action_url, inputs

    def submit_vget_form(self, html: str) -> str:
        action_url, form_data = self._parse_hidden_form(html)

        headers = {
            "User-Agent": self.session.headers["User-Agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9",
            "Referer": self.MANUAL_PAGE_URL,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        last_error = None

        for attempt in range(3):
            try:
                print(f"PROVIDER4_VGET_ATTEMPT_{attempt+1}_START", flush=True)

                resp = self.session.post(
                    action_url,
                    data=form_data,
                    headers=headers,
                    timeout=(15, 240),
                )
                resp.raise_for_status()

                print(
                    f"PROVIDER4_VGET_ATTEMPT_{attempt+1}_STATUS = {resp.status_code}",
                    flush=True,
                )

                return resp.text

            except requests.exceptions.RequestException as e:
                last_error = e
                print(
                    f"PROVIDER4_VGET_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
                    flush=True,
                )
                if attempt < 2:
                    time.sleep(5 + attempt * 3)

        raise RuntimeError(f"PROVIDER4_VGET_FAILED: {last_error}")

    def get_history_html(self) -> str:
        last_error = None

        for attempt in range(3):
            try:
                print(f"PROVIDER4_HISTORY_ATTEMPT_{attempt+1}_START", flush=True)

                resp = self.session.get(
                    self.HISTORY_URL,
                    timeout=(15, 120),
                    headers={
                        "User-Agent": self.session.headers["User-Agent"],
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "es-ES,es;q=0.9",
                        "Referer": self.MANUAL_PAGE_URL,
                    },
                )
                resp.raise_for_status()

                print(
                    f"PROVIDER4_HISTORY_ATTEMPT_{attempt+1}_STATUS = {resp.status_code}",
                    flush=True,
                )

                return resp.text

            except requests.exceptions.RequestException as e:
                last_error = e
                print(
                    f"PROVIDER4_HISTORY_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
                    flush=True,
                )
                if attempt < 2:
                    time.sleep(4)

        raise RuntimeError(f"PROVIDER4_HISTORY_FAILED: {last_error}")

    def _folio_pdf_direct_url(self, term: str, tipoa: str) -> str:
        tipo_map = {
            "nacimiento": "NAC",
            "matrimonio": "MAT",
            "defuncion": "DEF",
            "divorcio": "DIV",
        }
        code = tipo_map.get((tipoa or "").strip().lower(), "NAC")
        return f"{self.BASE_URL}/servicio/ActasN/{term}_{code}_FOLIO.pdf"
    
    def _history_row_for_term(self, history_html: str, term: str) -> str | None:
        term = (term or "").strip().upper()
        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        m = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)
        return m.group(0) if m else None
    
    def _detect_no_result(self, history_html: str, term: str) -> bool:
        row_html = self._history_row_for_term(history_html, term)
        if not row_html:
            return False
    
        row_up = row_html.upper()
    
        if "NO_LOCALIZADO" in row_up:
            return True
    
        if "DESCARGAR PDF" not in row_up and "DESCARGAR FOLIADO" not in row_up:
            return True
    
        return False
    
    def _extract_pdf_link(self, history_html: str, term: str) -> str | None:
        term = (term or "").strip().upper()

        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        row_match = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)

        if row_match:
            row_html = row_match.group(0)
            m = re.search(r'href="(\./d\.php\?f=[^"]+)"', row_html, flags=re.IGNORECASE)
            if m:
                return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))

        m = re.search(
            rf'href="(\./d\.php\?f=[^"]*{re.escape(term)}[^"]*)"',
            history_html,
            flags=re.IGNORECASE,
        )
        if m:
            return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))

        return None

    def _extract_folio_link(self, history_html: str, term: str) -> str | None:
        term = (term or "").strip().upper()

        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        row_match = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)

        if row_match:
            row_html = row_match.group(0)
            m = re.search(
                r'href="(\./ActasN/addFol\.php\?[^"]+)"',
                row_html,
                flags=re.IGNORECASE,
            )
            if m:
                return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))

        m = re.search(
            r'href="(\./ActasN/addFol\.php\?[^"]+)"',
            history_html,
            flags=re.IGNORECASE,
        )
        if m:
            return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))

        return None

    def download_pdf_bytes(self, url: str) -> bytes:
        last_error = None

        for attempt in range(3):
            try:
                print(f"PROVIDER4_DOWNLOAD_ATTEMPT_{attempt+1}_URL = {url}", flush=True)

                resp = self.session.get(
                    url,
                    timeout=(15, 240),
                    headers={
                        "User-Agent": self.session.headers["User-Agent"],
                        "Accept": "*/*",
                        "Referer": self.HISTORY_URL,
                    },
                )
                resp.raise_for_status()

                content_type = (resp.headers.get("content-type") or "").lower()
                print("PROVIDER4_DOWNLOAD_CONTENT_TYPE =", content_type, flush=True)

                if "pdf" not in content_type and not resp.content.startswith(b"%PDF"):
                    raise RuntimeError(f"PROVIDER4_INVALID_PDF_RESPONSE:{content_type}")

                return resp.content

            except Exception as e:
                last_error = e
                print(
                    f"PROVIDER4_DOWNLOAD_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
                    flush=True,
                )
                if attempt < 2:
                    time.sleep(4)

        raise RuntimeError(f"PROVIDER4_DOWNLOAD_FAILED: {last_error}")

    def process_and_download(
        self,
        term: str,
        tipoa: str,
        inc_folio: bool = False,
        is_chain: bool = False,
    ) -> bytes:
        if is_chain:
            html = self.consultar_por_cadena(
                cadena=term,
                tipoa=tipoa,
                inc_folio=inc_folio,
            )
        else:
            html = self.consultar_por_curp(
                curp=term,
                tipoa=tipoa,
                inc_folio=inc_folio,
            )

        print("PROVIDER4_BACKEND_HTML_PREVIEW =", html[:1200], flush=True)

        vget_html = self.submit_vget_form(html)
        print("PROVIDER4_VGET_HTML_PREVIEW =", vget_html[:1200], flush=True)

        max_polls = 12
        poll_sleep_seconds = 3

        for poll_attempt in range(max_polls):
            history_html = self.get_history_html()
            print(
                f"PROVIDER4_HISTORY_HTML_PREVIEW_ATTEMPT_{poll_attempt+1} =",
                history_html[:1500],
                flush=True,
            )

            if inc_folio:
                direct_folio_url = self._folio_pdf_direct_url(term, tipoa)
                print("PROVIDER4_DIRECT_FOLIO_URL =", direct_folio_url, flush=True)
            
                try:
                    return self.download_pdf_bytes(direct_folio_url)
                except Exception as direct_exc:
                    print("PROVIDER4_DIRECT_FOLIO_FAILED =", str(direct_exc), flush=True)
            
                link = self._extract_folio_link(history_html, term)
                if link:
                    print("PROVIDER4_FINAL_FOLIO_LINK =", link, flush=True)
                    return self._download_foliated_pdf(link)
            else:
                link = self._extract_pdf_link(history_html, term)
                if link:
                    print("PROVIDER4_FINAL_DOWNLOAD_LINK =", link, flush=True)
                    return self.download_pdf_bytes(link)

            print(
                f"PROVIDER4_HISTORY_LINK_NOT_READY_ATTEMPT_{poll_attempt+1} = {term}",
                flush=True,
            )
            time.sleep(poll_sleep_seconds)

        final_history_html = self.get_history_html()

        if self._detect_no_result(final_history_html, term):
            raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")

        if inc_folio:
            raise RuntimeError(f"PROVIDER4_NO_FOLIO_LINK_FOR:{term}")
        else:
            raise RuntimeError(f"PROVIDER4_NO_PDF_LINK_FOR:{term}")
