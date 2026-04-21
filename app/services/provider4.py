import re
import time
import requests
from datetime import datetime, timedelta
from html import unescape
from urllib.parse import urljoin

from io import BytesIO
from pypdf import PdfReader
from pathlib import Path

from app.services.provider7 import (
    _enmarcar_pdf_frente,
    _unir_pdfs_bytes,
    _resolver_reverso_por_estado,
)


class Provider4Client:
    BASE_URL = "https://www.tramitanet.org"
    HID = "D0cUExpr3ss"

    MANUAL_PAGE_URL = f"{BASE_URL}/servicio/manual.php?HID={HID}"
    MANUAL_ENDPOINT = f"{BASE_URL}/servicio/backend-manualCVL.php"
    VGET_URL = f"{BASE_URL}/servicio/vGetOfi.php"
    HISTORY_URL = f"{BASE_URL}/servicio/vHistory.php?HID={HID}"

    HISTORY_MAX_POLLS = 60
    HISTORY_POLL_SLEEP = 3

    MAPA_ESTADOS_CURP = {
        "AS": "AGUASCALIENTES",
        "BC": "BAJA_CALIFORNIA",
        "BS": "BAJA_CALIFORNIA_SUR",
        "CC": "CAMPECHE",
        "CL": "COAHUILA",
        "CM": "COLIMA",
        "CS": "CHIAPAS",
        "CH": "CHIHUAHUA",
        "DF": "CIUDAD_DE_MEXICO",
        "DG": "DURANGO",
        "GT": "GUANAJUATO",
        "GR": "GUERRERO",
        "HG": "HIDALGO",
        "JC": "JALISCO",
        "MC": "MEXICO",
        "MN": "MICHOACAN",
        "MS": "MORELOS",
        "NT": "NAYARIT",
        "NL": "NUEVO_LEON",
        "OC": "OAXACA",
        "PL": "PUEBLA",
        "QT": "QUERETARO",
        "QR": "QUINTANA_ROO",
        "SP": "SAN_LUIS_POTOSI",
        "SL": "SINALOA",
        "SR": "SONORA",
        "TC": "TABASCO",
        "TS": "TAMAULIPAS",
        "TL": "TLAXCALA",
        "VZ": "VERACRUZ",
        "YN": "YUCATAN",
        "ZS": "ZACATECAS",
        "NE": "NACIDO_EN_EL_EXTRANJERO",
    }

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

    def _pdf_num_pages(self, pdf_bytes: bytes) -> int:
        reader = PdfReader(BytesIO(pdf_bytes))
        return len(reader.pages)

    def _pdf_has_two_pages(self, pdf_bytes: bytes) -> bool:
        try:
            return self._pdf_num_pages(pdf_bytes) >= 2
        except Exception:
            return False

    def _estado_desde_curp(self, curp: str) -> str:
        curp = (curp or "").strip().upper()
        if len(curp) < 13:
            raise RuntimeError("PROVIDER4_CURP_INVALID_FOR_STATE")

        clave = curp[11:13]
        estado = self.MAPA_ESTADOS_CURP.get(clave)

        if not estado:
            raise RuntimeError(f"PROVIDER4_STATE_NOT_FOUND:{clave}")

        return estado

    def _repair_pdf_if_needed(self, pdf_bytes: bytes, term: str, inc_folio: bool) -> bytes:
        if self._pdf_has_two_pages(pdf_bytes):
            print("PROVIDER4_PDF_ALREADY_COMPLETE = TRUE", flush=True)
            return pdf_bytes

        print("PROVIDER4_PDF_INCOMPLETE_REPAIRING = TRUE", flush=True)

        estado = self._estado_desde_curp(term)

        base_dir = Path(__file__).resolve().parent.parent
        estados_dir = base_dir / "assets" / "estados"

        pdf_bytes = _enmarcar_pdf_frente(
            pdf_bytes,
            f"{term}.pdf",
            folio=inc_folio,
        )

        if estado == "NACIDO_EN_EL_EXTRANJERO":
            print("PROVIDER4_NO_REAR_FRAME_FOR_FOREIGN_BIRTH = TRUE", flush=True)
            print(f"PROVIDER4_PDF_PAGE_COUNT = {self._pdf_num_pages(pdf_bytes)}", flush=True)
            return pdf_bytes

        reverso_path = _resolver_reverso_por_estado(estado, estados_dir)
        pdf_bytes = _unir_pdfs_bytes(pdf_bytes, reverso_path)
        
        print(f"PROVIDER4_PDF_PAGE_COUNT = {self._pdf_num_pages(pdf_bytes)}", flush=True)

        if not self._pdf_has_two_pages(pdf_bytes):
            raise RuntimeError("PROVIDER4_REPAIRED_PDF_STILL_INCOMPLETE")

        return pdf_bytes

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

    def get_history_html_for_date(self, fecha: str) -> str:
        """
        fecha debe venir como: 31/Mar/2026
        """
        last_error = None
        url = f"{self.HISTORY_URL}&fecha={fecha}"
    
        for attempt in range(3):
            try:
                print(f"PROVIDER4_HISTORY_DATE_ATTEMPT_{attempt+1}_URL = {url}", flush=True)
    
                resp = self.session.get(
                    url,
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
                    f"PROVIDER4_HISTORY_DATE_ATTEMPT_{attempt+1}_STATUS = {resp.status_code}",
                    flush=True,
                )
    
                return resp.text
    
            except requests.exceptions.RequestException as e:
                last_error = e
                print(
                    f"PROVIDER4_HISTORY_DATE_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
                    flush=True,
                )
                if attempt < 2:
                    time.sleep(4)
    
        raise RuntimeError(f"PROVIDER4_HISTORY_DATE_FAILED: {last_error}")

    def extract_daily_done_count(self, history_html: str) -> int:
        """
        Extrae el número mostrado en algo como:
        <b><font size="4">*54*</font></b>
        """
        if not history_html:
            return 0
    
        patterns = [
            r"<b>\s*<font[^>]*>\s*\*(\d+)\*\s*</font>\s*</b>",
            r"<font[^>]*>\s*\*(\d+)\*\s*</font>",
            r"\*(\d+)\*",
        ]
    
        for pattern in patterns:
            m = re.search(pattern, history_html, flags=re.IGNORECASE | re.DOTALL)
            if m:
                value = int(m.group(1))
                print("PROVIDER4_DAILY_DONE_COUNT =", value, flush=True)
                return value
    
        print("PROVIDER4_DAILY_DONE_COUNT_NOT_FOUND = 0", flush=True)
        return 0
    
    def get_done_count_for_date(self, fecha: str) -> int:
        """
        fecha en formato: 31/Mar/2026
        """
        self.warm()
        html = self.get_history_html_for_date(fecha)
        print(f"PROVIDER4_HISTORY_DATE_HTML_PREVIEW_{fecha} =", html[:1500], flush=True)
        return self.extract_daily_done_count(html)
    
    def get_week_done_counts(self, start_date: datetime, end_date: datetime) -> dict:
        """
        Suma por día desde start_date hasta end_date sin incluir end_date.
        Regresa detalle diario + total.
        """
        self.warm()
    
        month_map = {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
        }
    
        rows = []
        total = 0
    
        cur = start_date
        while cur < end_date:
            fecha_param = f"{cur.day:02d}/{month_map[cur.month]}/{cur.year}"
    
            try:
                daily_count = self.get_done_count_for_date(fecha_param)
            except Exception as e:
                print(f"PROVIDER4_WEEK_COUNT_ERROR_{fecha_param} = {str(e)}", flush=True)
                daily_count = 0
    
            rows.append({
                "date": cur.strftime("%Y-%m-%d"),
                "fecha_param": fecha_param,
                "count": daily_count,
            })
            total += daily_count
            cur += timedelta(days=1)
    
        return {
            "rows": rows,
            "total": total,
        }

    def _folio_pdf_direct_url(self, term: str, tipoa: str) -> str:
        tipo_map = {
            "nacimiento": "NAC",
            "matrimonio": "MAT",
            "defuncion": "DEF",
            "divorcio": "DIV",
        }
        code = tipo_map.get((tipoa or "").strip().lower(), "NAC")
        return f"{self.BASE_URL}/servicio/ActasN/{term}_{code}_FOLIO.pdf"

    def _normal_pdf_direct_url(self, term: str, tipoa: str) -> str:
        tipo_map = {
            "nacimiento": "NAC",
            "matrimonio": "MAT",
            "defuncion": "DEF",
            "divorcio": "DIV",
        }
        code = tipo_map.get((tipoa or "").strip().lower(), "NAC")
        return f"{self.BASE_URL}/servicio/d.php?f={term}_{code}"
    
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
            print("PROVIDER4_NO_RECORD_DETECTED =", term, flush=True)
            return True
    
        return False
    
    def _extract_pdf_link(self, history_html: str, term: str) -> str | None:
        term = (term or "").strip().upper()
    
        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        row_match = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)
    
        if not row_match:
            return None
    
        row_html = row_match.group(0)
    
        m = re.search(r'href="(\./d\.php\?f=[^"]+)"', row_html, flags=re.IGNORECASE)
        if m:
            return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))
    
        return None

    def _extract_folio_link(self, history_html: str, term: str) -> str | None:
        term = (term or "").strip().upper()

        row_pattern = rf"<tr>.*?{re.escape(term)}.*?</tr>"
        row_match = re.search(row_pattern, history_html, flags=re.IGNORECASE | re.DOTALL)

        if not row_match:
            return None

        row_html = row_match.group(0)

        # Prioridad: d.php final
        m = re.search(
            r'href="(\./d\.php\?f=[^"]+)"',
            row_html,
            flags=re.IGNORECASE,
        )
        if m:
            return urljoin(f"{self.BASE_URL}/servicio/", m.group(1))

        # Respaldo: addFol.php
        m = re.search(
            r'href="(\./ActasN/addFol\.php\?[^"]+)"',
            row_html,
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
                    html_preview = resp.text[:2000] if resp.text else ""
                    print("PROVIDER4_NON_PDF_HTML_PREVIEW =", html_preview, flush=True)
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

    def _extract_pdf_url_from_html(self, html: str, base_url: str) -> str | None:
        patterns = [
            r'window\.location\s*=\s*"([^"]+)"',
            r"window\.location\s*=\s*'([^']+)'",
            r'location\.href\s*=\s*"([^"]+)"',
            r"location\.href\s*=\s*'([^']+)'",
            r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][^;]+;\s*url=([^"\']+)["\']',
            r'<iframe[^>]+src=["\']([^"\']+)["\']',
            r'<embed[^>]+src=["\']([^"\']+)["\']',
            r'<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\']',
            r'<a[^>]+href=["\']([^"\']*d\.php\?[^"\']*)["\']',
        ]
    
        for pattern in patterns:
            m = re.search(pattern, html, flags=re.IGNORECASE)
            if m:
                return urljoin(base_url, unescape(m.group(1)))
    
        return None

    def _download_foliated_pdf(self, url: str) -> bytes:
        last_error = None
    
        for attempt in range(3):
            try:
                print(f"PROVIDER4_FOLIO_ATTEMPT_{attempt+1}_URL = {url}", flush=True)
    
                resp = self.session.get(
                    url,
                    timeout=(15, 240),
                    headers={
                        "User-Agent": self.session.headers["User-Agent"],
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Referer": self.HISTORY_URL,
                    },
                )
                resp.raise_for_status()
    
                content_type = (resp.headers.get("content-type") or "").lower()
                print("PROVIDER4_FOLIO_CONTENT_TYPE =", content_type, flush=True)
    
                if "pdf" in content_type or resp.content.startswith(b"%PDF"):
                    return resp.content
    
                html = resp.text or ""
                print("PROVIDER4_FOLIO_HTML_PREVIEW =", html[:2000], flush=True)
    
                next_url = self._extract_pdf_url_from_html(html, url)
                if not next_url:
                    raise RuntimeError("PROVIDER4_FOLIO_NO_NEXT_PDF_URL")
    
                print("PROVIDER4_FOLIO_NEXT_URL =", next_url, flush=True)
    
                pdf_resp = self.session.get(
                    next_url,
                    timeout=(15, 240),
                    headers={
                        "User-Agent": self.session.headers["User-Agent"],
                        "Accept": "*/*",
                        "Referer": url,
                    },
                )
                pdf_resp.raise_for_status()
    
                pdf_content_type = (pdf_resp.headers.get("content-type") or "").lower()
                print("PROVIDER4_FOLIO_NEXT_CONTENT_TYPE =", pdf_content_type, flush=True)
    
                if "pdf" not in pdf_content_type and not pdf_resp.content.startswith(b"%PDF"):
                    html_preview = pdf_resp.text[:2000] if pdf_resp.text else ""
                    print("PROVIDER4_FOLIO_NEXT_HTML_PREVIEW =", html_preview, flush=True)
                    raise RuntimeError(f"PROVIDER4_FOLIO_INVALID_FINAL_RESPONSE:{pdf_content_type}")
    
                return pdf_resp.content
    
            except Exception as e:
                last_error = e
                print(f"PROVIDER4_FOLIO_ATTEMPT_{attempt+1}_ERROR = {str(e)}", flush=True)
                if attempt < 2:
                    time.sleep(4)
    
        raise RuntimeError(f"PROVIDER4_FOLIO_DOWNLOAD_FAILED: {last_error}")
    
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
    
        max_polls = self.HISTORY_MAX_POLLS
        poll_sleep_seconds = self.HISTORY_POLL_SLEEP
    
        # Solo intento rápido, NO entrega final sin confirmación de history
        #early_direct_pdf_bytes = None
        #direct_normal_url = self._normal_pdf_direct_url(term, tipoa)
        #print("PROVIDER4_DIRECT_URL =", direct_normal_url, flush=True)

        #try:
        #    early_direct_pdf_bytes = self.download_pdf_bytes(direct_normal_url)
        #    print("PROVIDER4_DIRECT_EARLY_PDF_READY = TRUE", flush=True)
        #except Exception as direct_exc:
        #    print("PROVIDER4_DIRECT_FAILED =", str(direct_exc), flush=True)
    
        history_confirmed = False
    
        for poll_attempt in range(max_polls):
            history_html = self.get_history_html()
            print(
                f"PROVIDER4_HISTORY_HTML_PREVIEW_ATTEMPT_{poll_attempt+1} =",
                history_html[:1500],
                flush=True,
            )
    
            if self._detect_no_result(history_html, term):
                raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
    
            row_html = self._history_row_for_term(history_html, term)
    
            if row_html:
                history_confirmed = True

                #if early_direct_pdf_bytes and not inc_folio:
                #    print("PROVIDER4_FAST_DIRECT_AFTER_HISTORY = TRUE", flush=True)
                #    return early_direct_pdf_bytes
                
                print(
                    f"PROVIDER4_HISTORY_CONFIRMED_ATTEMPT_{poll_attempt+1} = {term}",
                    flush=True,
                )
    
                if inc_folio:
                    link = self._extract_folio_link(history_html, term)
                    if link:
                        print("PROVIDER4_FINAL_FOLIO_LINK =", link, flush=True)

                        if "addFol.php" in link:
                            pdf_bytes = self._download_foliated_pdf(link)
                        else:
                            pdf_bytes = self.download_pdf_bytes(link)
                        
                        pdf_bytes = self._repair_pdf_if_needed(pdf_bytes, term, inc_folio)
                        return pdf_bytes

                    # Si history ya mostró la fila correcta, ahora sí se permite directo
                    #try:
                    #    return self.download_pdf_bytes(direct_normal_url)
                    #except Exception as direct_retry_exc:
                    #    print("PROVIDER4_DIRECT_RETRY_FAILED =", str(direct_retry_exc), flush=True)

                    # Usar early direct SOLO después de confirmación en history
                    #if early_direct_pdf_bytes:
                    #    print("PROVIDER4_USING_CONFIRMED_EARLY_DIRECT_PDF = TRUE", flush=True)
                    #    return early_direct_pdf_bytes
    
                else:
                    link = self._extract_pdf_link(history_html, term)
                    if link:
                        print("PROVIDER4_FINAL_DOWNLOAD_LINK =", link, flush=True)
                        pdf_bytes = self.download_pdf_bytes(link)
                        pdf_bytes = self._repair_pdf_if_needed(pdf_bytes, term, inc_folio)
                        return pdf_bytes
    
                    # Si history ya mostró la fila correcta, ahora sí se permite directo
                    #try:
                    #    return self.download_pdf_bytes(direct_normal_url)
                    #except Exception as direct_retry_exc:
                    #    print("PROVIDER4_DIRECT_RETRY_FAILED =", str(direct_retry_exc), flush=True)
                    
                    # Usar early direct SOLO después de confirmación en history
                    #if early_direct_pdf_bytes:
                    #    print("PROVIDER4_USING_CONFIRMED_EARLY_DIRECT_PDF = TRUE", flush=True)
                    #    return early_direct_pdf_bytes
    
            print(
                f"PROVIDER4_HISTORY_LINK_NOT_READY_ATTEMPT_{poll_attempt+1} = {term}",
                flush=True,
            )
            time.sleep(poll_sleep_seconds)
    
        final_history_html = self.get_history_html()
    
        if self._detect_no_result(final_history_html, term):
            raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
    
        # Ya NO usar early direct sin confirmación de history
        if not history_confirmed:
            if inc_folio:
                raise RuntimeError(f"PROVIDER4_HISTORY_NOT_CONFIRMED_FOLIO:{term}")
            else:
                raise RuntimeError(f"PROVIDER4_HISTORY_NOT_CONFIRMED_PDF:{term}")
    
        if inc_folio:
            raise RuntimeError(f"PROVIDER4_NO_FOLIO_LINK_FOR:{term}")
        else:
            raise RuntimeError(f"PROVIDER4_NO_PDF_LINK_FOR:{term}")
