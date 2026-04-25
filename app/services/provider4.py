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
    HID = "L4zar0"

    MANUAL_PAGE_URL = f"{BASE_URL}/servicio/manual.php?HID={HID}"
    MANUAL_ENDPOINT = f"{BASE_URL}/servicio/backend-manualCVL.php"
    VGET_URL = f"{BASE_URL}/servicio/vGetOfi.php"
    HISTORY_URL = f"{BASE_URL}/servicio/vHistory.php?HID={HID}"

    HISTORY_MAX_POLLS = 60
    HISTORY_POLL_SLEEP = 7

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

    def _extract_pdf_visible_text(self, pdf_bytes: bytes) -> str:
        parts = []
    
        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            for page in reader.pages:
                try:
                    txt = page.extract_text() or ""
                except Exception:
                    txt = ""
                if txt:
                    parts.append(txt)
        except Exception as e:
            print("PROVIDER4_PDF_TEXT_EXTRACT_ERROR =", str(e), flush=True)
    
        text = "\n".join(parts).upper().strip()
    
        if not text:
            try:
                text = pdf_bytes.decode("latin1", errors="ignore").upper()
            except Exception:
                text = ""
    
        return text
    
    
    def _normalize_alnum(self, value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", (value or "").upper())
    
    
    def _find_curps_in_text(self, text: str) -> list[str]:
        if not text:
            return []
    
        pattern = r"[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d"
        found = re.findall(pattern, text, flags=re.IGNORECASE)
    
        unique = []
        seen = set()
        for item in found:
            curp = item.upper()
            if curp not in seen:
                seen.add(curp)
                unique.append(curp)
    
        return unique
    
    def _pdf_matches_expected(self, pdf_bytes: bytes, expected_curp: str, tipoa: str) -> bool:
        text = self._extract_pdf_visible_text(pdf_bytes)
        if not text or len(text.strip()) < 30:
            print("PROVIDER4_VALIDATE_TEXT_TOO_SHORT = TRUE", flush=True)
            return False
    
        expected = self._normalize_alnum(expected_curp)
        found_curps = self._find_curps_in_text(text)
    
        print("PROVIDER4_VALIDATE_EXPECTED_CURP =", expected, flush=True)
        print("PROVIDER4_VALIDATE_FOUND_CURPS =", found_curps, flush=True)
        print("PROVIDER4_VALIDATE_TIPOA =", tipoa, flush=True)
    
        # Si se detectan CURPs visibles, la esperada debe aparecer.
        # Ya NO rechazar solo porque existan varias.
        if found_curps:
            if expected not in found_curps:
                return False
    
            if len(found_curps) > 1:
                print("PROVIDER4_VALIDATE_MULTIPLE_CURPS_ALLOWED = TRUE", flush=True)
    
        text_up = text.upper()
        tipoa_up = (tipoa or "").strip().lower()
    
        if tipoa_up == "nacimiento" and "ACTA DE NACIMIENTO" not in text_up:
            return False
        if tipoa_up == "matrimonio" and "ACTA DE MATRIMONIO" not in text_up:
            return False
        if tipoa_up == "defuncion" and ("ACTA DE DEFUNCION" not in text_up and "ACTA DE DEFUNCIÓN" not in text_up):
            return False
        if tipoa_up == "divorcio" and "ACTA DE DIVORCIO" not in text_up:
            return False
    
        return True

    def _download_and_validate_with_retries(
        self,
        *,
        url: str,
        term: str,
        tipoa: str,
        inc_folio: bool,
        use_folio_downloader: bool = False,
        max_attempts: int = 4,
        sleep_seconds: int = 4,
    ) -> bytes:
        for attempt in range(max_attempts):
            print(f"PROVIDER4_VALIDATE_DOWNLOAD_ATTEMPT_{attempt+1}_URL = {url}", flush=True)
    
            if use_folio_downloader:
                pdf_bytes = self._download_foliated_pdf(url)
            else:
                pdf_bytes = self.download_pdf_bytes(url)
    
            for retry_complete in range(8):
                if self._pdf_has_two_pages(pdf_bytes):
                    print("PROVIDER4_COMPLETE_PDF_FROM_LAZARO =", retry_complete + 1, flush=True)
                    break
    
                print("PROVIDER4_PDF_NOT_COMPLETE_RETRY_DOWNLOAD =", retry_complete + 1, flush=True)
                time.sleep(5)
                pdf_bytes = self._download_foliated_pdf(url) if use_folio_downloader else self.download_pdf_bytes(url)
    
            pdf_bytes = self._repair_pdf_if_needed(pdf_bytes, term, inc_folio)
    
            if not self._pdf_has_two_pages(pdf_bytes):
                print("PROVIDER4_FINAL_PDF_STILL_ONE_PAGE_RETRY =", term, flush=True)
                if attempt < max_attempts - 1:
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(f"PROVIDER4_FINAL_PDF_INCOMPLETE:{term}")
    
            if self._pdf_matches_expected(pdf_bytes, term, tipoa):
                print(f"PROVIDER4_VALIDATE_DOWNLOAD_OK_ATTEMPT_{attempt+1} = {term}", flush=True)
                return pdf_bytes
    
            print(f"PROVIDER4_VALIDATE_DOWNLOAD_BAD_ATTEMPT_{attempt+1} = {term}", flush=True)
    
            if attempt < max_attempts - 1:
                time.sleep(sleep_seconds)
    
        raise RuntimeError(f"PROVIDER4_WRONG_CURP_IN_PDF:{term}")

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
    
        print("PROVIDER4_PDF_ONE_PAGE_OR_INCOMPLETE = TRUE", flush=True)
        print("PROVIDER4_PDF_INCOMPLETE_REPAIRING = TRUE", flush=True)
    
        original_pdf_bytes = pdf_bytes
    
        try:
            estado = self._estado_desde_curp(term)
        except Exception as e:
            print("PROVIDER4_REPAIR_SKIP_STATE_ERROR_NO_SEND =", str(e), flush=True)
            raise RuntimeError(f"PROVIDER4_CANNOT_REPAIR_NO_STATE:{term}")
    
        base_dir = Path(__file__).resolve().parent.parent
        estados_dir = base_dir / "assets" / "estados"
    
        try:
            framed_pdf = _enmarcar_pdf_frente(
                original_pdf_bytes,
                f"{term}.pdf",
                folio=inc_folio,
            )
        except Exception as e:
            print("PROVIDER7_FRAME_FAILED_NO_SEND =", str(e), flush=True)
            raise RuntimeError(f"PROVIDER7_FRAME_FAILED:{term}")
    
        if estado == "NACIDO_EN_EL_EXTRANJERO":
            if self._pdf_has_two_pages(framed_pdf):
                print("PROVIDER4_FOREIGN_BIRTH_FRAMED_OK = TRUE", flush=True)
                return framed_pdf
            raise RuntimeError(f"PROVIDER4_FOREIGN_BIRTH_FRAME_INCOMPLETE:{term}")
    
        try:
            reverso_path = _resolver_reverso_por_estado(estado, estados_dir)
            repaired_pdf = _unir_pdfs_bytes(framed_pdf, reverso_path)
        except Exception as e:
            print("PROVIDER4_REAR_JOIN_FAILED_NO_SEND =", str(e), flush=True)
            raise RuntimeError(f"PROVIDER4_REAR_JOIN_FAILED:{term}")
    
        if not self._pdf_has_two_pages(repaired_pdf):
            print("PROVIDER4_REPAIRED_STILL_INCOMPLETE_NO_SEND =", term, flush=True)
            raise RuntimeError(f"PROVIDER4_REPAIRED_STILL_INCOMPLETE:{term}")
    
        print(f"PROVIDER4_PDF_PAGE_COUNT = {self._pdf_num_pages(repaired_pdf)}", flush=True)
        return repaired_pdf

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
                self.warm()
    
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
        #print(f"PROVIDER4_HISTORY_DATE_HTML_PREVIEW_{fecha} =", html[:1500], flush=True)
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
    
    def _history_row_for_term(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        term_up = (term or "").strip().upper()
        tipoa_up = (tipoa or "").strip().upper()
    
        rows = re.findall(r"<tr\b[^>]*>.*?</tr>", history_html or "", flags=re.IGNORECASE | re.DOTALL)
    
        for row_html in rows:
            row_text = unescape(re.sub(r"<[^>]+>", " ", row_html))
            row_text = re.sub(r"\s+", " ", row_text).strip().upper()
    
            if term_up not in row_text:
                continue
    
            if tipoa_up:
                tipo_map = {
                    "NACIMIENTO": "NACIMIENTO",
                    "MATRIMONIO": "MATRIMONIO",
                    "DEFUNCION": "DEFUNCION",
                    "DIVORCIO": "DIVORCIO",
                }
                expected_tipo = tipo_map.get(tipoa_up, tipoa_up)
                if expected_tipo not in row_text:
                    continue
    
            print("PROVIDER4_HISTORY_ROW_MATCHED_TERM =", term_up, flush=True)
            print("PROVIDER4_HISTORY_ROW_MATCHED_TIPOA =", tipoa_up, flush=True)
            print("PROVIDER4_HISTORY_ROW_TEXT =", row_text[:500], flush=True)
            return row_html
    
        return None
    
    def _detect_no_result(self, history_html: str, term: str, tipoa: str | None = None) -> bool:
        row_html = self._history_row_for_term(history_html, term, tipoa)
    
        if not row_html:
            return False
    
        row_up = row_html.upper()
    
        if "NO_LOCALIZADO" in row_up:
            print("PROVIDER4_NO_RECORD_DETECTED =", term, flush=True)
            return True
    
        return False
    
    def _extract_pdf_link(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        row_html = self._history_row_for_term(history_html, term, tipoa)
        if not row_html:
            return None
    
        m = re.search(r'href="(\./d\.php\?f=[^"]+)"', row_html, flags=re.IGNORECASE)
        if m:
            link = urljoin(f"{self.BASE_URL}/servicio/", m.group(1))
            print("PROVIDER4_EXTRACTED_PDF_LINK =", link, flush=True)
            return link
    
        return None

    def _extract_folio_link(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        row_html = self._history_row_for_term(history_html, term, tipoa)
        if not row_html:
            return None
    
        # Prioridad real: d.php = PDF final correcto
        m = re.search(
            r'href="(\./d\.php\?f=[^"]+)"',
            row_html,
            flags=re.IGNORECASE,
        )
        if m:
            link = urljoin(f"{self.BASE_URL}/servicio/", m.group(1))
            print("PROVIDER4_EXTRACTED_FOLIO_LINK =", link, flush=True)
            return link
    
        # Respaldo: addFol.php
        m = re.search(
            r'href="(\./ActasN/addFol\.php\?[^"]+)"',
            row_html,
            flags=re.IGNORECASE,
        )
        if m:
            link = urljoin(f"{self.BASE_URL}/servicio/", m.group(1))
            print("PROVIDER4_EXTRACTED_FOLIO_LINK =", link, flush=True)
            return link
    
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
        
        backend_up = (html or "").upper()
        term_up = (term or "").strip().upper()
        
        if "NO_LOCALIZADO" in backend_up and term_up in backend_up:
            print("PROVIDER4_NO_RECORD_DETECTED_IN_BACKEND =", term_up, flush=True)
            raise RuntimeError(f"PROVIDER4_NO_RECORD:{term_up}")
    
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
    
            if self._detect_no_result(history_html, term, tipoa):
                raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
    
            row_html = self._history_row_for_term(history_html, term, tipoa)
    
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
                    dphp_link = self._extract_pdf_link(history_html, term, tipoa)
                    folio_link = self._extract_folio_link(history_html, term, tipoa)
                
                    print("PROVIDER4_INC_FOLIO_DPHP_LINK =", dphp_link, flush=True)
                    print("PROVIDER4_INC_FOLIO_FOLIO_LINK =", folio_link, flush=True)
                
                    final_link = dphp_link or folio_link
                    print("PROVIDER4_FINAL_FOLIO_LINK =", final_link, flush=True)
                
                    if final_link:
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=final_link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            use_folio_downloader=False,
                            max_attempts=4,
                            sleep_seconds=4,
                        )
                        return pdf_bytes

                    print(f"PROVIDER4_HISTORY_ROW_FOUND_BUT_FOLIO_LINK_MISSING_ATTEMPT_{poll_attempt+1} = {term}", flush=True)

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
                    link = self._extract_pdf_link(history_html, term, tipoa)
                    if link:
                        print("PROVIDER4_FINAL_DOWNLOAD_LINK =", link, flush=True)
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            use_folio_downloader=False,
                            max_attempts=4,
                            sleep_seconds=4,
                        )
                        return pdf_bytes
                
                    print(f"PROVIDER4_HISTORY_ROW_FOUND_BUT_LINK_MISSING_ATTEMPT_{poll_attempt+1} = {term}", flush=True)
    
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

        if self._detect_no_result(final_history_html, term, tipoa):
            raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
        
        if not history_confirmed:
            if inc_folio:
                raise RuntimeError(f"PROVIDER4_HISTORY_NOT_CONFIRMED_FOLIO:{term}")
            else:
                raise RuntimeError(f"PROVIDER4_HISTORY_NOT_CONFIRMED_PDF:{term}")
        
        # =====================================================
        # FASE EXTRA: history ya confirmado, esperar solo el link
        # =====================================================
        extra_link_polls = 12  # ~84 segundos extra si HISTORY_POLL_SLEEP=7
        
        for extra_attempt in range(extra_link_polls):
            history_html = self.get_history_html()
        
            row_html = self._history_row_for_term(history_html, term, tipoa)
            if row_html:
                if inc_folio:
                    dphp_link = self._extract_pdf_link(history_html, term, tipoa)
                    folio_link = self._extract_folio_link(history_html, term, tipoa)
                    final_link = dphp_link or folio_link
        
                    if final_link:
                        print(f"PROVIDER4_LATE_FOLIO_LINK_FOUND_ATTEMPT_{extra_attempt+1} = {final_link}", flush=True)
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=final_link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            use_folio_downloader=False,
                            max_attempts=4,
                            sleep_seconds=4,
                        )
                        return pdf_bytes
                else:
                    link = self._extract_pdf_link(history_html, term, tipoa)
                    if link:
                        print(f"PROVIDER4_LATE_PDF_LINK_FOUND_ATTEMPT_{extra_attempt+1} = {link}", flush=True)
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            use_folio_downloader=False,
                            max_attempts=4,
                            sleep_seconds=4,
                        )
                        return pdf_bytes
        
            print(f"PROVIDER4_LATE_LINK_STILL_MISSING_ATTEMPT_{extra_attempt+1} = {term}", flush=True)
            time.sleep(self.HISTORY_POLL_SLEEP)
        
        if inc_folio:
            raise RuntimeError(f"PROVIDER4_NO_FOLIO_LINK_FOR:{term}")
        else:
            raise RuntimeError(f"PROVIDER4_NO_PDF_LINK_FOR:{term}")
