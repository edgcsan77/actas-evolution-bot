import re
import time
import requests
import base64
import json
import unicodedata
from datetime import datetime, timedelta
from html import unescape
from urllib.parse import urljoin

from io import BytesIO
from pypdf import PdfReader, PdfWriter, Transformation, PageObject
from pathlib import Path

from app.services.provider7 import (
    _enmarcar_pdf_frente as _enmarcar_pdf_frente_provider7,
    _unir_pdfs_bytes,
    _unir_pdfs_bytes_raw,
    _resolver_reverso_por_estado,
    _estado_desde_cadena,
    _solo_pagina_pdf,
    _pdf_page_has_visible_content,
    _pdf_front_has_green_frame,
)


class Provider4Client:
    BASE_URL = "https://www.tramitesfull.net"
    DEFAULT_HID = "D0cuExServ1"

    HISTORY_MAX_POLLS = 90
    HISTORY_POLL_SLEEP = 5

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

    def __init__(self, hid: str | None = None) -> None:
        self.HID = hid or self.DEFAULT_HID

        self.MANUAL_PAGE_URL = f"{self.BASE_URL}/servicio/manual.php?HID={self.HID}"
        self.MANUAL_ENDPOINT = f"{self.BASE_URL}/servicio/vGetOfi2.php"
        self.VGET_URL = f"{self.BASE_URL}/servicio/vGetOfi2.php"
        self.VGET_OFI_URL = f"{self.BASE_URL}/servicio/vGetOfi.php"
        self.HISTORY_URL = f"{self.BASE_URL}/servicio/vHistory.php?HID={self.HID}"

        self.NEW_PETICION_URL = f"{self.BASE_URL}/servicio/peticion.php"
        self.NEW_VERIFICAR_PDF_URL = f"{self.BASE_URL}/servicio/verificarpdf.php"
    
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

    def _b64(self, value: str) -> str:
        return base64.b64encode((value or "").encode("utf-8")).decode("ascii")

    def consultar_por_curp_folio_vgetofi(self, curp: str, tipoa: str = "nacimiento") -> str:
        curp_clean = (curp or "").strip().upper()
        tipo_norm = (tipoa or "nacimiento").strip().lower()

        datos_obj = {
            "curp": curp_clean,
        }

        data = {
            "p1": "true",
            "p4": self._b64(self.HID),
            "p6": self._b64(curp_clean),
            "p7": self._b64(tipo_norm),
            "incF": "true",
            "cadena": "datos",
            "datos": self._b64(json.dumps(datos_obj, ensure_ascii=False, separators=(",", ":"))),
            "valor": self._b64("tramiINE20"),
            "continuar": "",
        }

        print("PROVIDER4_VGETOFI_FOLIO_URL =", self.VGET_OFI_URL, flush=True)
        print("PROVIDER4_VGETOFI_FOLIO_DATA =", data, flush=True)

        r = self.session.post(
            self.VGET_OFI_URL,
            data=data,
            timeout=60,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.MANUAL_PAGE_URL,
                "Origin": self.BASE_URL,
                "User-Agent": "Mozilla/5.0",
            },
        )

        print("PROVIDER4_VGETOFI_FOLIO_STATUS =", r.status_code, flush=True)
        print("PROVIDER4_VGETOFI_FOLIO_PREVIEW =", (r.text or "")[:500], flush=True)

        r.raise_for_status()
        return r.text or ""

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
    
    def _pdf_matches_expected(
        self,
        pdf_bytes: bytes,
        expected_curp: str,
        tipoa: str,
        is_chain: bool = False,
    ) -> bool:
        text = self._extract_pdf_visible_text(pdf_bytes)
        text_up = (text or "").upper()
    
        expected = self._normalize_alnum(expected_curp)
    
        # Si no se pudo extraer texto confiable, NO rechazar aquí.
        # El worker ya hace validación posterior con _validate_pdf_term_detailed.
        if not text or len(text.strip()) < 30:
            print("PROVIDER4_VALIDATE_TEXT_TOO_SHORT_SOFT_PASS = TRUE", flush=True)
            return True
    
        # Cadena: no validar tipo; la cadena identifica el acta.
        if is_chain:
            print("PROVIDER4_VALIDATE_CHAIN_MODE = TRUE", flush=True)
    
            expected_chain = self._normalize_alnum(expected_curp)
            normalized_text = self._normalize_alnum(text_up)
    
            if expected_chain and expected_chain in normalized_text:
                print("PROVIDER4_VALIDATE_CHAIN_FOUND_IN_PDF = TRUE", flush=True)
                return True
    
            print("PROVIDER4_VALIDATE_CHAIN_NOT_FOUND_SOFT_PASS = TRUE", flush=True)
            return True
    
        tipoa_up = (tipoa or "").strip().lower()
    
        detected_types = set()
    
        if "ACTA DE NACIMIENTO" in text_up:
            detected_types.add("nacimiento")
    
        if "ACTA DE MATRIMONIO" in text_up:
            detected_types.add("matrimonio")
    
        if "ACTA DE DEFUNCION" in text_up or "ACTA DE DEFUNCIÓN" in text_up:
            detected_types.add("defuncion")
    
        if "ACTA DE DIVORCIO" in text_up:
            detected_types.add("divorcio")
    
        print("PROVIDER4_VALIDATE_DETECTED_TYPES =", detected_types, flush=True)
        print("PROVIDER4_VALIDATE_EXPECTED_TIPOA =", tipoa_up, flush=True)
    
        # Solo rechazar si detectó claramente otro tipo.
        # Si no detectó tipo, soft-pass.
        if detected_types and tipoa_up in {"nacimiento", "matrimonio", "defuncion", "divorcio"}:
            if tipoa_up not in detected_types:
                print("PROVIDER4_VALIDATE_WRONG_ACT_TYPE_CONFIRMED = TRUE", flush=True)
                return False
    
        found_curps = self._find_curps_in_text(text)
    
        print("PROVIDER4_VALIDATE_EXPECTED_CURP =", expected, flush=True)
        print("PROVIDER4_VALIDATE_FOUND_CURPS =", found_curps, flush=True)
        print("PROVIDER4_VALIDATE_TIPOA =", tipoa, flush=True)
    
        # Si encontró CURP(s) internas, ahí sí debe estar la esperada.
        # Si hay una CURP interna diferente, rechazar.
        if found_curps:
            if expected not in found_curps:
                print("PROVIDER4_VALIDATE_WRONG_INTERNAL_CURP_CONFIRMED = TRUE", flush=True)
                return False
    
            if len(found_curps) > 1:
                print("PROVIDER4_VALIDATE_MULTIPLE_CURPS_ALLOWED = TRUE", flush=True)
    
            return True
    
        # Si no encontró ninguna CURP interna, NO rechazar aquí.
        # El worker/main hacen la validación final.
        normalized_text = self._normalize_alnum(text_up)
    
        if expected and expected in normalized_text:
            print("PROVIDER4_VALIDATE_EXPECTED_FOUND_IN_NORMALIZED_TEXT = TRUE", flush=True)
            return True
    
        print("PROVIDER4_VALIDATE_NO_INTERNAL_CURP_SOFT_PASS = TRUE", flush=True)
        return True

    def _download_and_validate_with_retries(
        self,
        *,
        url: str,
        term: str,
        tipoa: str,
        inc_folio: bool,
        is_chain: bool = False,
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
            
            try:
                self._assert_pdf_readable(pdf_bytes, term)
            except Exception as e:
                print("PROVIDER4_DOWNLOADED_PDF_BAD_RETRY =", str(e), flush=True)
            
                if attempt < max_attempts - 1:
                    time.sleep(sleep_seconds)
                    continue
            
                raise
    
            for retry_complete in range(8):
                if self._pdf_has_two_pages(pdf_bytes):
                    print("PROVIDER4_COMPLETE_PDF_FROM_LAZARO =", retry_complete + 1, flush=True)
                    break
    
                print("PROVIDER4_PDF_NOT_COMPLETE_RETRY_DOWNLOAD =", retry_complete + 1, flush=True)
                time.sleep(5)
                
                pdf_bytes = self._download_foliated_pdf(url) if use_folio_downloader else self.download_pdf_bytes(url)
                try:
                    self._assert_pdf_readable(pdf_bytes, term)
                except Exception as e:
                    print("PROVIDER4_REDOWNLOADED_PDF_BAD =", str(e), flush=True)
                    break
    
            if not is_chain:
                # Si viene de addFol.php, el folio ya lo puso Lázaro.
                # No pasamos inc_folio=True al reparador para evitar folio doble/encimado.
                repair_as_folio = inc_folio and not use_folio_downloader
                pdf_bytes = self._repair_pdf_if_needed(pdf_bytes, term, repair_as_folio)
            else:
                # Para cadena, Lázaro puede devolver solo frente.
                # El estado se obtiene de la cadena y se agrega reverso de Provider7/assets/estados.
                pdf_bytes = self._repair_chain_pdf_if_needed(pdf_bytes, term)
    
            if not self._pdf_has_two_pages(pdf_bytes):
                print("PROVIDER4_FINAL_PDF_STILL_ONE_PAGE_RETRY =", term, flush=True)
                if attempt < max_attempts - 1:
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(f"PROVIDER4_FINAL_PDF_INCOMPLETE:{term}")
    
            if self._pdf_matches_expected(pdf_bytes, term, tipoa, is_chain=is_chain):
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

    def _assert_pdf_readable(self, pdf_bytes: bytes, term: str = "") -> None:
        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            pages = len(reader.pages)
    
            if pages < 1:
                raise RuntimeError("PDF_WITHOUT_PAGES")
    
            # Fuerza lectura real de la primera página
            try:
                _ = reader.pages[0].mediabox
            except Exception:
                pass
    
        except Exception as e:
            print("PROVIDER4_PDF_NOT_READABLE =", term, str(e), flush=True)
            raise RuntimeError(f"PROVIDER4_PDF_NOT_READABLE:{term}:{str(e)[:300]}")

    def _normalize_pdf_pages_to_letter(
        self,
        pdf_bytes: bytes,
        term: str = "",
        label: str = "PDF",
    ) -> bytes:
        """
        Normaliza páginas a Letter 8.5x11 pulgadas = 612x792 pts.

        Esto corrige PDFs de Lázaro que vienen en A4/mediaBox raro.
        Importante:
        - NO estira el contenido.
        - Mantiene proporción.
        - Centra la página.
        - Evita que _enmarcar_pdf_frente o _unir_pdfs_bytes
          achiquen/aplasten algunas actas.
        """
        LETTER_W = 612.0
        LETTER_H = 792.0
        TOL = 2.0

        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            writer = PdfWriter()
            changed = False

            for idx, page in enumerate(reader.pages):
                try:
                    page.transfer_rotation_to_content()
                except Exception:
                    pass

                try:
                    w = float(page.mediabox.width)
                    h = float(page.mediabox.height)
                except Exception:
                    writer.add_page(page)
                    continue

                print(
                    "PROVIDER4_PDF_PAGE_SIZE =",
                    {
                        "label": label,
                        "term": term,
                        "page": idx + 1,
                        "width": round(w, 2),
                        "height": round(h, 2),
                    },
                    flush=True,
                )

                # Ya está en Letter.
                if abs(w - LETTER_W) <= TOL and abs(h - LETTER_H) <= TOL:
                    writer.add_page(page)
                    continue

                if w <= 0 or h <= 0:
                    writer.add_page(page)
                    continue

                # Escalado proporcional. Nunca usar sx/sy separados porque eso deforma.
                scale = min(LETTER_W / w, LETTER_H / h)
                tx = (LETTER_W - (w * scale)) / 2.0
                ty = (LETTER_H - (h * scale)) / 2.0

                blank = PageObject.create_blank_page(
                    width=LETTER_W,
                    height=LETTER_H,
                )

                transform = Transformation().scale(scale).translate(tx, ty)
                blank.merge_transformed_page(page, transform, expand=False)

                writer.add_page(blank)
                changed = True

                print(
                    "PROVIDER4_PDF_NORMALIZED_TO_LETTER =",
                    {
                        "label": label,
                        "term": term,
                        "page": idx + 1,
                        "old_width": round(w, 2),
                        "old_height": round(h, 2),
                        "scale": round(scale, 6),
                        "tx": round(tx, 2),
                        "ty": round(ty, 2),
                    },
                    flush=True,
                )

            if not changed:
                return pdf_bytes

            out = BytesIO()
            writer.write(out)
            return out.getvalue()

        except Exception as e:
            print(
                "PROVIDER4_NORMALIZE_TO_LETTER_FAILED =",
                {
                    "label": label,
                    "term": term,
                    "error": str(e),
                },
                flush=True,
            )
            return pdf_bytes

    def _pdf_first_page_is_letter(self, pdf_bytes: bytes, term: str = "") -> bool:
        """
        Detecta si la primera página ya viene en tamaño Letter 612x792.
        En Lázaro/Provider4/10/11, si el frente ya viene Letter y trae texto válido,
        NO debemos reenmarcarlo porque se reduce visualmente.
        """
        LETTER_W = 612.0
        LETTER_H = 792.0
        TOL = 2.0

        try:
            reader = PdfReader(BytesIO(pdf_bytes))

            if len(reader.pages) < 1:
                return False

            page = reader.pages[0]

            try:
                page.transfer_rotation_to_content()
            except Exception:
                pass

            w = float(page.mediabox.width)
            h = float(page.mediabox.height)

            is_letter = (
                abs(w - LETTER_W) <= TOL
                and abs(h - LETTER_H) <= TOL
            )

            print(
                "PROVIDER4_FRONT_IS_LETTER_CHECK =",
                {
                    "term": term,
                    "width": round(w, 2),
                    "height": round(h, 2),
                    "is_letter": bool(is_letter),
                },
                flush=True,
            )

            return bool(is_letter)

        except Exception as e:
            print(
                "PROVIDER4_FRONT_IS_LETTER_CHECK_FAILED =",
                {
                    "term": term,
                    "error": str(e),
                },
                flush=True,
            )
            return False

    def _enmarcar_pdf_frente_lazaro_letter_casi_entero(
        self,
        pdf_bytes: bytes,
        filename: str,
        folio: bool = False,
    ) -> bytes:
        """
        Para Provider4/10/11 cuando Lázaro devuelve frente Letter 612x792
        sin marco verde detectable.

        Objetivo:
        - Poner marco verde.
        - NO achicar como antes.
        - Mantener casi todo el tamaño original.
        """
        LETTER_W = 612.0
        LETTER_H = 792.0

        # 0.84 quedó chico.
        # 0.96 deja visible el marco, pero conserva el acta casi completa.
        CONTENT_SCALE = 0.96

        try:
            base_dir = Path(__file__).resolve().parent.parent
            frame_path = base_dir / "assets" / "MARCO-ACTA-DE-NACIMIENTO.pdf"

            frame_reader = PdfReader(str(frame_path))
            src_reader = PdfReader(BytesIO(pdf_bytes))

            frame_page = frame_reader.pages[0]
            src_page = src_reader.pages[0]

            try:
                src_page.transfer_rotation_to_content()
            except Exception:
                pass

            try:
                sw = float(src_page.mediabox.width)
                sh = float(src_page.mediabox.height)
            except Exception:
                sw = LETTER_W
                sh = LETTER_H

            # Si por alguna razón no viene Letter, normalizar antes.
            if abs(sw - LETTER_W) > 2 or abs(sh - LETTER_H) > 2:
                pdf_bytes = self._normalize_pdf_pages_to_letter(
                    pdf_bytes,
                    term=filename,
                    label="PROVIDER4_LAZARO_LETTER_FRAME_PRE_NORMALIZE",
                )
                src_reader = PdfReader(BytesIO(pdf_bytes))
                src_page = src_reader.pages[0]
                sw = float(src_page.mediabox.width)
                sh = float(src_page.mediabox.height)

            out_page = PageObject.create_blank_page(
                width=LETTER_W,
                height=LETTER_H,
            )

            # Marco primero.
            out_page.merge_page(frame_page)

            scale = CONTENT_SCALE
            tx = (LETTER_W - (sw * scale)) / 2.0
            ty = (LETTER_H - (sh * scale)) / 2.0

            transform = Transformation().scale(scale).translate(tx, ty)

            # Acta encima, casi completa, dejando visible el marco.
            out_page.merge_transformed_page(
                src_page,
                transform,
                expand=False,
            )

            writer = PdfWriter()
            writer.add_page(out_page)

            out = BytesIO()
            writer.write(out)

            print(
                "PROVIDER4_LAZARO_LETTER_FRAME_OK =",
                {
                    "filename": filename,
                    "folio": bool(folio),
                    "frame": str(frame_path),
                    "scale": scale,
                    "tx": round(tx, 2),
                    "ty": round(ty, 2),
                    "src_width": round(sw, 2),
                    "src_height": round(sh, 2),
                },
                flush=True,
            )

            return out.getvalue()

        except Exception as e:
            print(
                "PROVIDER4_LAZARO_LETTER_FRAME_FAILED_FALLBACK_SOFT =",
                {
                    "filename": filename,
                    "error": str(e),
                },
                flush=True,
            )

            return self._enmarcar_pdf_frente_lazaro_suave(
                pdf_bytes,
                filename,
                folio=folio,
            )

    def _enmarcar_pdf_frente_lazaro_suave(
        self,
        pdf_bytes: bytes,
        filename: str,
        folio: bool = False,
    ) -> bytes:
        """
        Enmarcado especial para Provider4/10/11.

        Motivo:
        - Lázaro a veces entrega frente Letter 612x792 sin marco verde detectable.
        - El helper general de Provider7 lo reduce demasiado; logs: scale 0.7475.
        - Aquí usamos una escala más suave para que el acta no quede tan chica.

        Importante:
        - Solo se usa para Provider4/10/11.
        - Mantiene Letter 612x792.
        - No deforma.
        - Centra el contenido.
        """
        LETTER_W = 612.0
        LETTER_H = 792.0

        # Ajuste principal:
        # 0.7475 era demasiado pequeño.
        # 0.84 deja margen para el marco, pero no achica tanto el acta.
        CONTENT_SCALE = 0.84

        try:
            base_dir = Path(__file__).resolve().parent.parent
            frame_path = base_dir / "assets" / "MARCO-ACTA-DE-NACIMIENTO.pdf"

            frame_reader = PdfReader(str(frame_path))
            src_reader = PdfReader(BytesIO(pdf_bytes))

            frame_page = frame_reader.pages[0]
            src_page = src_reader.pages[0]

            try:
                src_page.transfer_rotation_to_content()
            except Exception:
                pass

            try:
                sw = float(src_page.mediabox.width)
                sh = float(src_page.mediabox.height)
            except Exception:
                sw = LETTER_W
                sh = LETTER_H

            # Asegurar que el frente entra en Letter antes del marco.
            if abs(sw - LETTER_W) > 2 or abs(sh - LETTER_H) > 2:
                pdf_bytes = self._normalize_pdf_pages_to_letter(
                    pdf_bytes,
                    term=filename,
                    label="PROVIDER4_LAZARO_SOFT_FRAME_PRE_NORMALIZE",
                )
                src_reader = PdfReader(BytesIO(pdf_bytes))
                src_page = src_reader.pages[0]
                sw = float(src_page.mediabox.width)
                sh = float(src_page.mediabox.height)

            # Página base siempre Letter.
            out_page = PageObject.create_blank_page(
                width=LETTER_W,
                height=LETTER_H,
            )

            # Primero ponemos el marco.
            out_page.merge_page(frame_page)

            scale = CONTENT_SCALE
            tx = (LETTER_W - (sw * scale)) / 2.0
            ty = (LETTER_H - (sh * scale)) / 2.0

            transform = Transformation().scale(scale).translate(tx, ty)

            # Luego ponemos el acta encima, más grande que antes.
            out_page.merge_transformed_page(
                src_page,
                transform,
                expand=False,
            )

            writer = PdfWriter()
            writer.add_page(out_page)

            out = BytesIO()
            writer.write(out)

            print(
                "PROVIDER4_LAZARO_SOFT_FRAME_OK =",
                {
                    "filename": filename,
                    "folio": bool(folio),
                    "frame": str(frame_path),
                    "scale": scale,
                    "tx": round(tx, 2),
                    "ty": round(ty, 2),
                    "src_width": round(sw, 2),
                    "src_height": round(sh, 2),
                },
                flush=True,
            )

            return out.getvalue()

        except Exception as e:
            print(
                "PROVIDER4_LAZARO_SOFT_FRAME_FAILED_FALLBACK_PROVIDER7 =",
                {
                    "filename": filename,
                    "error": str(e),
                },
                flush=True,
            )

            # Fallback al método anterior si algo falla.
            return _enmarcar_pdf_frente_provider7(
                pdf_bytes,
                filename,
                folio=folio,
            )

    def _estado_desde_curp(self, curp: str) -> str:
        curp = (curp or "").strip().upper()
        if len(curp) < 13:
            raise RuntimeError("PROVIDER4_CURP_INVALID_FOR_STATE")

        clave = curp[11:13]
        estado = self.MAPA_ESTADOS_CURP.get(clave)

        if not estado:
            raise RuntimeError(f"PROVIDER4_STATE_NOT_FOUND:{clave}")

        return estado

    def _estado_desde_pdf_o_curp(self, pdf_bytes: bytes, curp: str) -> str:
        """
        Para reverso local, preferimos la Entidad de Registro visible en el PDF.
        Si no se puede detectar, caemos al estado del CURP.
        """
        text = self._extract_pdf_visible_text(pdf_bytes)
        text = re.sub(r"\s+", " ", (text or "").upper()).strip()
    
        estados = [
            "AGUASCALIENTES",
            "BAJA CALIFORNIA SUR",
            "BAJA CALIFORNIA",
            "CAMPECHE",
            "COAHUILA",
            "COLIMA",
            "CHIAPAS",
            "CHIHUAHUA",
            "CIUDAD DE MEXICO",
            "CIUDAD DE MÉXICO",
            "DURANGO",
            "GUANAJUATO",
            "GUERRERO",
            "HIDALGO",
            "JALISCO",
            "MEXICO",
            "MÉXICO",
            "MICHOACAN",
            "MICHOACÁN",
            "MORELOS",
            "NAYARIT",
            "NUEVO LEON",
            "NUEVO LEÓN",
            "OAXACA",
            "PUEBLA",
            "QUERETARO",
            "QUERÉTARO",
            "QUINTANA ROO",
            "SAN LUIS POTOSI",
            "SAN LUIS POTOSÍ",
            "SINALOA",
            "SONORA",
            "TABASCO",
            "TAMAULIPAS",
            "TLAXCALA",
            "VERACRUZ",
            "YUCATAN",
            "YUCATÁN",
            "ZACATECAS",
        ]
    
        m = re.search(
            r"ENTIDAD\s+DE\s+REGISTRO\s+([A-ZÁÉÍÓÚÑ ]{3,45})",
            text,
            flags=re.IGNORECASE,
        )
    
        if m:
            chunk = m.group(1).upper()
            for estado in estados:
                if estado in chunk:
                    estado_norm = (
                        estado.replace("Á", "A")
                        .replace("É", "E")
                        .replace("Í", "I")
                        .replace("Ó", "O")
                        .replace("Ú", "U")
                        .replace("Ñ", "N")
                        .replace(" ", "_")
                    )
                    print("PROVIDER4_REAR_STATE_FROM_PDF =", estado_norm, flush=True)
                    return estado_norm
    
        estado_curp = self._estado_desde_curp(curp)
        print("PROVIDER4_REAR_STATE_FROM_CURP_FALLBACK =", estado_curp, flush=True)
        return estado_curp

    def _estado_desde_pdf_o_cadena(self, pdf_bytes: bytes, cadena: str) -> str:
        """
        Para reverso local en modo cadena, preferimos la Entidad de Registro visible en el PDF.
        Si no se puede detectar, caemos al estado derivado de la cadena.
        """
        try:
            text = self._extract_pdf_visible_text(pdf_bytes)
            text = re.sub(r"\s+", " ", (text or "").upper()).strip()
    
            estados = [
                "AGUASCALIENTES",
                "BAJA CALIFORNIA SUR",
                "BAJA CALIFORNIA",
                "CAMPECHE",
                "COAHUILA",
                "COLIMA",
                "CHIAPAS",
                "CHIHUAHUA",
                "CIUDAD DE MEXICO",
                "CIUDAD DE MÉXICO",
                "DURANGO",
                "GUANAJUATO",
                "GUERRERO",
                "HIDALGO",
                "JALISCO",
                "MEXICO",
                "MÉXICO",
                "MICHOACAN",
                "MICHOACÁN",
                "MORELOS",
                "NAYARIT",
                "NUEVO LEON",
                "NUEVO LEÓN",
                "OAXACA",
                "PUEBLA",
                "QUERETARO",
                "QUERÉTARO",
                "QUINTANA ROO",
                "SAN LUIS POTOSI",
                "SAN LUIS POTOSÍ",
                "SINALOA",
                "SONORA",
                "TABASCO",
                "TAMAULIPAS",
                "TLAXCALA",
                "VERACRUZ",
                "YUCATAN",
                "YUCATÁN",
                "ZACATECAS",
            ]
    
            m = re.search(
                r"ENTIDAD\s+DE\s+REGISTRO\s+([A-ZÁÉÍÓÚÑ ]{3,45})",
                text,
                flags=re.IGNORECASE,
            )
    
            if m:
                chunk = m.group(1).upper()
                for estado in estados:
                    if estado in chunk:
                        estado_norm = (
                            estado.replace("Á", "A")
                            .replace("É", "E")
                            .replace("Í", "I")
                            .replace("Ó", "O")
                            .replace("Ú", "U")
                            .replace("Ñ", "N")
                            .replace(" ", "_")
                        )
                        print("PROVIDER4_CHAIN_REAR_STATE_FROM_PDF =", estado_norm, flush=True)
                        return estado_norm
    
        except Exception as e:
            print("PROVIDER4_CHAIN_REAR_STATE_FROM_PDF_FAILED =", str(e), flush=True)
    
        estado_cadena = _estado_desde_cadena(cadena)
    
        if not estado_cadena or estado_cadena == "DESCONOCIDO":
            raise RuntimeError(f"PROVIDER4_CHAIN_CANNOT_REPAIR_NO_STATE:{cadena}")
    
        print("PROVIDER4_CHAIN_REAR_STATE_FROM_CHAIN_FALLBACK =", estado_cadena, flush=True)
        return estado_cadena
    
    def _pdf_rear_matches_estado(self, pdf_bytes: bytes, estado: str) -> bool:
        """
        Valida si la página 2 corresponde al estado esperado.
        Evita aceptar reversos con contenido pero de otro estado.
        """
        try:
            reader = PdfReader(BytesIO(pdf_bytes))
    
            if len(reader.pages) < 2:
                print("PROVIDER4_REAR_STATE_MATCH_NO_PAGE_2 = TRUE", flush=True)
                return False
    
            try:
                text = reader.pages[1].extract_text() or ""
            except Exception:
                text = ""
    
            text = re.sub(r"\s+", " ", text.upper()).strip()
    
            expected = (estado or "").upper().replace("_", " ").strip()
    
            aliases = {
                "MEXICO": ["MEXICO", "MÉXICO", "ESTADO DE MEXICO", "ESTADO DE MÉXICO"],
                "CIUDAD DE MEXICO": ["CIUDAD DE MEXICO", "CIUDAD DE MÉXICO", "CDMX"],
                "NUEVO LEON": ["NUEVO LEON", "NUEVO LEÓN"],
                "SAN LUIS POTOSI": ["SAN LUIS POTOSI", "SAN LUIS POTOSÍ"],
                "QUERETARO": ["QUERETARO", "QUERÉTARO"],
                "MICHOACAN": ["MICHOACAN", "MICHOACÁN"],
                "YUCATAN": ["YUCATAN", "YUCATÁN"],
                "QUINTANA ROO": ["QUINTANA ROO"],
                "CHIAPAS": ["CHIAPAS"],
            }
    
            candidates = aliases.get(expected, [expected])
            match = any(c in text for c in candidates)
    
            print(
                "PROVIDER4_REAR_STATE_MATCH =",
                {
                    "expected": expected,
                    "candidates": candidates,
                    "rear_text_preview": text[:120],
                    "match": bool(match),
                },
                flush=True,
            )
    
            return bool(match)
    
        except Exception as e:
            print("PROVIDER4_REAR_STATE_MATCH_FAILED =", str(e), flush=True)
            return False

    def _repair_pdf_if_needed(self, pdf_bytes: bytes, term: str, inc_folio: bool) -> bytes:
        original_pdf_bytes = pdf_bytes
        original_pages = self._pdf_num_pages(original_pdf_bytes)
    
        front_has_frame = False
        rear_has_content = False
    
        try:
            front_has_frame = _pdf_front_has_green_frame(original_pdf_bytes)
        except Exception as e:
            print("PROVIDER4_FRONT_FRAME_CHECK_FAILED =", str(e), flush=True)
            front_has_frame = False
    
        if original_pages >= 2:
            try:
                rear_has_content = _pdf_page_has_visible_content(
                    original_pdf_bytes,
                    page_index=1,
                )
            except Exception as e:
                print("PROVIDER4_REAR_CONTENT_CHECK_FAILED =", str(e), flush=True)
                rear_has_content = False
    
        try:
            estado = self._estado_desde_pdf_o_curp(original_pdf_bytes, term)
        except Exception as e:
            print("PROVIDER4_STATE_FOR_REAR_CHECK_FAILED =", str(e), flush=True)
            estado = self._estado_desde_curp(term)
    
        rear_matches_estado = False
    
        if original_pages >= 2 and rear_has_content:
            rear_matches_estado = self._pdf_rear_matches_estado(
                original_pdf_bytes,
                estado,
            )
    
        print(
            "PROVIDER4_ORIGINAL_PDF_CHECK =",
            {
                "pages": original_pages,
                "front_has_frame": front_has_frame,
                "rear_has_content": rear_has_content,
                "estado": estado,
                "rear_matches_estado": rear_matches_estado,
            },
            flush=True,
        )
    
        # Si History ya entregó PDF completo Y el reverso corresponde al estado,
        # se respeta completo.
        if original_pages >= 2 and front_has_frame and rear_has_content and rear_matches_estado:
            print("PROVIDER4_ORIGINAL_PDF_COMPLETE_KEEP_AS_IS = TRUE", flush=True)
            return original_pdf_bytes
    
        if original_pages >= 2:
            print("PROVIDER4_PDF_NEEDS_REPAIR_DESPITE_2_PAGES = TRUE", flush=True)
        else:
            print("PROVIDER4_PDF_ONE_PAGE_OR_INCOMPLETE = TRUE", flush=True)
            print("PROVIDER4_PDF_INCOMPLETE_REPAIRING = TRUE", flush=True)
    
        base_dir = Path(__file__).resolve().parent.parent
        estados_dir = base_dir / "assets" / "estados"
    
        # Frente:
        # Si ya trae marco, _enmarcar_pdf_frente devuelve solo página 1 original.
        # Si no trae marco, aplica marco local.
        try:
            raw_front = _solo_pagina_pdf(original_pdf_bytes, 0)
            raw_front = self._normalize_pdf_pages_to_letter(
                raw_front,
                term=term,
                label="PROVIDER4_FRONT_BEFORE_FRAME",
            )

            front_is_letter = self._pdf_first_page_is_letter(raw_front, term=term)

            if front_has_frame:
                # Si Lázaro ya entregó frente con marco verde,
                # NO volver a enmarcar.
                framed_front = raw_front
                print("PROVIDER4_FRONT_ALREADY_FRAMED_KEEP_RAW_PAGE_LETTER = TRUE", flush=True)

            elif front_is_letter:
                # Lázaro/Provider10 puede devolver frente Letter 612x792
                # sin marco verde. NO lo dejamos crudo porque saldría sin marco.
                # Lo enmarcamos casi entero con scale 0.96 para no achicarlo.
                framed_front = self._enmarcar_pdf_frente_lazaro_letter_casi_entero(
                    raw_front,
                    f"{term}.pdf",
                    folio=inc_folio,
                )

                framed_front = self._normalize_pdf_pages_to_letter(
                    framed_front,
                    term=term,
                    label="PROVIDER4_FRONT_AFTER_LETTER_FRAME",
                )

            else:
                # Solo si NO viene Letter, usamos el marco suave.
                # Esto evita deformar o reducir actas que ya vienen correctas.
                framed_front = self._enmarcar_pdf_frente_lazaro_suave(
                    raw_front,
                    f"{term}.pdf",
                    folio=inc_folio,
                )

                framed_front = self._normalize_pdf_pages_to_letter(
                    framed_front,
                    term=term,
                    label="PROVIDER4_FRONT_AFTER_FRAME",
                )
        except Exception as e:
            print("LOCAL_FRAME_FAILED_NO_SEND =", str(e), flush=True)
            raise RuntimeError(f"LOCAL_FRAME_FAILED:{term}:{str(e)[:300]}")
    
        # Reverso:
        # Solo se respeta el reverso de Provider4 si tiene contenido Y coincide con la entidad del acta.
        use_provider4_rear = (
            original_pages >= 2
            and rear_has_content
            and rear_matches_estado
        )
    
        if use_provider4_rear:
            print("PROVIDER4_USING_ORIGINAL_REAR_PAGE = TRUE", flush=True)
    
            try:
                original_rear = _solo_pagina_pdf(original_pdf_bytes, 1)
                original_rear = self._normalize_pdf_pages_to_letter(
                    original_rear,
                    term=term,
                    label="PROVIDER4_ORIGINAL_REAR",
                )
                repaired_pdf = _unir_pdfs_bytes_raw(framed_front, original_rear)
            except Exception as e:
                print("PROVIDER4_ORIGINAL_REAR_JOIN_FAILED =", str(e), flush=True)
                raise RuntimeError(f"PROVIDER4_ORIGINAL_REAR_JOIN_FAILED:{term}")
    
        else:
            print("PROVIDER4_USING_LOCAL_REAR_PAGE = TRUE", flush=True)
    
            try:
                if estado == "NACIDO_EN_EL_EXTRANJERO":
                    print("PROVIDER4_FOREIGN_FORCE_MEXICO_BACK =", term, flush=True)
                    reverso_path = _resolver_reverso_por_estado("MEXICO", estados_dir)
                else:
                    reverso_path = _resolver_reverso_por_estado(estado, estados_dir)
    
                repaired_pdf = _unir_pdfs_bytes(framed_front, reverso_path)
    
            except Exception as e:
                print("PROVIDER4_LOCAL_REAR_JOIN_FAILED =", str(e), flush=True)
                raise RuntimeError(f"PROVIDER4_LOCAL_REAR_JOIN_FAILED:{term}")
    
        if not self._pdf_has_two_pages(repaired_pdf):
            print("PROVIDER4_REPAIRED_STILL_INCOMPLETE_NO_SEND =", term, flush=True)
            raise RuntimeError(f"PROVIDER4_REPAIRED_STILL_INCOMPLETE:{term}")
    
        print(f"PROVIDER4_PDF_PAGE_COUNT = {self._pdf_num_pages(repaired_pdf)}", flush=True)
        return repaired_pdf

    def _repair_chain_pdf_if_needed(self, pdf_bytes: bytes, term: str) -> bytes:
        original_pdf_bytes = pdf_bytes
        original_pages = self._pdf_num_pages(original_pdf_bytes)
    
        front_has_frame = False
        rear_has_content = False
    
        try:
            front_has_frame = _pdf_front_has_green_frame(original_pdf_bytes)
        except Exception as e:
            print("PROVIDER4_CHAIN_FRONT_FRAME_CHECK_FAILED =", str(e), flush=True)
            front_has_frame = False
    
        if original_pages >= 2:
            try:
                rear_has_content = _pdf_page_has_visible_content(
                    original_pdf_bytes,
                    page_index=1,
                )
            except Exception as e:
                print("PROVIDER4_CHAIN_REAR_CONTENT_CHECK_FAILED =", str(e), flush=True)
                rear_has_content = False
    
        try:
            estado = self._estado_desde_pdf_o_cadena(original_pdf_bytes, term)
        except Exception as e:
            print("PROVIDER4_CHAIN_STATE_ERROR =", str(e), flush=True)
            raise RuntimeError(f"PROVIDER4_CHAIN_CANNOT_REPAIR_NO_STATE:{term}")
    
        if not estado or estado == "DESCONOCIDO":
            print("PROVIDER4_CHAIN_STATE_UNKNOWN =", term, flush=True)
            raise RuntimeError(f"PROVIDER4_CHAIN_CANNOT_REPAIR_NO_STATE:{term}")
    
        rear_matches_estado = False
    
        if original_pages >= 2 and rear_has_content:
            rear_matches_estado = self._pdf_rear_matches_estado(
                original_pdf_bytes,
                estado,
            )
    
        print(
            "PROVIDER4_CHAIN_ORIGINAL_PDF_CHECK =",
            {
                "pages": original_pages,
                "front_has_frame": front_has_frame,
                "rear_has_content": rear_has_content,
                "estado": estado,
                "rear_matches_estado": rear_matches_estado,
            },
            flush=True,
        )
    
        # Si History/Provider4 ya entregó cadena completa y reverso correcto,
        # se respeta completo.
        if original_pages >= 2 and front_has_frame and rear_has_content and rear_matches_estado:
            print("PROVIDER4_CHAIN_ORIGINAL_PDF_COMPLETE_KEEP_AS_IS = TRUE", flush=True)
            return original_pdf_bytes
    
        if original_pages >= 2:
            print("PROVIDER4_CHAIN_PDF_NEEDS_REPAIR_DESPITE_2_PAGES = TRUE", flush=True)
        else:
            print("PROVIDER4_CHAIN_PDF_ONE_PAGE_OR_INCOMPLETE = TRUE", flush=True)
    
        base_dir = Path(__file__).resolve().parent.parent
        estados_dir = base_dir / "assets" / "estados"
    
        # Frente:
        # Si ya trae marco, _enmarcar_pdf_frente devuelve solo página 1 original.
        # Si no trae marco, aplica marco local.
        try:
            raw_front = _solo_pagina_pdf(original_pdf_bytes, 0)
            raw_front = self._normalize_pdf_pages_to_letter(
                raw_front,
                term=term,
                label="PROVIDER4_CHAIN_FRONT_BEFORE_FRAME",
            )

            front_is_letter = self._pdf_first_page_is_letter(raw_front, term=term)

            if front_has_frame:
                framed_front = raw_front
                print("PROVIDER4_CHAIN_FRONT_ALREADY_FRAMED_KEEP_RAW_PAGE_LETTER = TRUE", flush=True)

            elif front_is_letter:
                # Cadena también puede venir Letter sin marco.
                # Se pone marco casi entero para no achicarla.
                framed_front = self._enmarcar_pdf_frente_lazaro_letter_casi_entero(
                    raw_front,
                    f"{term}.pdf",
                    folio=False,
                )

                framed_front = self._normalize_pdf_pages_to_letter(
                    framed_front,
                    term=term,
                    label="PROVIDER4_CHAIN_FRONT_AFTER_LETTER_FRAME",
                )

            else:
                framed_front = self._enmarcar_pdf_frente_lazaro_suave(
                    raw_front,
                    f"{term}.pdf",
                    folio=False,
                )

                framed_front = self._normalize_pdf_pages_to_letter(
                    framed_front,
                    term=term,
                    label="PROVIDER4_CHAIN_FRONT_AFTER_FRAME",
                )
        except Exception as e:
            print("PROVIDER4_CHAIN_LOCAL_FRAME_FAILED_NO_SEND =", str(e), flush=True)
            raise RuntimeError(f"PROVIDER4_CHAIN_LOCAL_FRAME_FAILED:{term}:{str(e)[:300]}")
    
        # Reverso:
        # Solo respetar reverso Provider4 si tiene contenido Y coincide con la entidad.
        use_provider4_rear = (
            original_pages >= 2
            and rear_has_content
            and rear_matches_estado
        )
    
        if use_provider4_rear:
            print("PROVIDER4_CHAIN_USING_ORIGINAL_REAR_PAGE = TRUE", flush=True)
    
            try:
                original_rear = _solo_pagina_pdf(original_pdf_bytes, 1)
                original_rear = self._normalize_pdf_pages_to_letter(
                    original_rear,
                    term=term,
                    label="PROVIDER4_CHAIN_ORIGINAL_REAR",
                )
                repaired_pdf = _unir_pdfs_bytes_raw(framed_front, original_rear)
            except Exception as e:
                print("PROVIDER4_CHAIN_ORIGINAL_REAR_JOIN_FAILED =", str(e), flush=True)
                raise RuntimeError(f"PROVIDER4_CHAIN_ORIGINAL_REAR_JOIN_FAILED:{term}")
    
        else:
            print("PROVIDER4_CHAIN_USING_LOCAL_REAR_PAGE = TRUE", flush=True)
    
            try:
                if estado == "NACIDO_EN_EL_EXTRANJERO":
                    print("PROVIDER4_CHAIN_FOREIGN_FORCE_MEXICO_BACK =", term, flush=True)
                    reverso_path = _resolver_reverso_por_estado("MEXICO", estados_dir)
                else:
                    reverso_path = _resolver_reverso_por_estado(estado, estados_dir)
    
                repaired_pdf = _unir_pdfs_bytes(framed_front, reverso_path)
    
            except Exception as e:
                print("PROVIDER4_CHAIN_LOCAL_REAR_JOIN_FAILED =", str(e), flush=True)
                raise RuntimeError(f"PROVIDER4_CHAIN_LOCAL_REAR_JOIN_FAILED:{term}")
    
        if not self._pdf_has_two_pages(repaired_pdf):
            print("PROVIDER4_CHAIN_REPAIRED_STILL_INCOMPLETE =", term, flush=True)
            raise RuntimeError(f"PROVIDER4_CHAIN_REPAIRED_STILL_INCOMPLETE:{term}")
    
        print("PROVIDER4_CHAIN_REPAIRED_ESTADO =", estado, flush=True)
        print(f"PROVIDER4_CHAIN_REPAIRED_PAGE_COUNT = {self._pdf_num_pages(repaired_pdf)}", flush=True)
    
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
        tipo_norm = (tipoa or "nacimiento").strip().lower()
    
        if inc_folio:
            return self.consultar_por_curp_folio_vgetofi(
                curp=curp,
                tipoa=tipoa,
            )

        data = {
            "tipoActa": tipo_norm,
            "tipoa": tipo_norm,
            "curpID": curp or "",
            "curp": curp or "",
            "cadena": cadena or "",
            "cadenaA": cadena or "",
            "p1": "RDBjdUV4cHJS",
            "p2": "",
            "p3": "NDA=",
            "p5": "",
            "p6": "",
            "p7": self.HID,
            "p4": self.HID,
            "hidU": self.HID,
        }
    
        if trami_ine:
            data["tramiteINE"] = "on"
            data["tramiINE"] = "true"
    
        last_error = None
    
        for attempt in range(3):
            try:
                self.warm()

                print("PROVIDER4_REAL_SUBMIT_TO_LAZARO =", {
                    "curp": curp,
                    "tipoa_received": tipoa,
                    "tipo_norm_sent": tipo_norm,
                    "inc_folio": inc_folio,
                    "cadena": cadena,
                    "hid": self.HID,
                }, flush=True)
    
                print("PROVIDER4_REQUEST_DATA =", data, flush=True)
                print(f"PROVIDER4_VGETOFI2_ATTEMPT_{attempt+1}_START", flush=True)
    
                resp = self.session.post(
                    self.VGET_URL,
                    data=data,
                    timeout=(15, 60),
                    headers={
                        "User-Agent": self.session.headers["User-Agent"],
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "es-ES,es;q=0.9",
                        "Referer": self.MANUAL_PAGE_URL,
                        "Origin": self.BASE_URL,
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
    
                resp.raise_for_status()
    
                html = resp.text or ""

                print(
                    f"PROVIDER4_VGETOFI2_ATTEMPT_{attempt+1}_STATUS = {resp.status_code}",
                    flush=True,
                )
                print("PROVIDER4_VGETOFI2_HTML_LEN =", len(html), flush=True)
                print("PROVIDER4_VGETOFI2_HTML_PREVIEW =", html[:1200], flush=True)
                
                html_clean = html.strip()
                html_up = html_clean.upper()
                
                looks_useful = (
                    "ENVIADO" in html_up
                    or "NO_LOCALIZADO" in html_up
                    or "TRAMITEEXISTENTE" in html_up
                    or "TRÁMITEEXISTENTE" in html_up
                    or "<FORM" in html_up
                    or "ACTION" in html_up
                )
                
                if not html_clean or not looks_useful:
                    print(
                        f"PROVIDER4_VGETOFI2_EMPTY_OR_USELESS_HTML_ATTEMPT_{attempt+1} = "
                        f"len={len(html_clean)} useful={looks_useful} preview={repr(html_clean[:120])}",
                        flush=True,
                    )
                
                    last_error = RuntimeError("PROVIDER4_EMPTY_OR_USELESS_HTML")
                
                    if attempt < 2:
                        time.sleep(10 + attempt * 8)
                        continue
                
                    raise RuntimeError("PROVIDER4_EMPTY_OR_USELESS_HTML")
                
                return html
    
            except requests.exceptions.RequestException as e:
                last_error = e
                print(
                    f"PROVIDER4_VGETOFI2_ATTEMPT_{attempt+1}_ERROR = {str(e)}",
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
        html = html or ""
    
        form_tag_match = re.search(
            r"<form\b[^>]*>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
    
        if not form_tag_match:
            print("PROVIDER4_NO_FORM_TAG_HTML_PREVIEW =", html[:1500], flush=True)
            raise RuntimeError("PROVIDER4_NO_FORM_ACTION")
    
        form_tag = form_tag_match.group(0)
    
        action_match = re.search(
            r"""\baction\s*=\s*(['"])(.*?)\1""",
            form_tag,
            flags=re.IGNORECASE | re.DOTALL,
        )
    
        if not action_match:
            action_match = re.search(
                r"""\baction\s*=\s*([^\s>]+)""",
                form_tag,
                flags=re.IGNORECASE | re.DOTALL,
            )
    
        if not action_match:
            print("PROVIDER4_FORM_TAG_WITHOUT_ACTION =", form_tag[:1000], flush=True)
            print("PROVIDER4_NO_FORM_ACTION_HTML_PREVIEW =", html[:1500], flush=True)
            raise RuntimeError("PROVIDER4_NO_FORM_ACTION")
    
        action = action_match.group(2 if action_match.lastindex and action_match.lastindex >= 2 else 1).strip()
        action_url = urljoin(f"{self.BASE_URL}/servicio/", action)
    
        inputs = {}
    
        input_tags = re.findall(
            r"<input\b[^>]*>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
    
        for input_tag in input_tags:
            name_match = re.search(
                r"""\bname\s*=\s*(['"])(.*?)\1""",
                input_tag,
                flags=re.IGNORECASE | re.DOTALL,
            )
    
            if not name_match:
                name_match = re.search(
                    r"""\bname\s*=\s*([^\s>]+)""",
                    input_tag,
                    flags=re.IGNORECASE | re.DOTALL,
                )
    
            if not name_match:
                continue
    
            name = name_match.group(2 if name_match.lastindex and name_match.lastindex >= 2 else 1)
    
            value_match = re.search(
                r"""\bvalue\s*=\s*(['"])(.*?)\1""",
                input_tag,
                flags=re.IGNORECASE | re.DOTALL,
            )
    
            if not value_match:
                value_match = re.search(
                    r"""\bvalue\s*=\s*([^\s>]*)""",
                    input_tag,
                    flags=re.IGNORECASE | re.DOTALL,
                )
    
            value = ""
            if value_match:
                value = value_match.group(2 if value_match.lastindex and value_match.lastindex >= 2 else 1)
    
            inputs[unescape(name)] = unescape(value)
    
        if not inputs:
            print("PROVIDER4_FORM_ACTION_BUT_NO_INPUTS =", action_url, flush=True)
            print("PROVIDER4_FORM_INPUTS_HTML_PREVIEW =", html[:1500], flush=True)
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
                    timeout=(15, 60),
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
                    timeout=(15, 60),
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
                    timeout=(15, 60),
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

    def _strip_accents(self, value: str) -> str:
        value = value or ""
        value = unicodedata.normalize("NFD", value)
        value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
        return value.upper()

    def _expected_tipo_code(self, tipoa: str | None) -> str:
        tipo = (tipoa or "").strip().lower()
        return {
            "nacimiento": "NAC",
            "matrimonio": "MAT",
            "defuncion": "DEF",
            "defunción": "DEF",
            "divorcio": "DIV",
        }.get(tipo, "")

    def _expected_tipo_aliases(self, tipoa: str | None) -> list[str]:
        tipo = self._strip_accents(tipoa or "").strip().lower()

        aliases = {
            "nacimiento": ["NACIMIENTO", "_NAC", " NAC ", "NAC_"],
            "matrimonio": ["MATRIMONIO", "_MAT", " MAT ", "MAT_"],
            "defuncion": ["DEFUNCION", "DEFUNCIÓN", "_DEF", " DEF ", "DEF_"],
            "divorcio": ["DIVORCIO", "_DIV", " DIV ", "DIV_"],
        }

        return aliases.get(tipo, [])

    def _row_matches_expected_tipo(self, row_html: str, row_text: str, tipoa: str | None) -> bool:
        aliases = self._expected_tipo_aliases(tipoa)

        if not aliases:
            return True

        haystack = self._strip_accents((row_text or "") + " " + (row_html or ""))

        return any(self._strip_accents(alias) in haystack for alias in aliases)

    def _link_matches_expected_tipo(self, link: str, tipoa: str | None) -> bool:
        code = self._expected_tipo_code(tipoa)

        if not code:
            return True

        link_up = self._strip_accents(unescape(link or ""))

        # Lázaro normalmente usa:
        # d.php?f=CURP_NAC
        # d.php?f=CURP_MAT
        # d.php?f=CURP_DEF
        # d.php?f=CURP_DIV
        # o variantes foliadas.
        return (
            f"_{code}" in link_up
            or f"{code}_FOLIO" in link_up
            or f"TIPO={code}" in link_up
            or f"TIPOA={code}" in link_up
        )
    
    def _history_row_for_term(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        term_up = (term or "").strip().upper()

        rows = re.findall(
            r"<tr\b[^>]*>.*?</tr>",
            history_html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )

        for row_html in rows:
            row_text = unescape(re.sub(r"<[^>]+>", " ", row_html))
            row_text = re.sub(r"\s+", " ", row_text).strip().upper()

            row_text_norm = self._strip_accents(row_text)

            if term_up not in row_text_norm:
                continue

            # IMPORTANTÍSIMO:
            # Para MAT/DEF/DIV no basta con que aparezca la CURP.
            # Debe coincidir también el tipo, ya sea por texto visible o por sufijo del link.
            if not self._row_matches_expected_tipo(row_html, row_text, tipoa):
                print("PROVIDER4_HISTORY_ROW_SKIPPED_WRONG_TIPOA =", {
                    "term": term_up,
                    "tipoa": tipoa,
                    "row_text": row_text[:500],
                }, flush=True)
                continue

            print("PROVIDER4_HISTORY_ROW_MATCHED_TERM =", term_up, flush=True)
            print("PROVIDER4_HISTORY_ROW_MATCHED_TIPOA =", tipoa, flush=True)
            print("PROVIDER4_HISTORY_ROW_TEXT =", row_text[:500], flush=True)
            return row_html

        return None

    def _history_folio_row_for_term(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        term_up = (term or "").strip().upper()

        rows = re.findall(
            r"<tr\b[^>]*>.*?</tr>",
            history_html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )

        for row_html in rows:
            row_text = unescape(re.sub(r"<[^>]+>", " ", row_html))
            row_text = re.sub(r"\s+", " ", row_text).strip().upper()
            row_up = row_html.upper()

            row_text_norm = self._strip_accents(row_text)

            if term_up not in row_text_norm:
                continue

            if not self._row_matches_expected_tipo(row_html, row_text, tipoa):
                print("PROVIDER4_HISTORY_FOLIO_ROW_SKIPPED_WRONG_TIPOA =", {
                    "term": term_up,
                    "tipoa": tipoa,
                    "row_text": row_text[:700],
                }, flush=True)
                continue

            if "DESCARGAR FOLIADO" not in row_text and "ADDFOL.PHP" not in row_up:
                continue

            print("PROVIDER4_HISTORY_FOLIO_ROW_MATCHED_TERM =", term_up, flush=True)
            print("PROVIDER4_HISTORY_FOLIO_ROW_MATCHED_TIPOA =", tipoa, flush=True)
            print("PROVIDER4_HISTORY_FOLIO_ROW_TEXT =", row_text[:700], flush=True)
            return row_html

        print("PROVIDER4_HISTORY_FOLIO_ROW_NOT_FOUND =", term_up, flush=True)
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

    def _detect_no_result_loose(self, history_html: str, term: str, tipoa: str | None = None) -> bool:
        term_up = (term or "").strip().upper()
    
        if not history_html or not term_up:
            return False
    
        rows = re.findall(
            r"<tr\b[^>]*>.*?</tr>",
            history_html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )
    
        for row_html in rows:
            row_text = unescape(re.sub(r"<[^>]+>", " ", row_html))
            row_text = re.sub(r"\s+", " ", row_text).strip().upper()
            row_text_norm = self._strip_accents(row_text)
    
            if term_up not in row_text_norm:
                continue
    
            if "NO_LOCALIZADO" not in row_text_norm:
                continue
    
            # MUY IMPORTANTE:
            # Si se está buscando por tipo específico, no aceptar NO_LOCALIZADO
            # de otra fila/tipo. Esto evita que NACIMIENTO afecte MATRIMONIO.
            if tipoa:
                if not self._row_matches_expected_tipo(row_html, row_text, tipoa):
                    print("PROVIDER4_NO_RECORD_LOOSE_SKIPPED_WRONG_TIPOA =", {
                        "term": term_up,
                        "tipoa": tipoa,
                        "row_text": row_text[:500],
                    }, flush=True)
                    continue
    
            print("PROVIDER4_NO_RECORD_DETECTED_LOOSE =", term_up, flush=True)
            print("PROVIDER4_NO_RECORD_LOOSE_TIPOA =", tipoa, flush=True)
            print("PROVIDER4_NO_RECORD_LOOSE_ROW_TEXT =", row_text[:500], flush=True)
            return True
    
        return False
    
    def _extract_pdf_link(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        html = history_html or ""
        term_up = (term or "").strip().upper()

        # 1) Buscar en fila exacta por CURP + tipo.
        row_html = self._history_row_for_term(html, term, tipoa)

        if row_html:
            matches = re.findall(
                r"""href\s*=\s*["']([^"']*d\.php\?f=[^"']+)["']""",
                row_html,
                flags=re.IGNORECASE,
            )

            for raw_link in matches:
                link = urljoin(f"{self.BASE_URL}/servicio/", unescape(raw_link))

                if not self._link_matches_expected_tipo(link, tipoa):
                    print("PROVIDER4_ROW_LINK_SKIPPED_WRONG_TIPOA =", {
                        "term": term_up,
                        "tipoa": tipoa,
                        "link": link,
                    }, flush=True)
                    continue

                print("PROVIDER4_EXTRACTED_PDF_LINK =", link, flush=True)
                return link

            print("PROVIDER4_ROW_FOUND_BUT_VALID_DPHP_NOT_FOUND =", {
                "term": term_up,
                "tipoa": tipoa,
            }, flush=True)

        # 2) Fallback global, pero ahora AMARRADO al tipo.
        # Antes aquí estaba el bug: agarraba cualquier d.php?f=*CURP* aunque fuera NAC.
        if term_up:
            all_links = re.findall(
                r"""href\s*=\s*["']([^"']*d\.php\?f=[^"']*"""
                + re.escape(term_up)
                + r"""[^"']*)["']""",
                html,
                flags=re.IGNORECASE,
            )

            for raw_link in all_links:
                link = urljoin(f"{self.BASE_URL}/servicio/", unescape(raw_link))

                if not self._link_matches_expected_tipo(link, tipoa):
                    print("PROVIDER4_HISTORY_PDF_REGEX_SKIPPED_WRONG_TIPOA =", {
                        "term": term_up,
                        "tipoa": tipoa,
                        "link": link,
                    }, flush=True)
                    continue

                print("PROVIDER4_HISTORY_PDF_FOUND_REGEX =", link, flush=True)
                return link

        print("PROVIDER4_EXTRACT_PDF_LINK_NOT_FOUND =", {
            "term": term_up,
            "tipoa": tipoa,
        }, flush=True)

        return None

    def _extract_pdf_link_from_folio_row(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        term_up = (term or "").strip().upper()
    
        row_html = self._history_folio_row_for_term(history_html, term, tipoa)
    
        if not row_html:
            print("PROVIDER4_FOLIO_ROW_FOR_DPHP_NOT_FOUND =", term_up, flush=True)
            return None
    
        m = re.search(
            r"""href\s*=\s*["']([^"']*d\.php\?f=[^"']+)["']""",
            row_html,
            flags=re.IGNORECASE,
        )
    
        if m:
            link = urljoin(f"{self.BASE_URL}/servicio/", unescape(m.group(1)))
            print("PROVIDER4_EXTRACTED_DPHP_FROM_FOLIO_ROW =", link, flush=True)
            return link
    
        print("PROVIDER4_FOLIO_ROW_FOUND_BUT_DPHP_NOT_FOUND =", term_up, flush=True)
        return None

    def _find_pdf_link_in_any_lazaro_history(
        self,
        term: str,
        tipoa: str | None = None,
    ) -> str | None:
        current_hid = self.HID
    
        hids = [
            "D0cuExprR",
            "D0cuExprRServ2",
            "D0cuExprRServ3",
        ]
    
        # Primero probar el HID actual, luego los demás.
        ordered_hids = [current_hid] + [h for h in hids if h != current_hid]
    
        for hid in ordered_hids:
            try:
                temp_client = self if hid == current_hid else Provider4Client(hid=hid)
    
                print("PROVIDER4_CROSS_HISTORY_CHECK_HID =", hid, flush=True)
    
                history_html = temp_client.get_history_html()
    
                link = temp_client._extract_pdf_link(history_html, term, tipoa)
    
                if link:
                    print("PROVIDER4_CROSS_HISTORY_LINK_FOUND_HID =", hid, flush=True)
                    print("PROVIDER4_CROSS_HISTORY_LINK_FOUND =", link, flush=True)
                    return link
    
            except Exception as e:
                print(
                    "PROVIDER4_CROSS_HISTORY_CHECK_ERROR =",
                    hid,
                    str(e),
                    flush=True,
                )
    
        print("PROVIDER4_CROSS_HISTORY_LINK_NOT_FOUND =", term, flush=True)
        return None

    def _extract_folio_link(self, history_html: str, term: str, tipoa: str | None = None) -> str | None:
        row_html = self._history_folio_row_for_term(history_html, term, tipoa)
        if not row_html:
            return None
    
        print("PROVIDER4_FOLIO_ROW_HTML_PREVIEW =", row_html[:1200], flush=True)
    
        # Para foliada queremos SOLO el link addFol.php.
        # d.php lo maneja _extract_pdf_link().
        m = re.search(
            r'href="(\./ActasN/addFol\.php\?[^"]+)"',
            row_html,
            flags=re.IGNORECASE,
        )
        if m:
            link = urljoin(f"{self.BASE_URL}/servicio/", m.group(1))
            print("PROVIDER4_EXTRACTED_ADD_FOL_LINK =", link, flush=True)
            return link
    
        print("PROVIDER4_ADD_FOL_LINK_NOT_FOUND =", term, flush=True)
        return None

    def download_pdf_bytes(self, url: str) -> bytes:
        last_error = None

        for attempt in range(3):
            try:
                print(f"PROVIDER4_DOWNLOAD_ATTEMPT_{attempt+1}_URL = {url}", flush=True)

                resp = self.session.get(
                    url,
                    timeout=(15, 60),
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
                    timeout=(15, 60),
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
                    timeout=(15, 60),
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

    def _new_tipo_num(self, tipoa: str) -> int:
        tipo = (tipoa or "").strip().lower()

        if tipo == "nacimiento":
            return 1
        if tipo == "defuncion":
            return 2
        if tipo == "matrimonio":
            return 3
        if tipo == "divorcio":
            return 4

        raise RuntimeError(f"PROVIDER4_NEW_UNKNOWN_TIPO:{tipoa}")


    def submit_peticion_new_api(
        self,
        *,
        curp: str,
        tipoa: str,
        inc_folio: bool = False,
        user: str = "",
        is_chain: bool = False,
    ) -> dict:
        """
        Nuevo flujo Provider4:
        Solo ingresa la solicitud.
        No consulta historial.
        No descarga PDF.
        """
        curp_clean = (curp or "").strip().upper()
        identifier_kind = "CADENA" if is_chain else "CURP"
        tipo_num = self._new_tipo_num(tipoa)
        foliado_num = 1 if inc_folio else 0

        params = {
            "curp": curp_clean,
            "tipo": str(tipo_num),
            "foliado": str(foliado_num),
            "HID": self.HID,
        }

        if user:
            params["User"] = user

        print("PROVIDER4_NEW_PETICION_URL =", self.NEW_PETICION_URL, flush=True)
        print("PROVIDER4_NEW_PETICION_PARAMS =", {
            **params,
            "_identifier_kind": identifier_kind,
        }, flush=True)

        r = self.session.get(
            self.NEW_PETICION_URL,
            params=params,
            timeout=(8, 30),
            headers={
                "User-Agent": self.session.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "*/*",
                "Referer": self.MANUAL_PAGE_URL,
            },
        )

        text = (r.text or "").strip()
        text_up = text.upper()

        print("PROVIDER4_NEW_PETICION_STATUS =", r.status_code, flush=True)
        print("PROVIDER4_NEW_PETICION_RESPONSE =", text[:1000], flush=True)

        r.raise_for_status()

        if text_up.startswith("EN_PROCESO_") or "EN_PROCESO_CURP18DIGITOS" in text_up:
            return {
                "ok": True,
                "submitted": True,
                "code": text_up,
                "raw": text,
            }

        if "PDF_EXISTENTE" in text_up:
            return {"ok": True, "submitted": True, "code": "PDF_EXISTENTE", "raw": text}

        if "ERROR_TRAMITEEXISTENTE" in text_up:
            return {"ok": True, "submitted": True, "code": "ERROR_TRAMITEEXISTENTE", "raw": text}

        if "CURP_INVALIDA" in text_up:
            error_code = (
                "PROVIDER4_CHAIN_INVALIDA"
                if is_chain
                else "PROVIDER4_CURP_INVALIDA"
            )
            raise RuntimeError(f"{error_code}:{curp_clean}")

        if "CUENTAINEXISTENTE" in text_up:
            raise RuntimeError("PROVIDER4_CUENTA_INEXISTENTE")

        raise RuntimeError(f"PROVIDER4_NEW_PETICION_UNKNOWN_RESPONSE:{text[:300]}")


    def verificar_pdf_new_api(
        self,
        *,
        curp: str,
        tipoa: str,
        is_chain: bool = False,
    ) -> dict:
        """
        Nuevo flujo Provider4:
        Consulta directa por CURP/tipo.
        Si todavía no está: ARCHIVO NO EXISTE.
        Si ya está: PDF directo.
        """
        curp_clean = (curp or "").strip().upper()
        identifier_kind = "CADENA" if is_chain else "CURP"
        tipo_num = self._new_tipo_num(tipoa)

        params = {
            "curp": curp_clean,
            "tipo": str(tipo_num),
        }

        print("PROVIDER4_NEW_VERIFICAR_URL =", self.NEW_VERIFICAR_PDF_URL, flush=True)
        print("PROVIDER4_NEW_VERIFICAR_PARAMS =", {
            **params,
            "_identifier_kind": identifier_kind,
        }, flush=True)

        r = self.session.get(
            self.NEW_VERIFICAR_PDF_URL,
            params=params,
            timeout=(8, 40),
            headers={
                "User-Agent": self.session.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/pdf,*/*",
                "Referer": self.MANUAL_PAGE_URL,
            },
        )

        content_type = (r.headers.get("Content-Type") or "").lower()
        content = r.content or b""

        try:
            text_preview = content.decode("utf-8", errors="ignore").strip()
        except Exception:
            text_preview = ""

        text_up = (text_preview or "").upper()

        print("PROVIDER4_NEW_VERIFICAR_STATUS =", r.status_code, flush=True)
        print("PROVIDER4_NEW_VERIFICAR_CONTENT_TYPE =", content_type, flush=True)
        print("PROVIDER4_NEW_VERIFICAR_LEN =", len(content), flush=True)
        print("PROVIDER4_NEW_VERIFICAR_TEXT_PREVIEW =", text_preview[:500], flush=True)

        r.raise_for_status()

        # PDF directo desde verificarpdf.php.
        # OJO: puede venir prematuro/sin marco/sin reverso.
        if "application/pdf" in content_type or content.startswith(b"%PDF"):
            self._assert_pdf_readable(content, curp_clean)
        
            return {
                "ready": True,
                "pdf_bytes": content,
                "code": "PDF_READY",
                "source": "verificarpdf",
            }

        not_ready_values = {"FALSE", "0", "NO", "NULL", "NONE", ""}

        if "ARCHIVO NO EXISTE" in text_up or text_up.strip().upper() in not_ready_values:
            return {
                "ready": False,
                "code": text_up.strip().upper() or "PDF_NOT_READY",
                "reason": "PDF_NOT_READY",
            }

        if "CURP_INVALIDA" in text_up:
            error_code = (
                "PROVIDER4_CHAIN_INVALIDA"
                if is_chain
                else "PROVIDER4_CURP_INVALIDA"
            )
            raise RuntimeError(f"{error_code}:{curp_clean}")

        # NO_LOCALIZADO_VERIFICAR_PDF_OK:
        # La API de verificarpdf.php puede responder NO_LOCALIZADO cuando el trámite ya quedó sin registro.
        # Esto NO es respuesta desconocida ni espera; el worker lo convertirá en "No hay registros disponibles".
        _txt_no_loc = (text_preview or "").strip().upper().replace(" ", "_")
        
        if (
            "NO_LOCALIZADO" in _txt_no_loc
            or "NO_REGISTRO" in _txt_no_loc
            or "SIN_REGISTRO" in _txt_no_loc
        ):
            return {
                "ready": False,
                "code": "NO_LOCALIZADO",
                "reason": "NO_LOCALIZADO",
                "raw": text_preview,
            }

        raise RuntimeError(f"PROVIDER4_NEW_VERIFICAR_UNKNOWN_RESPONSE:{text_preview[:300]}")
    
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
                inc_folio=False,
            )
        else:
            if inc_folio:
                html = self.consultar_por_curp_folio_vgetofi(
                    curp=term,
                    tipoa=tipoa,
                )
            else:
                html = self.consultar_por_curp(
                    curp=term,
                    tipoa=tipoa,
                    inc_folio=False,
                )
    
        print("PROVIDER4_BACKEND_HTML_PREVIEW =", html[:1200], flush=True)
        
        backend_up = (html or "").upper()
        term_up = (term or "").strip().upper()
        
        if "NO_LOCALIZADO" in backend_up and term_up in backend_up:
            print("PROVIDER4_NO_RECORD_DETECTED_IN_BACKEND =", term_up, flush=True)
            raise RuntimeError(f"PROVIDER4_NO_RECORD:{term_up}")
    
        html_up = (html or "").upper()

        if (
            "ENVIADO EXITOSAMENTE" in html_up
            or "SU TRAMITE HA SIDO ENVIADO" in html_up
            or "ENVIADO CON EXITO" in html_up
            or "ENVIADO CON ÉXITO" in html_up
            or "TRAMITEEXISTENTE" in html_up
            or "TRÁMITEEXISTENTE" in html_up
        ):
            vget_html = html
        
            print("PROVIDER4_DIRECT_SUCCESS_RESPONSE_DETECTED", flush=True)
            print("PROVIDER4_VGET_BYPASSED_SUCCESS_RESPONSE =", vget_html[:1200], flush=True)
        
        else:
            vget_html = self.submit_vget_form(html)
            print("PROVIDER4_VGET_HTML_PREVIEW =", vget_html[:1200], flush=True)
    
        max_polls = self.HISTORY_MAX_POLLS
        poll_sleep_seconds = self.HISTORY_POLL_SLEEP

        history_tipoa = None if is_chain else tipoa
    
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
    
            if self._detect_no_result(history_html, term, history_tipoa) or self._detect_no_result_loose(history_html, term, history_tipoa):
                raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
    
            row_html = self._history_row_for_term(history_html, term, history_tipoa)

            tipoa_norm = (tipoa or "").strip().lower()
            is_special_curp_type = (
                not is_chain
                and tipoa_norm in {"matrimonio", "defuncion", "divorcio"}
            )
            
            if not inc_folio and not is_special_curp_type:
                direct_history_link = self._extract_pdf_link(history_html, term, history_tipoa)
                if direct_history_link:
                    history_confirmed = True
                    print("PROVIDER4_HISTORY_DIRECT_LINK_OK_NON_SPECIAL =", {
                        "term": term,
                        "tipoa": tipoa,
                        "link": direct_history_link,
                    }, flush=True)
                    print("PROVIDER4_FINAL_DOWNLOAD_LINK =", direct_history_link, flush=True)
            
                    pdf_bytes = self._download_and_validate_with_retries(
                        url=direct_history_link,
                        term=term,
                        tipoa=tipoa,
                        inc_folio=inc_folio,
                        is_chain=is_chain,
                        use_folio_downloader=False,
                        max_attempts=4,
                        sleep_seconds=4,
                    )
                    return pdf_bytes
            elif is_special_curp_type:
                print("PROVIDER4_HISTORY_DIRECT_LINK_DISABLED_FOR_SPECIAL_CURP_TYPE =", {
                    "term": term,
                    "tipoa": tipoa,
                }, flush=True)
    
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
                    dphp_link = self._extract_pdf_link_from_folio_row(history_html, term, history_tipoa)
                    folio_link = self._extract_folio_link(history_html, term, history_tipoa)
                
                    print("PROVIDER4_INC_FOLIO_MODE = TRUE", flush=True)
                    print("PROVIDER4_INC_FOLIO_TERM =", term, flush=True)
                    print("PROVIDER4_INC_FOLIO_TIPOA =", tipoa, flush=True)
                    print("PROVIDER4_INC_FOLIO_DPHP_LINK =", dphp_link, flush=True)
                    print("PROVIDER4_INC_FOLIO_ADD_FOL_LINK =", folio_link, flush=True)
                
                    # NUEVA REGLA:
                    # Si ya se pidió incF=on, preferimos d.php.
                    # addFol.php se deja solo como respaldo.
                    attempts = []
                
                    if dphp_link:
                        attempts.append(("DPHP_FOLIADO", dphp_link, False))
                
                    if folio_link:
                        attempts.append(("ADDFOL_FALLBACK", folio_link, True))
                
                    last_folio_error = None
                
                    for final_link_source, final_link, use_folio_downloader in attempts:
                        try:
                            print("PROVIDER4_FINAL_FOLIO_LINK_SOURCE =", final_link_source, flush=True)
                            print("PROVIDER4_FINAL_FOLIO_LINK =", final_link, flush=True)
                
                            pdf_bytes = self._download_and_validate_with_retries(
                                url=final_link,
                                term=term,
                                tipoa=tipoa,
                                inc_folio=inc_folio,
                                is_chain=is_chain,
                                use_folio_downloader=use_folio_downloader,
                                max_attempts=4,
                                sleep_seconds=4,
                            )
                
                            print("PROVIDER4_FOLIO_DOWNLOAD_OK_SOURCE =", final_link_source, flush=True)
                            return pdf_bytes
                
                        except Exception as folio_exc:
                            last_folio_error = folio_exc
                            print("PROVIDER4_FOLIO_DOWNLOAD_SOURCE_FAILED =", {
                                "source": final_link_source,
                                "url": final_link,
                                "error": str(folio_exc),
                            }, flush=True)
                            continue
                
                    if last_folio_error:
                        raise RuntimeError(f"PROVIDER4_FOLIO_ALL_DOWNLOADS_FAILED:{term}:{str(last_folio_error)[:250]}")
                
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
                    link = self._extract_pdf_link(history_html, term, history_tipoa)
                    if link:
                        print("PROVIDER4_FINAL_DOWNLOAD_LINK =", link, flush=True)
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            is_chain=is_chain,
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

            if not inc_folio and not is_special_curp_type:
                cross_link = self._find_pdf_link_in_any_lazaro_history(term, history_tipoa)
                if cross_link:
                    history_confirmed = True
                    print("PROVIDER4_CROSS_HISTORY_FINAL_DOWNLOAD_LINK =", cross_link, flush=True)
            
                    pdf_bytes = self._download_and_validate_with_retries(
                        url=cross_link,
                        term=term,
                        tipoa=tipoa,
                        inc_folio=inc_folio,
                        is_chain=is_chain,
                        use_folio_downloader=False,
                        max_attempts=4,
                        sleep_seconds=4,
                    )
                    return pdf_bytes
            elif is_special_curp_type:
                print("PROVIDER4_CROSS_HISTORY_DISABLED_FOR_SPECIAL_CURP_TYPE =", {
                    "term": term,
                    "tipoa": tipoa,
                    "hid": self.HID,
                }, flush=True)
            
            print(
                f"PROVIDER4_HISTORY_LINK_NOT_READY_ATTEMPT_{poll_attempt+1} = {term}",
                flush=True,
            )
            time.sleep(poll_sleep_seconds)
    
        final_history_html = self.get_history_html()

        if self._detect_no_result(final_history_html, term, history_tipoa) or self._detect_no_result_loose(final_history_html, term, history_tipoa):
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

            if self._detect_no_result(history_html, term, history_tipoa) or self._detect_no_result_loose(history_html, term, history_tipoa):
                raise RuntimeError(f"PROVIDER4_NO_RECORD:{term}")
        
            row_html = self._history_row_for_term(history_html, term, history_tipoa)
            if row_html:
                if inc_folio:
                    dphp_link = self._extract_pdf_link_from_folio_row(history_html, term, history_tipoa)
                    folio_link = self._extract_folio_link(history_html, term, history_tipoa)
                
                    print("PROVIDER4_LATE_INC_FOLIO_MODE = TRUE", flush=True)
                    print("PROVIDER4_LATE_INC_FOLIO_DPHP_LINK =", dphp_link, flush=True)
                    print("PROVIDER4_LATE_INC_FOLIO_ADD_FOL_LINK =", folio_link, flush=True)
                
                    attempts = []
                
                    if dphp_link:
                        attempts.append(("LATE_DPHP_FOLIADO", dphp_link, False))
                
                    if folio_link:
                        attempts.append(("LATE_ADDFOL_FALLBACK", folio_link, True))
                
                    last_folio_error = None
                
                    for final_link_source, final_link, use_folio_downloader in attempts:
                        try:
                            print("PROVIDER4_LATE_FINAL_FOLIO_LINK_SOURCE =", final_link_source, flush=True)
                            print("PROVIDER4_LATE_FINAL_FOLIO_LINK =", final_link, flush=True)
                
                            pdf_bytes = self._download_and_validate_with_retries(
                                url=final_link,
                                term=term,
                                tipoa=tipoa,
                                inc_folio=inc_folio,
                                is_chain=is_chain,
                                use_folio_downloader=use_folio_downloader,
                                max_attempts=4,
                                sleep_seconds=4,
                            )
                
                            print("PROVIDER4_LATE_FOLIO_DOWNLOAD_OK_SOURCE =", final_link_source, flush=True)
                            return pdf_bytes
                
                        except Exception as folio_exc:
                            last_folio_error = folio_exc
                            print("PROVIDER4_LATE_FOLIO_DOWNLOAD_SOURCE_FAILED =", {
                                "source": final_link_source,
                                "url": final_link,
                                "error": str(folio_exc),
                            }, flush=True)
                            continue
                else:
                    link = self._extract_pdf_link(history_html, term, history_tipoa)
                    if link:
                        print(f"PROVIDER4_LATE_PDF_LINK_FOUND_ATTEMPT_{extra_attempt+1} = {link}", flush=True)
                        pdf_bytes = self._download_and_validate_with_retries(
                            url=link,
                            term=term,
                            tipoa=tipoa,
                            inc_folio=inc_folio,
                            is_chain=is_chain,
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
