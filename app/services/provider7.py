import io
import re
import time
from pathlib import Path
from typing import Any

import json
import random
import redis

from app.config import settings


def _get_redis_client():
    return redis.from_url(settings.REDIS_URL, decode_responses=True)

_PROVIDER7_LOCK_KEY = "provider7:sid:lock"
_PROVIDER7_MINUTE_KEY = "provider7:sid:minute_bucket"
_PROVIDER7_COOLDOWN_KEY = "provider7:sid:cooldown_until"
_PROVIDER7_BATCH_KEY = "provider7:sid:batch_count"

import requests
from pypdf import PdfReader, PdfWriter, Transformation

from app.config import settings
from app.services.provider_sid_oaxaca import SidOaxacaClient


DEFAULT_FRAME_URL = "https://enmarcadonew-production.up.railway.app/process_pdf"

MAPA_ESTADOS = {
    "01": "AGUASCALIENTES",
    "02": "BAJA_CALIFORNIA",
    "03": "BAJA_CALIFORNIA_SUR",
    "04": "CAMPECHE",
    "05": "COAHUILA",
    "06": "COLIMA",
    "07": "CHIAPAS",
    "08": "CHIHUAHUA",
    "09": "CIUDAD_DE_MEXICO",
    "10": "DURANGO",
    "11": "GUANAJUATO",
    "12": "GUERRERO",
    "13": "HIDALGO",
    "14": "JALISCO",
    "15": "MEXICO",
    "16": "MICHOACAN",
    "17": "MORELOS",
    "18": "NAYARIT",
    "19": "NUEVO_LEON",
    "20": "OAXACA",
    "21": "PUEBLA",
    "22": "QUERETARO",
    "23": "QUINTANA_ROO",
    "24": "SAN_LUIS_POTOSI",
    "25": "SINALOA",
    "26": "SONORA",
    "27": "TABASCO",
    "28": "TAMAULIPAS",
    "29": "TLAXCALA",
    "30": "VERACRUZ",
    "31": "YUCATAN",
    "32": "ZACATECAS",
}


def _strip_or_default(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _provider7_now() -> float:
    return time.time()


def _provider7_sleep_human(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _provider7_set_cooldown(seconds: int = 600) -> None:
    r = _get_redis_client()
    until = _provider7_now() + max(1, int(seconds))
    r.set(_PROVIDER7_COOLDOWN_KEY, str(until), ex=max(60, int(seconds) + 60))


def _provider7_check_cooldown() -> None:
    r = _get_redis_client()
    raw = r.get(_PROVIDER7_COOLDOWN_KEY)
    if not raw:
        return

    try:
        until = float(raw)
    except Exception:
        return

    now = _provider7_now()
    if now < until:
        remaining = until - now
        raise RuntimeError(f"PROVIDER7_COOLDOWN_ACTIVE: espera {remaining:.1f}s")


def _provider7_wait_turn() -> None:
    r = _get_redis_client()

    _provider7_check_cooldown()

    while True:
        now = _provider7_now()
        minute_start = int(now // 60) * 60
        bucket_value = r.get(_PROVIDER7_MINUTE_KEY)

        if bucket_value:
            try:
                bucket = json.loads(bucket_value)
            except Exception:
                bucket = {"minute_start": minute_start, "count": 0}
        else:
            bucket = {"minute_start": minute_start, "count": 0}

        if bucket["minute_start"] != minute_start:
            bucket = {"minute_start": minute_start, "count": 0}

        max_this_minute = random.choice([4, 5])

        if bucket["count"] < max_this_minute:
            bucket["count"] += 1
            r.set(_PROVIDER7_MINUTE_KEY, json.dumps(bucket), ex=90)
            break

        wait_s = (minute_start + 60) - now
        wait_s += random.uniform(2.0, 8.0)
        time.sleep(max(1.0, wait_s))

    _provider7_sleep_human(2.0, 5.0)

    batch_count = r.incr(_PROVIDER7_BATCH_KEY)
    if batch_count == 1:
        r.expire(_PROVIDER7_BATCH_KEY, 3600)

    if batch_count % random.randint(4, 7) == 0:
        _provider7_sleep_human(8.0, 15.0)


class Provider7RedisLock:
    def __init__(self, key: str, ttl: int = 120):
        self.key = key
        self.ttl = ttl
        self.r = _get_redis_client()
        self.token = f"{time.time()}-{random.randint(1000,999999)}"

    def __enter__(self):
        while True:
            _provider7_check_cooldown()
            acquired = self.r.set(self.key, self.token, nx=True, ex=self.ttl)
            if acquired:
                return self
            time.sleep(random.uniform(2.0,5.0))

    def __exit__(self, exc_type, exc, tb):
        try:
            current = self.r.get(self.key)
            if current == self.token:
                self.r.delete(self.key)
        except Exception:
            pass


def _normalize_estado(nombre: str) -> str:
    if not nombre:
        return ""

    nombre = nombre.strip().upper()
    reemplazos = {
        "Á": "A",
        "É": "E",
        "Í": "I",
        "Ó": "O",
        "Ú": "U",
        "Ü": "U",
        "Ñ": "N",
    }
    for a, b in reemplazos.items():
        nombre = nombre.replace(a, b)

    alias = {
        "MICHOACAN DE OCAMPO": "MICHOACAN",
        "COAHUILA DE ZARAGOZA": "COAHUILA",
        "VERACRUZ DE IGNACIO DE LA LLAVE": "VERACRUZ",
        "MEXICO": "MEXICO",
        "ESTADO DE MEXICO": "MEXICO",
        "CIUDAD DE MEXICO": "CIUDAD_DE_MEXICO",
    }

    nombre = alias.get(nombre, nombre)
    nombre = nombre.replace(" ", "_")
    nombre = re.sub(r"[^A-Z_]", "", nombre)
    return nombre


def _is_curp(term: str) -> bool:
    term = _strip_or_default(term).upper()
    return bool(
        re.match(
            r"^[A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d$",
            term,
            re.IGNORECASE,
        )
    )


def _is_chain(term: str) -> bool:
    term = _strip_or_default(term)
    return term.isdigit() and len(term) >= 18


def _act_type_to_sid(act_type: str) -> str:
    act_type = _strip_or_default(act_type).upper()
    mapping = {
        "NACIMIENTO": "nacimiento",
        "NACIMIENTO FOLIO": "nacimiento",
        "MATRIMONIO": "matrimonio",
        "MATRIMONIO FOLIO": "matrimonio",
        "DEFUNCION": "defuncion",
        "DEFUNCION FOLIO": "defuncion",
        "DIVORCIO": "divorcio",
        "DIVORCIO FOLIO": "divorcio",
    }
    return mapping.get(act_type, "nacimiento")


def _estado_desde_row(row: dict[str, Any]) -> str:
    if row.get("estadoNacNombre"):
        return _strip_or_default(row["estadoNacNombre"])

    if row.get("estadoRegNombre"):
        return _strip_or_default(row["estadoRegNombre"])

    if row.get("estadoNac"):
        clave = str(row["estadoNac"]).zfill(2)
        if clave in MAPA_ESTADOS:
            return MAPA_ESTADOS[clave]

    if row.get("estadoReg"):
        clave = str(row["estadoReg"]).zfill(2)
        if clave in MAPA_ESTADOS:
            return MAPA_ESTADOS[clave]

    raise RuntimeError(f"PROVIDER7_NO_ESTADO_EN_ROW: keys={list(row.keys())}")


def _estado_desde_cadena(cadena: str) -> str:
    cadena = _strip_or_default(cadena)
    if len(cadena) < 3:
        return "DESCONOCIDO"

    # En tus pruebas reales la clave correcta quedó en cadena[1:3]
    clave = cadena[1:3]
    return MAPA_ESTADOS.get(clave, "DESCONOCIDO")


def _unir_pdfs_bytes(pdf1_bytes: bytes, pdf2_path: Path) -> bytes:
    writer = PdfWriter()

    reader1 = PdfReader(io.BytesIO(pdf1_bytes))
    for page in reader1.pages:
        writer.add_page(page)

    reader2 = PdfReader(str(pdf2_path))
    for page in reader2.pages:
        writer.add_page(page)

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _resolver_reverso_por_estado(estado: str, estados_dir: Path) -> Path:
    estado_norm = _normalize_estado(estado)

    pdf_path = estados_dir / f"{estado_norm}.pdf"
    if pdf_path.exists():
        return pdf_path

    for candidate in estados_dir.glob("*.pdf"):
        if _normalize_estado(candidate.stem) == estado_norm:
            return candidate

    raise RuntimeError(
        f"PROVIDER7_REAR_FRAME_NOT_FOUND: estado={estado} dir={estados_dir}"
    )


def _solo_primera_pagina_pdf(pdf_bytes: bytes) -> bytes:
    reader = PdfReader(io.BytesIO(pdf_bytes))

    if len(reader.pages) < 1:
        raise RuntimeError("SOURCE_PDF_WITHOUT_PAGES")

    writer = PdfWriter()
    writer.add_page(reader.pages[0])

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _pdf_front_has_green_frame(pdf_bytes: bytes) -> bool:
    """
    Detecta si la página 1 ya trae marco verde.
    No solo revisa el borde exterior, porque algunos PDFs de Lázaro
    traen el marco verde metido/insetado dentro de la página.
    """
    try:
        import fitz

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        if len(doc) < 1:
            return False

        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(0.35, 0.35), alpha=False)

        w = pix.width
        h = pix.height
        n = pix.n
        data = pix.samples

        total_all = 0
        green_all = 0

        total_top = 0
        green_top = 0

        total_left_right = 0
        green_left_right = 0

        # Zonas donde suele venir el marco original:
        # - parte superior completa
        # - laterales izquierdo/derecho
        top_limit = int(h * 0.22)
        side_width = int(w * 0.18)

        for y in range(0, h, 2):
            for x in range(0, w, 2):
                idx = (y * w + x) * n

                r = data[idx]
                g = data[idx + 1]
                b = data[idx + 2]

                # Verde oscuro del marco.
                # Evita contar el watermark verde muy claro.
                is_dark_green = (
                    45 <= g <= 150
                    and r <= 130
                    and b <= 130
                    and g > r + 10
                    and g > b + 10
                )

                total_all += 1
                if is_dark_green:
                    green_all += 1

                if y <= top_limit:
                    total_top += 1
                    if is_dark_green:
                        green_top += 1

                if x <= side_width or x >= w - side_width:
                    total_left_right += 1
                    if is_dark_green:
                        green_left_right += 1

        ratio_all = green_all / max(total_all, 1)
        ratio_top = green_top / max(total_top, 1)
        ratio_sides = green_left_right / max(total_left_right, 1)

        print(
            "LOCAL_FRAME_GREEN_DETECT =",
            {
                "green_all": green_all,
                "total_all": total_all,
                "ratio_all": round(ratio_all, 4),
                "ratio_top": round(ratio_top, 4),
                "ratio_sides": round(ratio_sides, 4),
            },
            flush=True,
        )

        # Si ya trae marco, suele haber bastante verde oscuro arriba/laterales.
        return (
            ratio_all >= 0.012
            or ratio_top >= 0.018
            or ratio_sides >= 0.015
        )

    except Exception as e:
        print("LOCAL_FRAME_GREEN_DETECT_FAILED =", str(e), flush=True)
        return False


def _enmarcar_pdf_frente(pdf_bytes: bytes, filename: str, timeout: int = 120, folio: bool = False) -> bytes:

    base_dir = Path(__file__).resolve().parent.parent
    marco_path = base_dir / "assets" / "MARCO-ACTA-DE-NACIMIENTO.pdf"

    if not marco_path.exists():
        raise RuntimeError(f"LOCAL_FRAME_NOT_FOUND:{marco_path}")

    if _pdf_front_has_green_frame(pdf_bytes):
        print("LOCAL_FRAME_SKIPPED_SOURCE_ALREADY_HAS_GREEN_FRAME =", filename, flush=True)
        return _solo_primera_pagina_pdf(pdf_bytes)

    try:
        src_reader = PdfReader(io.BytesIO(pdf_bytes))
        if len(src_reader.pages) < 1:
            raise RuntimeError("SOURCE_PDF_WITHOUT_PAGES")

        marco_reader = PdfReader(str(marco_path))
        if len(marco_reader.pages) < 1:
            raise RuntimeError("FRAME_PDF_WITHOUT_PAGES")

        # Página del marco como fondo
        frame_page = marco_reader.pages[0]

        # Solo usamos la primera página del acta.
        # Si Lázaro trae 2 páginas, ignoramos su reverso y luego pegamos nuestro reverso local.
        src_page = src_reader.pages[0]

        fw = float(frame_page.mediabox.width)
        fh = float(frame_page.mediabox.height)
        sw = float(src_page.mediabox.width)
        sh = float(src_page.mediabox.height)

        # Ajustes del área interior del marco.
        # Si quieres que el acta salga más grande/chica, ajusta estos valores.
        margin_left = 58
        margin_right = 58
        margin_top = 130
        margin_bottom = 70

        target_w = fw - margin_left - margin_right
        target_h = fh - margin_top - margin_bottom

        scale = min(target_w / sw, target_h / sh)

        tx = margin_left + ((target_w - (sw * scale)) / 2)
        ty = margin_bottom + ((target_h - (sh * scale)) / 2)

        src_page.add_transformation(
            Transformation()
            .scale(scale)
            .translate(tx, ty)
        )

        # Marco primero, acta encima, dejando visible el borde.
        frame_page.merge_page(src_page)

        writer = PdfWriter()
        writer.add_page(frame_page)

        out = io.BytesIO()
        writer.write(out)

        framed = out.getvalue()

        if not framed.startswith(b"%PDF"):
            raise RuntimeError("LOCAL_FRAME_INVALID_OUTPUT")

        print(
            "LOCAL_FRAME_OK =",
            {
                "filename": filename,
                "folio": folio,
                "frame": str(marco_path),
                "scale": round(scale, 4),
                "tx": round(tx, 2),
                "ty": round(ty, 2),
                "src_pages": len(src_reader.pages),
            },
            flush=True,
        )

        return framed

    except Exception as e:
        print("LOCAL_FRAME_FAILED =", str(e), flush=True)
        raise RuntimeError(f"LOCAL_FRAME_FAILED:{filename}:{str(e)[:300]}")


def procesar_pdf_externo_provider8(
    *,
    pdf_bytes: bytes,
    term: str,
    act_type: str,
    filename: str = "provider8.pdf",
) -> dict:
    term = _strip_or_default(term).upper()
    act_upper = _strip_or_default(act_type).upper()

    is_folio = "FOLI" in act_upper

    pdf_bytes = _enmarcar_pdf_frente(
        pdf_bytes,
        filename,
        folio=is_folio,
    )

    estado = ""

    if _is_chain(term):
        estado = _estado_desde_cadena(term)

    elif _is_curp(term):
        # CURP: entidad nacimiento posiciones 12-13 visuales = índices 11:13
        clave_estado = term[11:13]
        estado = MAPA_ESTADOS.get(clave_estado, "")

    if not estado or estado == "DESCONOCIDO":
        raise RuntimeError(f"PROVIDER8_NO_ESTADO_FOR_REAR_FRAME: {term}")

    base_dir = Path(__file__).resolve().parent.parent
    estados_dir = base_dir / "assets" / "estados"

    reverso_path = _resolver_reverso_por_estado(estado, estados_dir)

    # 3) Agregar reverso del estado
    pdf_bytes = _unir_pdfs_bytes(pdf_bytes, reverso_path)

    return {
        "pdf_bytes": pdf_bytes,
        "estado": estado,
        "folio": is_folio,
    }


class Provider7Client:
    def __init__(
        self,
        *,
        access_token: str,
        jsessionid: str,
        oficialia: int | str,
        rfc_usuario: str,
    ) -> None:
        self.access_token = _strip_or_default(access_token)
        self.jsessionid = _strip_or_default(jsessionid)
        self.oficialia = _strip_or_default(oficialia)
        self.rfc_usuario = _strip_or_default(rfc_usuario).upper()

        BASE_DIR = Path(__file__).resolve().parent.parent
        self.estados_dir = BASE_DIR / "assets" / "estados"

        self.sid = SidOaxacaClient(
            access_token=self.access_token,
            jsessionid=self.jsessionid,
            oficialia=self.oficialia,
            rfc_usuario=self.rfc_usuario,
        )

    def warm_session(self) -> dict[str, Any]:
        return self.sid.warm_session()

    def _consultar_por_curp(self, term: str, act_type: str) -> dict[str, Any]:
        acto = _act_type_to_sid(act_type)
        rows = self.sid.consultar_por_curp(term, acto=acto)

        if not rows:
            raise RuntimeError("PROVIDER7_CURP_NO_RESULTS")

        row = rows[0]
        cadena = _strip_or_default(row.get("cadena"))
        sexo = _strip_or_default(row.get("sexo")).upper()
        estado = _estado_desde_row(row)

        if not cadena:
            raise RuntimeError("PROVIDER7_CURP_NO_CADENA")
        
        if sexo == "H":
            sexo = "M"

        if sexo not in {"M", "F"}:
            sexo = "M"

        return {
            "row": row,
            "cadena": cadena,
            "sexo": sexo,
            "estado": estado,
            "term_type": "CURP",
            "filename_base": _strip_or_default(row.get("curp"), term).upper(),
        }

    def _consultar_por_cadena(self, term: str, act_type: str) -> dict[str, Any]:
        row = None

        # Si tu SidOaxacaClient ya trae consultar_por_cadena, se aprovecha.
        if hasattr(self.sid, "consultar_por_cadena"):
            try:
                result = self.sid.consultar_por_cadena(term)
                if isinstance(result, list) and result:
                    row = result[0]
                elif isinstance(result, dict) and result:
                    row = result
            except Exception:
                row = None

        sexo = ""
        estado = ""

        if row:
            sexo = _strip_or_default(row.get("sexo")).upper()
            try:
                estado = _estado_desde_row(row)
            except Exception:
                estado = ""

        if sexo == "H":
            sexo = "M"

        if sexo not in {"M", "F"}:
            sexo = "M"

        if not estado:
            estado = _estado_desde_cadena(term)

        if not estado or estado == "DESCONOCIDO":
            raise RuntimeError("PROVIDER7_CHAIN_NO_ESTADO")

        return {
            "row": row or {},
            "cadena": _strip_or_default(term),
            "sexo": sexo,
            "estado": estado,
            "term_type": "CADENA",
            "filename_base": _strip_or_default(term),
        }

    def _resolver_contexto(self, term: str, act_type: str) -> dict[str, Any]:
        term = _strip_or_default(term).upper()

        if _is_curp(term):
            return self._consultar_por_curp(term, act_type)

        if _is_chain(term):
            return self._consultar_por_cadena(term, act_type)

        raise RuntimeError("PROVIDER7_TERM_UNSUPPORTED")

    def generar_pdf_bytes(
        self,
        *,
        term: str,
        act_type: str,
        agregar_marco_frontal: bool = True,
        agregar_reverso_estado: bool = True,
    ) -> dict[str, Any]:

        with Provider7RedisLock(_PROVIDER7_LOCK_KEY, ttl=settings.REQUEST_TIMEOUT_MINUTES * 60 + 120):

            _provider7_wait_turn()

            warm = self.warm_session()
            if not warm.get("ok"):
                warm_text = str(warm)

                if "invalid_token" in warm_text or "SESSION_INVALID" in warm_text:
                    _provider7_set_cooldown(600)

                raise RuntimeError(f"PROVIDER7_WARM_FAILED: {warm}")

            _provider7_wait_turn()

            ctx = self._resolver_contexto(term, act_type)

            referencia = f"{ctx['cadena']}__XX_X"
            folio_impresion = f"{int(time.time())}-S"

            _provider7_wait_turn()

            try:
                pdf_bytes = self.sid.descargar_pdf_acta(
                    folio_impresion=folio_impresion,
                    referencia=referencia,
                    formato=1,
                    sexo=ctx["sexo"],
                )
            except Exception as e:

                err = str(e)

                if "invalid_token" in err or "SESSION_INVALID" in err:
                    _provider7_set_cooldown(600)

                raise

            if not pdf_bytes:
                raise RuntimeError("PROVIDER7_NO_PDF_DOWNLOADED")

            filename_base = ctx["filename_base"] or "SID_OAXACA"

            act_upper = _strip_or_default(act_type).upper()
            is_folio = "FOLI" in act_upper

            if agregar_marco_frontal:
                pdf_bytes = _enmarcar_pdf_frente(
                    pdf_bytes,
                    f"{filename_base}.pdf",
                    folio=is_folio,
                )

            if agregar_reverso_estado:

                if not self.estados_dir:
                    raise RuntimeError("PROVIDER7_ESTADOS_DIR_EMPTY")

                reverso_path = _resolver_reverso_por_estado(
                    ctx["estado"],
                    self.estados_dir,
                )

                pdf_bytes = _unir_pdfs_bytes(pdf_bytes, reverso_path)

            return {
                "pdf_bytes": pdf_bytes,
                "estado": ctx["estado"],
                "sexo": ctx["sexo"],
                "cadena": ctx["cadena"],
                "term_type": ctx["term_type"],
                "filename_base": filename_base,
                "warm": warm,
                "raw_row": ctx.get("row") or {},
            }
