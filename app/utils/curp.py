import re
import unicodedata


def normalize_text(text: str) -> str:
    text = (text or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


def detect_act_type(text: str) -> str:
    t = normalize_text(text)
    t_nospace = t.replace(" ", "")

    if "NACIMIENTOFOLIO" in t_nospace:
        return "NACIMIENTO FOLIO"
    if "DEFUNCION" in t_nospace:
        return "DEFUNCION"
    if "MATRIMONIO" in t_nospace:
        return "MATRIMONIO"
    if "DIVORCIO" in t_nospace:
        return "DIVORCIO"

    # nacimiento por default y tolerante a typo tipo "nacimieinto"
    if "NACIMIENTO" in t_nospace or "NACIMI" in t_nospace:
        return "NACIMIENTO"

    return "NACIMIENTO"


def provider_label_for_type(act_type: str) -> str:
    act_type = normalize_text(act_type)

    if act_type == "NACIMIENTO FOLIO":
        return "nacimiento folio"
    if act_type == "DEFUNCION":
        return "defuncion"
    if act_type == "MATRIMONIO":
        return "matrimonio"
    if act_type == "DIVORCIO":
        return "divorcio"

    return "nacimiento"


def _remove_type_words(line: str) -> str:
    x = normalize_text(line)

    # orden importante: primero el más específico
    patterns = [
        r"NACIMIENTO\s*FOLIO",
        r"NACIMIENTOFOLIO",
        r"DE\s+NACIMIENTO",
        r"NACIMIENTO",
        r"NACIMI\w*",   # tolera "nacimieinto"
        r"DEFUNCION",
        r"MATRIMONIO",
        r"DIVORCIO",
    ]

    for p in patterns:
        x = re.sub(p, " ", x, flags=re.IGNORECASE)

    return " ".join(x.split())


def _extract_identifier_from_line(line: str) -> str | None:
    """
    Prioridad:
    1) CURP-like: 18 alfanuméricos
    2) cadena de 20 dígitos
    3) código genérico alfanumérico (6 a 30)
    """
    cleaned = _remove_type_words(line)

    # CURP o identificador de 18
    m = re.search(r"\b([A-Z0-9]{18})\b", cleaned)
    if m:
        return m.group(1)

    # cadena de 20 dígitos
    m = re.search(r"\b(\d{20})\b", cleaned)
    if m:
        return m.group(1)

    # código genérico
    m = re.search(r"\b([A-Z0-9]{6,30})\b", cleaned)
    if m:
        return m.group(1)

    return None


def extract_request_terms(text: str) -> list[str]:
    """
    Soporta:
    - una solicitud por línea
    - term + tipo separado por espacios
    - term pegado con 'Nacimiento'
    """
    text = text or ""
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    if not lines:
        lines = [text.strip()] if text.strip() else []

    found = []
    for line in lines:
        term = _extract_identifier_from_line(line)
        if term and term not in found:
            found.append(term)

    return found


def extract_identifier_loose(text: str) -> str | None:
    """
    Para respuestas del proveedor:
    intenta sacar un identificador desde texto libre.
    """
    text = normalize_text(text)

    m = re.search(r"\b([A-Z0-9]{18})\b", text)
    if m:
        return m.group(1)

    m = re.search(r"\b(\d{20})\b", text)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z0-9]{6,30})\b", text)
    if m:
        return m.group(1)

    return None


def extract_identifier_from_filename(filename: str) -> str | None:
    if not filename:
        return None

    name = normalize_text(filename)

    m = re.search(r"\b([A-Z0-9]{18})\b", name)
    if m:
        return m.group(1)

    m = re.search(r"\b(\d{20})\b", name)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z0-9]{6,30})\b", name)
    if m:
        return m.group(1)

    return None
