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

    # FOLIO primero
    if "NACIMIENTOFOLIO" in t_nospace:
        return "NACIMIENTO FOLIO"
    if "MATRIMONIOFOLIO" in t_nospace:
        return "MATRIMONIO FOLIO"
    if "DEFUNCIONFOLIO" in t_nospace:
        return "DEFUNCION FOLIO"
    if "DIVORCIOFOLIO" in t_nospace:
        return "DIVORCIO FOLIO"

    if "NACIMIENTO" in t_nospace or "NACIMI" in t_nospace:
        return "NACIMIENTO"
    if "MATRIMONIO" in t_nospace:
        return "MATRIMONIO"
    if "DEFUNCION" in t_nospace:
        return "DEFUNCION"
    if "DIVORCIO" in t_nospace:
        return "DIVORCIO"

    return "NACIMIENTO"


def provider_label_for_type(act_type: str) -> str:
    act_type = normalize_text(act_type)

    mapping = {
        "NACIMIENTO": "nacimiento",
        "MATRIMONIO": "matrimonio",
        "DEFUNCION": "defuncion",
        "DIVORCIO": "divorcio",
        "NACIMIENTO FOLIO": "nacimiento folio",
        "MATRIMONIO FOLIO": "matrimonio folio",
        "DEFUNCION FOLIO": "defuncion folio",
        "DIVORCIO FOLIO": "divorcio folio",
    }
    return mapping.get(act_type, "nacimiento")


def _remove_type_words(line: str) -> str:
    x = normalize_text(line)

    patterns = [
        r"NACIMIENTO\s*FOLIO",
        r"MATRIMONIO\s*FOLIO",
        r"DEFUNCION\s*FOLIO",
        r"DIVORCIO\s*FOLIO",
        r"NACIMIENTOFOLIO",
        r"MATRIMONIOFOLIO",
        r"DEFUNCIONFOLIO",
        r"DIVORCIOFOLIO",
        r"DE\s+NACIMIENTO",
        r"DE\s+MATRIMONIO",
        r"DE\s+DEFUNCION",
        r"DE\s+DIVORCIO",
        r"NACIMIENTO",
        r"MATRIMONIO",
        r"DEFUNCION",
        r"DIVORCIO",
        r"NACIMI\w*",
    ]

    for p in patterns:
        x = re.sub(p, " ", x, flags=re.IGNORECASE)

    return " ".join(x.split())


def is_chain(term: str) -> bool:
    term = (term or "").strip()
    return term.isdigit() and len(term) == 20


def _extract_identifier_from_line(line: str) -> str | None:
    cleaned = _remove_type_words(line)

    # CURP estricta
    m = re.search(r"\b([A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2})\b", cleaned)
    if m:
        return m.group(1)

    # cadena 20 dígitos
    m = re.search(r"\b(\d{20})\b", cleaned)
    if m:
        return m.group(1)

    # código genérico, evitando confundir CURP incompleta
    tokens = re.findall(r"[A-Z0-9]{6,30}", cleaned)
    for token in tokens:
        if re.match(r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{0,2}$", token):
            return None

        if token.isdigit():
            if len(token) == 20:
                return token
            continue

        return token

    return None


def extract_request_terms(text: str) -> list[str]:
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
    text = normalize_text(text)

    m = re.search(r"\b([A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2})\b", text)
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

    m = re.search(r"\b([A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2})\b", name)
    if m:
        return m.group(1)

    m = re.search(r"\b(\d{20})\b", name)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z0-9]{6,30})\b", name)
    if m:
        return m.group(1)

    return None


def detect_identifier_problem(text: str) -> str | None:
    t = normalize_text(text)
    cleaned = _remove_type_words(t)

    digit_runs = re.findall(r"\d{8,25}", cleaned)
    for d in digit_runs:
        if len(d) != 20:
            return (
                "⚠️ La cadena parece incompleta o incorrecta.\n"
                "Debe tener exactamente 20 dígitos."
            )

    alnum_runs = re.findall(r"[A-Z0-9]{6,30}", cleaned)
    for token in alnum_runs:
        if token.isdigit():
            continue

        if 10 <= len(token) <= 17 or 19 <= len(token) <= 21:
            return (
                "⚠️ La CURP parece incompleta o incorrecta.\n"
                "Debe tener exactamente 18 caracteres."
            )

    if re.search(r"[A-Z]", cleaned) and re.search(r"\d", cleaned):
        return (
            "⚠️ El dato parece inválido o incompleto.\n"
            "Revisa la CURP, cadena o código e inténtalo de nuevo."
        )

    if re.fullmatch(r"\d+", cleaned):
        return (
            "⚠️ La cadena parece incorrecta.\n"
            "Debe tener exactamente 20 dígitos."
        )

    return None
