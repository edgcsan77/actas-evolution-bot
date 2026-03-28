import re
import unicodedata


def normalize_text(text: str) -> str:
    text = (text or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


CURP_REGEX = r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2}"
NUM20_REGEX = r"\d{20}"


def detect_act_type(text: str) -> str:
    t = normalize_text(text)
    t_nospace = re.sub(r"\s+", "", t)

    if ("FOLIO" in t_nospace or "FOLIADO" in t_nospace or "FOLIADA" in t_nospace) and not any(x in t_nospace for x in ["NAC", "MAT", "DEF", "DIV"]):
        return "NACIMIENTO FOLIO"

    if any(x in t_nospace for x in [
        "NACIMIENTOFOLIO", "NACIMIENTOCONFOLIO", "ACTADENACIMIENTOFOLIO",
        "NACIMFOLIO", "NACFOLIO", "FOLIONACIMIENTO", "FOLIONAC",
        "NACIMIENTOFOLIADO", "ACTADENACIMIENTOFOLIADO",
        "NACIMFOLIADO", "NACFOLIADO", "FOLIADONACIMIENTO", "FOLIADONAC"
    ]):
        return "NACIMIENTO FOLIO"

    if any(x in t_nospace for x in [
        "MATRIMONIOFOLIO", "ACTADEMATRIMONIOFOLIO",
        "MATRIFOLIO", "MATFOLIO", "FOLIOMATRIMONIO", "FOLIOMAT"
    ]):
        return "MATRIMONIO FOLIO"

    if any(x in t_nospace for x in [
        "DEFUNCIONFOLIO", "ACTADEDEFUNCIONFOLIO",
        "DEFFOLIO", "DEFUNFOLIO", "FOLIODEFUNCION", "FOLIODEF"
    ]):
        return "DEFUNCION FOLIO"

    if any(x in t_nospace for x in [
        "DIVORCIOFOLIO", "ACTADEDIVORCIOFOLIO",
        "DIVFOLIO", "DIVORFOLIO", "FOLIODIVORCIO", "FOLIODIV"
    ]):
        return "DIVORCIO FOLIO"

    if "FOLIO" in t_nospace or "FOLIADO" in t_nospace or "FOLIADA" in t_nospace:
        if any(x in t_nospace for x in ["NACIMIENTO", "NACIM", "NAC"]):
            return "NACIMIENTO FOLIO"
        if any(x in t_nospace for x in ["MATRIMONIO", "MATRI", "MAT"]):
            return "MATRIMONIO FOLIO"
        if any(x in t_nospace for x in ["DEFUNCION", "DEFUN", "DEF"]):
            return "DEFUNCION FOLIO"
        if any(x in t_nospace for x in ["DIVORCIO", "DIVOR", "DIV"]):
            return "DIVORCIO FOLIO"

    if any(x in t_nospace for x in [
        "NACIMIENTO", "ACTADENACIMIENTO", "NACIM", "NAC"
    ]):
        return "NACIMIENTO"

    if any(x in t_nospace for x in [
        "MATRIMONIO", "ACTADEMATRIMONIO", "MATRI"
    ]):
        return "MATRIMONIO"

    if any(x in t_nospace for x in [
        "DEFUNCION", "ACTADEDEFUNCION", "DEFUN", "DEF"
    ]):
        return "DEFUNCION"

    if any(x in t_nospace for x in [
        "DIVORCIO", "ACTADEDIVORCIO", "DIVOR", "DIV"
    ]):
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
        r"CODIGO\s+DE\s+VERIFICACION",
        r"CODIGO\s+VERIFICACION",
        r"VERIFICACION",
        r"IDENTIFICADOR\s+ELECTRONICO",
        r"IDENTIFICADOR",
        r"CADENA",
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
        r"FOLIADA",
    ]

    for p in patterns:
        x = re.sub(p, " ", x, flags=re.IGNORECASE)

    return " ".join(x.split())


def is_chain(term: str) -> bool:
    term = (term or "").strip()
    return bool(re.fullmatch(NUM20_REGEX, term))


def is_curp(term: str) -> bool:
    term = normalize_text(term)
    return bool(re.fullmatch(CURP_REGEX, term))


def _extract_identifier_from_line(line: str) -> str | None:
    cleaned = _remove_type_words(line)

    m = re.search(rf"\b({CURP_REGEX})\b", cleaned)
    if m:
        return m.group(1)

    m = re.search(rf"\b({NUM20_REGEX})\b", cleaned)
    if m:
        return m.group(1)

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

    m = re.search(rf"\b({CURP_REGEX})\b", text)
    if m:
        return m.group(1)

    m = re.search(rf"\b({NUM20_REGEX})\b", text)
    if m:
        return m.group(1)

    return None


def extract_identifier_from_filename(filename: str) -> str | None:
    if not filename:
        return None

    name = normalize_text(filename)

    m = re.search(rf"\b({CURP_REGEX})\b", name)
    if m:
        return m.group(1)

    m = re.search(rf"\b({NUM20_REGEX})\b", name)
    if m:
        return m.group(1)

    return None


def seems_like_identifier_attempt(text: str) -> bool:
    t = normalize_text(text)

    # Palabras que indican que el usuario sí quiso mandar un dato
    keywords = [
        "CURP",
        "CADENA",
        "IDENTIFICADOR",
        "IDENTIFICADOR ELECTRONICO",
        "CODIGO",
        "CODIGO DE VERIFICACION",
        "VERIFICACION",
        "ACTA",
        "NACIMIENTO",
        "MATRIMONIO",
        "DEFUNCION",
        "DIVORCIO",
        "FOLIO",
    ]
    if any(k in t for k in keywords):
        return True

    # Si trae un bloque largo de números, probablemente intentó mandar cadena/código
    if re.search(r"\d{8,25}", t):
        return True

    # Si trae mezcla de letras y números tipo CURP rara
    if re.search(r"[A-Z]{3,}\d{2,}[A-Z0-9]{2,}", t):
        return True

    return False


def detect_identifier_problem(text: str) -> str | None:
    t = normalize_text(text)
    cleaned = _remove_type_words(t)

    # Si es conversación natural y no parece intento de dato, no marcar error
    if not seems_like_identifier_attempt(text):
        return None

    # Solo CURP
    if "CURP" in t:
        m = re.search(r"[A-Z0-9]+", cleaned)
        token = m.group(0) if m else ""

        if not token:
            return (
                "⚠️ No se detectó una CURP válida.\n\n"
                "La CURP debe tener exactamente 18 caracteres:\n"
                "4 letras, 6 dígitos, 6 letras y 2 caracteres alfanuméricos"
            )

        if not re.fullmatch(CURP_REGEX, token):
            return (
                "⚠️ La CURP parece incompleta o incorrecta.\n\n"
                "La CURP debe tener exactamente 18 caracteres:\n"
                "4 letras, 6 dígitos, 6 letras y 2 caracteres alfanuméricos"
            )

    # Solo cadena / identificador / código de verificación
    if any(x in t for x in [
        "CADENA",
        "IDENTIFICADOR ELECTRONICO",
        "IDENTIFICADOR",
        "CODIGO DE VERIFICACION",
        "CODIGO VERIFICACION",
        "VERIFICACION",
    ]):
        digit_runs = re.findall(r"\d+", cleaned)
        token = max(digit_runs, key=len) if digit_runs else ""

        if not token:
            return (
                "⚠️ No se detectó una cadena válida.\n\n"
                "La cadena, identificador electrónico o código de verificación debe tener exactamente 20 dígitos.\n"
            )

        if len(token) != 20:
            return (
                "⚠️ La cadena, identificador electrónico o código de verificación parece incompleto o incorrecto.\n\n"
                "Debe tener exactamente 20 dígitos.\n"
            )

    # Números parecidos pero mal
    digit_runs = re.findall(r"\d{1,25}", cleaned)
    for d in digit_runs:
        if len(d) != 20 and len(d) >= 8:
            return (
                "⚠️ La cadena, identificador electrónico o código de verificación parece incompleto o incorrecto.\n\n"
                "Debe tener exactamente 20 dígitos.\n"
            )

    # Tokens parecidos a CURP pero mal
    tokens = re.findall(r"[A-Z0-9]{8,25}", cleaned)
    for token in tokens:
        if re.fullmatch(CURP_REGEX, token):
            continue
        if re.fullmatch(NUM20_REGEX, token):
            continue

        has_letters = bool(re.search(r"[A-Z]", token))
        has_digits = bool(re.search(r"\d", token))

        if has_letters and has_digits:
            return (
                "⚠️ La CURP parece incompleta o incorrecta.\n\n"
                "La CURP debe tener exactamente 18 caracteres:\n"
                "4 letras, 6 dígitos, 6 letras y 2 caracteres alfanuméricos"
            )

    return (
        "⚠️ No se detectó un dato válido.\n\n"
        "Envía solo uno de estos formatos:\n\n"
        "CURP:\n"
        "AEHN810107MJCCRR09\n\n"
        "Cadena / Identificador electrónico:\n"
        "16102000120260027590\n\n"
        "Código de verificación:\n"
        "11610200011990009010"
    )
