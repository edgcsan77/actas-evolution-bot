import re
import unicodedata
from datetime import date


def normalize_text(text: str) -> str:
    text = (text or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


CURP_REGEX = r"[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2}"
NUM20_REGEX = r"\d{20}"


# Códigos de entidad utilizados dentro de la CURP.
# NE = nacido en el extranjero.
CURP_ENTITY_CODES = {
    "AS", "BC", "BS", "CC", "CS", "CH", "CL", "CM",
    "DF", "DG", "GT", "GR", "HG", "JC", "MC", "MN",
    "MS", "NT", "NL", "OC", "PL", "QT", "QR", "SP",
    "SL", "SR", "TC", "TS", "TL", "VZ", "YN", "ZS",
    "NE",
}


_CURP_INTERNAL_CONSONANTS_RE = re.compile(
    r"^[B-DF-HJ-NP-TV-Z]{3}$"
)


def validate_curp_structure(term: str) -> tuple[bool, str | None]:
    """
    Valida una CURP recibida como solicitud del cliente.

    Esta validación es deliberadamente más estricta que CURP_REGEX:
    - 18 caracteres exactos.
    - Solo A-Z / 0-9.
    - Primer bloque con estructura de CURP.
    - Fecha AAMMDD válida.
    - Sexo H/M.
    - Entidad federativa válida.
    - Consonantes internas en posiciones 14-16.
    - Posición 17 alfanumérica.
    - Posición 18 numérica.

    IMPORTANTE:
    no autocorrige O/0, I/1, E/3, A/4, etc.
    Si el carácter está en una posición imposible, la CURP se rechaza.
    """

    curp = normalize_text(term)
    curp = re.sub(r"\s+", "", curp)

    if len(curp) != 18:
        return False, "La CURP debe tener exactamente 18 caracteres."

    if not re.fullmatch(r"[A-Z0-9]{18}", curp):
        return False, "La CURP solo puede contener letras y números."

    # --------------------------------------------------------
    # POSICIONES 1-4
    #
    # 1: inicial del primer apellido
    # 2: vocal interna del primer apellido
    # 3: inicial del segundo apellido
    # 4: inicial del nombre
    #
    # X se acepta en la posición 2 para casos especiales.
    # --------------------------------------------------------
    if not re.fullmatch(r"[A-Z][AEIOUX][A-Z]{2}", curp[:4]):
        return (
            False,
            "Los primeros 4 caracteres no cumplen la estructura de una CURP.",
        )

    # --------------------------------------------------------
    # POSICIONES 5-10 = AAMMDD
    # --------------------------------------------------------
    birth = curp[4:10]

    if not birth.isdigit():
        return (
            False,
            "La fecha de nacimiento de la CURP debe contener solo números.",
        )

    yy = int(birth[0:2])
    mm = int(birth[2:4])
    dd = int(birth[4:6])

    # La posición 17 permite distinguir el siglo en el esquema
    # tradicional: número para 1900 y letra para 2000.
    century_char = curp[16]

    if century_char.isdigit():
        full_year = 1900 + yy
    else:
        full_year = 2000 + yy

    try:
        parsed_birth = date(full_year, mm, dd)
    except ValueError:
        return (
            False,
            "La fecha AAMMDD contenida en la CURP no es válida.",
        )

    if parsed_birth > date.today():
        return (
            False,
            "La fecha de nacimiento contenida en la CURP está en el futuro.",
        )

    # --------------------------------------------------------
    # POSICIÓN 11 = SEXO
    # --------------------------------------------------------
    if curp[10] not in {"H", "M"}:
        return (
            False,
            "La posición de sexo de la CURP debe ser H o M.",
        )

    # --------------------------------------------------------
    # POSICIONES 12-13 = ENTIDAD
    # --------------------------------------------------------
    entity = curp[11:13]

    if entity not in CURP_ENTITY_CODES:
        return (
            False,
            f"La clave de entidad '{entity}' no es válida para una CURP.",
        )

    # --------------------------------------------------------
    # POSICIONES 14-16 = CONSONANTES INTERNAS
    # --------------------------------------------------------
    internal = curp[13:16]

    if not _CURP_INTERNAL_CONSONANTS_RE.fullmatch(internal):
        return (
            False,
            "Las posiciones 14 a 16 deben contener consonantes válidas.",
        )

    # --------------------------------------------------------
    # POSICIÓN 17 = DIFERENCIADOR
    # --------------------------------------------------------
    if not re.fullmatch(r"[A-Z0-9]", curp[16]):
        return (
            False,
            "El carácter diferenciador de la CURP no es válido.",
        )

    # --------------------------------------------------------
    # POSICIÓN 18 = DÍGITO VERIFICADOR
    # --------------------------------------------------------
    if not curp[17].isdigit():
        return (
            False,
            "El último carácter de la CURP debe ser un número.",
        )

    # Algoritmo oficial de dígito verificador:
    # se calculan las primeras 17 posiciones y se compara
    # contra la posición 18.
    verification_chars = (
        "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
    )

    verification_sum = 0

    for index, char in enumerate(curp[:17]):
        try:
            value = verification_chars.index(char)
        except ValueError:
            return (
                False,
                "La CURP contiene un carácter no válido.",
            )

        verification_sum += value * (18 - index)

    expected_digit = (
        10 - (verification_sum % 10)
    ) % 10

    if int(curp[17]) != expected_digit:
        return (
            False,
            "El dígito verificador de la CURP no coincide.",
        )

    return True, None


def is_strict_curp(term: str) -> bool:
    ok, _reason = validate_curp_structure(term)
    return ok


def detect_act_type(text: str) -> str:
    t = normalize_text(text)
    t_nospace = re.sub(r"\s+", "", t)

    # Si es CURP pura, siempre nacimiento
    if re.fullmatch(CURP_REGEX, t_nospace):
        return "NACIMIENTO"

    # Quitar CURPs y cadenas antes de detectar tipo
    t_clean = re.sub(rf"\b{CURP_REGEX}\b", " ", t)
    t_clean = re.sub(rf"\b{NUM20_REGEX}\b", " ", t_clean)
    t_nospace = re.sub(r"\s+", "", t_clean)

    has_folio = any(x in t_nospace for x in ["FOLIO", "FOLIADO", "FOLIADA"])

    if has_folio:
        if any(x in t_nospace for x in [
            "MATRIMONIO", "ACTADEMATRIMONIO", "MATRI", "MATRIFOLIO", "FOLIOMATRIMONIO"
        ]):
            return "MATRIMONIO FOLIO"

        if any(x in t_nospace for x in [
            "DEFUNCION", "ACTADEDEFUNCION", "DEFUN", "DEFUNFOLIO", "FOLIODEFUNCION"
        ]):
            return "DEFUNCION FOLIO"

        if any(x in t_nospace for x in [
            "DIVORCIO", "ACTADEDIVORCIO", "DIVOR", "DIVORFOLIO", "FOLIODIVORCIO"
        ]):
            return "DIVORCIO FOLIO"

        if any(x in t_nospace for x in [
            "NACIMIENTO", "ACTADENACIMIENTO", "NACIM", "NACIMFOLIO", "FOLIONACIMIENTO"
        ]):
            return "NACIMIENTO FOLIO"

        # Si viene cadena + folio sin tipo específico,
        # no lo fuerces a NACIMIENTO FOLIO.
        if re.search(rf"\b{NUM20_REGEX}\b", t):
            return "FOLIO"

        return "NACIMIENTO FOLIO"

    if any(x in t_nospace for x in [
        "MATRIMONIO", "ACTADEMATRIMONIO", "MATRI"
    ]):
        return "MATRIMONIO"

    if any(x in t_nospace for x in [
        "DEFUNCION", "ACTADEDEFUNCION", "DEFUN"
    ]):
        return "DEFUNCION"

    if any(x in t_nospace for x in [
        "DIVORCIO", "ACTADEDIVORCIO", "DIVOR"
    ]):
        return "DIVORCIO"

    if any(x in t_nospace for x in [
        "NACIMIENTO", "ACTADENACIMIENTO", "NACIM"
    ]):
        return "NACIMIENTO"

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
        "FOLIO": "folio",
    }
    if act_type not in mapping:
        raise RuntimeError(
            f"UNSUPPORTED_PROVIDER_ACT_TYPE:{act_type}"
        )

    return mapping[act_type]


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
        r"FOLIADO",
        r"FOLIADA",
        r"FOLIO",
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



def _explicit_act_type_from_line(line: str) -> str | None:
    """
    Devuelve tipo solamente si la línea contiene una indicación explícita
    de tipo de acta.

    IMPORTANTE:
    una CURP sola NO cambia el tipo actual a NACIMIENTO.
    """
    t = normalize_text(line)

    # Quitar identificadores para buscar únicamente palabras de tipo.
    t_clean = re.sub(rf"\b{CURP_REGEX}\b", " ", t)
    t_clean = re.sub(rf"\b{NUM20_REGEX}\b", " ", t_clean)
    t_nospace = re.sub(r"\s+", "", t_clean)

    has_folio = any(
        x in t_nospace
        for x in ("FOLIO", "FOLIADO", "FOLIADA")
    )

    has_specific_type = any(
        x in t_nospace
        for x in (
            "MATRIMONIO",
            "ACTADEMATRIMONIO",
            "MATRI",
            "DEFUNCION",
            "ACTADEDEFUNCION",
            "DEFUN",
            "DIVORCIO",
            "ACTADEDIVORCIO",
            "DIVOR",
            "NACIMIENTO",
            "ACTADENACIMIENTO",
            "NACIM",
        )
    )

    if not has_folio and not has_specific_type:
        return None

    # FOLIO solo, sin tipo concreto ni identificador:
    # guardar un marcador genérico para decidir posteriormente
    # entre FOLIO (cadena) y NACIMIENTO FOLIO (CURP).
    has_identifier = bool(
        re.search(rf"\b{CURP_REGEX}\b", t)
        or re.search(rf"\b{NUM20_REGEX}\b", t)
    )

    if has_folio and not has_specific_type and not has_identifier:
        return "__FOLIO_GENERIC__"

    return detect_act_type(line)


def extract_typed_request_terms(text: str) -> list[tuple[str, str]]:
    """
    Extrae identificadores junto con su tipo de acta.

    Formatos soportados:
        TIPO
        CURP

        CURP TIPO

        TIPO CURP

        CURP
        TIPO

    También conserva contexto para lotes como:
        NACIMIENTO
        CURP1
        DEFUNCION
        CURP2

    Reglas:
    - CURP sin tipo explícito => NACIMIENTO.
    - Cadena de 20 dígitos sin tipo explícito:
        1 => NACIMIENTO
        2 => DEFUNCION
        3 => MATRIMONIO
        4 => DIVORCIO
    - Un tipo explícito siempre tiene prioridad.
    - Si una línea contiene solamente un tipo y la línea anterior contenía
      identificadores clasificados implícitamente, ese tipo se aplica
      retroactivamente a esos identificadores.
    """

    def resolve_type(raw_type: str, term: str) -> str:
        if raw_type == "__FOLIO_GENERIC__":
            if len(term) == 20 and term.isdigit():
                return "FOLIO"
            return "NACIMIENTO FOLIO"

        return raw_type

    chain_type_by_prefix = {
        "1": "NACIMIENTO",
        "2": "DEFUNCION",
        "3": "MATRIMONIO",
        "4": "DIVORCIO",
    }

    current_type = "NACIMIENTO"
    current_type_is_explicit = False

    # Se usa metadata interna para poder corregir el tipo de la línea
    # inmediatamente anterior cuando el cliente escribe:
    #
    # CURP
    # MATRIMONIO
    items: list[dict] = []

    # Identificadores consecutivos que todavía tienen
    # tipo implícito.
    #
    # Permite interpretar correctamente:
    #
    # CURP1
    # CURP2
    # CURP3
    # DEFUNCION FOLIO
    #
    # aplicando DEFUNCION FOLIO a todo el bloque.
    pending_implicit_indices: list[int] = []

    lines = [
        line.strip()
        for line in (text or "").splitlines()
        if line.strip()
    ]

    for line in lines:
        explicit_type = _explicit_act_type_from_line(line)
        identifiers = extract_request_terms(line)

        # ---------------------------------------------------------
        # TIPO DESPUÉS DEL IDENTIFICADOR
        #
        # Ejemplo:
        # PECF660614HCLRLR09
        # MATRIMONIO
        #
        # Solo corregimos retroactivamente elementos cuyo tipo era
        # implícito. Esto protege lotes como:
        #
        # NACIMIENTO
        # CURP1
        # DEFUNCION
        # CURP2
        #
        # CURP1 ya tenía NACIMIENTO explícito, por lo que DEFUNCION
        # será contexto para CURP2 y no modificará CURP1.
        # ---------------------------------------------------------
        if (
            explicit_type
            and not identifiers
            and pending_implicit_indices
        ):
            for idx in pending_implicit_indices:
                if not items[idx]["explicit"]:
                    items[idx]["act_type"] = resolve_type(
                        explicit_type,
                        items[idx]["term"],
                    )
                    items[idx]["explicit"] = True

            pending_implicit_indices = []

        # Un tipo explícito también se convierte en contexto para
        # identificadores posteriores.
        if explicit_type:
            current_type = explicit_type
            current_type_is_explicit = True

        current_line_indices: list[int] = []

        for term in identifiers:
            term = (term or "").strip().upper()
            if not term:
                continue

            # Tipo escrito en la misma línea:
            # CURP MATRIMONIO / MATRIMONIO CURP
            if explicit_type:
                term_type = resolve_type(explicit_type, term)
                source_is_explicit = True

            else:
                term_type = resolve_type(current_type, term)
                source_is_explicit = current_type_is_explicit

                # Cadena sola: inferir tipo por primer dígito únicamente
                # cuando no hay un tipo explícito gobernando el contexto.
                if (
                    len(term) == 20
                    and term.isdigit()
                    and not current_type_is_explicit
                ):
                    term_type = chain_type_by_prefix.get(
                        term[0],
                        term_type,
                    )

            items.append(
                {
                    "term": term,
                    "act_type": term_type,
                    "explicit": source_is_explicit,
                }
            )
            current_line_indices.append(len(items) - 1)

        # Una línea con tipo explícito + identificador marca
        # una nueva frontera y no debe modificar bloques anteriores.
        if explicit_type and identifiers:
            pending_implicit_indices = []

        # Acumular solamente identificadores cuyo tipo sigue
        # siendo implícito. Así un tipo escrito al final puede
        # aplicarse a todo el bloque consecutivo.
        for idx in current_line_indices:
            if not items[idx]["explicit"]:
                pending_implicit_indices.append(idx)

    # Deduplicar por IDENTIFICADOR + TIPO.
    # La misma CURP puede solicitarse legítimamente con dos tipos distintos.
    found: list[tuple[str, str]] = []

    for item in items:
        pair = (item["term"], item["act_type"])

        if pair not in found:
            found.append(pair)

    return found


def extract_request_terms(text: str) -> list[str]:
    text = text or ""
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    if not lines:
        lines = [text.strip()] if text.strip() else []

    found = []

    for line in lines:
        cleaned = _remove_type_words(line)

        curp_candidates = re.findall(
            r"(?<![A-Z0-9])([A-Z0-9]{18})(?![A-Z0-9])",
            cleaned,
        )

        curps = [
            token
            for token in curp_candidates
            if is_strict_curp(token)
        ]

        nums20 = re.findall(rf"\b({NUM20_REGEX})\b", cleaned)

        for term in curps + nums20:
            if term not in found:
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
    name = name.replace(".PDF", " ")
    name = name.replace("_", " ")
    name = name.replace("-", " ")
    name = " ".join(name.split())

    m = re.search(rf"(?<![A-Z0-9])({CURP_REGEX})(?![A-Z0-9])", name)
    if m:
        return m.group(1)

    m = re.search(rf"(?<!\d)({NUM20_REGEX})(?!\d)", name)
    if m:
        return m.group(1)

    return None


def _strip_mentions_and_phones(text: str) -> str:
    t = text or ""

    # Quitar menciones tipo @52283408707598
    t = re.sub(r"@\d{8,20}", " ", t)

    # Quitar teléfonos comunes con +, espacios o guiones
    t = re.sub(r"\+?\d[\d\s\-()]{7,20}\d", " ", t)

    return t


def seems_like_identifier_attempt(text: str) -> bool:
    raw = text or ""
    raw_clean = _strip_mentions_and_phones(raw)

    t = normalize_text(raw_clean)

    keywords = [
        "CURP",
        "IDENTIFICADOR",
        "IDENTIFICADOR ELECTRONICO",
        "CODIGO",
        "CODIGO DE VERIFICACION",
        "VERIFICACION",
        "NACIMIENTO",
        "MATRIMONIO",
        "DEFUNCION",
        "DIVORCIO",
        "FOLIO",
    ]

    if any(k in t for k in keywords):
        return True

    compact = re.sub(r"\s+", "", t)
    if re.fullmatch(r"\d{8,25}", compact):
        return True

    if re.search(r"\b[A-Z]{2,5}\d{4,}[HM][A-Z0-9]{4,}\b", t):
        return True

    # Intentos de CURP / clave de elector / identificadores
    # alfanuméricos de longitud similar.
    candidate_tokens = re.findall(
        r"(?<![A-Z0-9])([A-Z0-9]{16,20})(?![A-Z0-9])",
        t,
    )

    for token in candidate_tokens:
        if (
            re.search(r"[A-Z]", token)
            and re.search(r"\d", token)
        ):
            return True

    return False


def detect_identifier_problem(text: str) -> str | None:
    text = _strip_mentions_and_phones(text)
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
                "⚠️ CURP incorrecta o incompleta.\n"
                "Verifica que tenga 18 caracteres y que no haya confusiones "
                "entre letras y números (O/0, I/1, E/3, A/4, etc.)."
            )

        curp_ok, curp_reason = validate_curp_structure(token)

        if not curp_ok:
            return (
                "⚠️ CURP incorrecta o incompleta.\n"
                "Verifica que tenga 18 caracteres y que no haya confusiones "
                "entre letras y números (O/0, I/1, E/3, A/4, etc.)."
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
        if re.fullmatch(NUM20_REGEX, token):
            continue

        has_letters = bool(re.search(r"[A-Z]", token))
        has_digits = bool(re.search(r"\d", token))

        if not (has_letters and has_digits):
            continue

        if is_strict_curp(token):
            continue

        curp_ok, curp_reason = validate_curp_structure(token)

        return (
            "⚠️ CURP incorrecta o incompleta.\n"
            "Verifica que tenga 18 caracteres y que no haya confusiones "
            "entre letras y números (O/0, I/1, E/3, A/4, etc.)."
        )

    return None
