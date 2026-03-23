import re

CURP_REGEX = re.compile(
    r"\b([A-Z][AEIOUX][A-Z]{2}\d{2}"
    r"(0[1-9]|1[0-2])"
    r"(0[1-9]|[12]\d|3[01])"
    r"[HM]"
    r"(AS|BC|BS|CC|CL|CM|CS|CH|DF|DG|GT|GR|HG|JC|MC|MN|MS|NT|NL|OC|PL|QT|QR|SP|SL|SR|TC|TS|TL|VZ|YN|ZS|NE)"
    r"[B-DF-HJ-NP-TV-Z]{3}"
    r"[A-Z\d]\d)\b",
    re.IGNORECASE
)

def normalize_text(text: str) -> str:
    return (text or "").upper().strip()

def extract_curps(text: str) -> list[str]:
    text = normalize_text(text)
    matches = CURP_REGEX.findall(text)
    out = []
    for m in matches:
        curp = m[0] if isinstance(m, tuple) else m
        if curp not in out:
            out.append(curp)
    return out

def detect_act_type(text: str) -> str:
    t = normalize_text(text)
    if "MATRIMONIO" in t:
        return "MATRIMONIO"
    if "DEFUNCION" in t or "DEFUNCIÓN" in t:
        return "DEFUNCION"
    return "NACIMIENTO"