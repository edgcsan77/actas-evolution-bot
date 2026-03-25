from app.utils.curp import normalize_text, is_chain


def provider2_command(term: str, act_type: str) -> str:
    """
    PROVEEDOR 2:
    CURP:
      1 Nacimiento marco/reverso
      2 Matrimonio marco/reverso
      3 Defunción marco/reverso
      4 Divorcio marco/reverso
      5 Nacimiento marco/reverso/folio
      6 Matrimonio marco/reverso/folio
      7 Defunción marco/reverso/folio
      8 Divorcio marco/reverso/folio

    CADENA:
      1 Marco y reverso
      2 Marco, reverso y folio
    """
    act_type = normalize_text(act_type)

    if is_chain(term):
        if "FOLIO" in act_type:
            return f"{term} 2"
        return f"{term} 1"

    mapping = {
        "NACIMIENTO": "1",
        "MATRIMONIO": "2",
        "DEFUNCION": "3",
        "DIVORCIO": "4",
        "NACIMIENTO FOLIO": "5",
        "MATRIMONIO FOLIO": "6",
        "DEFUNCION FOLIO": "7",
        "DIVORCIO FOLIO": "8",
    }

    cmd = mapping.get(act_type, "1")
    return f"{term} {cmd}"
