def _clean_requester(value: str | None) -> str:
    value = str(value or "").strip()
    return value or "Usuario"


def _clean_act_type(value: str | None) -> str:
    value = str(value or "ACTA").strip().upper()

    aliases = {
        "NAC": "NACIMIENTO",
        "NACIMIENTO": "NACIMIENTO",
        "MAT": "MATRIMONIO",
        "MATRIMONIO": "MATRIMONIO",
        "DIV": "DIVORCIO",
        "DIVORCIO": "DIVORCIO",
        "DEF": "DEFUNCIÓN",
        "DEFUNCION": "DEFUNCIÓN",
        "DEFUNCIÓN": "DEFUNCIÓN",
        "FOLIO": "FOLIO",
        "FOLIADO": "FOLIO",
    }

    return aliases.get(value, value)


def received_message(
    *,
    act_type: str | None,
    requester: str | None,
    count: int = 1,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1

    received = "recibida" if count == 1 else "recibidas"
    processing = (
        "Se está procesando."
        if count == 1
        else "Se están procesando."
    )

    return (
        f"🔎 {act_type} · {count} {received}\n"
        f"👤 {requester}\n"
        f"⏳ {processing}"
    )


def duplicate_processing_message(
    *,
    act_type: str | None,
    requester: str | None,
    count: int = 1,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1

    return (
        f"⏳ {act_type} · {count} en proceso\n"
        f"👤 {requester}\n"
        "No es necesario volver a enviarla."
    )


def no_record_message(
    *,
    act_type: str | None,
    requester: str | None,
    count: int = 1,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    try:
        count = max(int(count or 1), 1)
    except Exception:
        count = 1

    state = (
        "no localizada"
        if count == 1
        else "no localizadas"
    )

    return (
        f"⚠️ {act_type} · {count} {state}\n"
        f"👤 {requester}\n\n"
        "No hay registros disponibles.\n"
        "Verifica que la CURP esté certificada en RENAPO."
    )


def service_unavailable_message(
    *,
    requester: str | None,
    detail: str | None = None,
) -> str:
    requester = _clean_requester(requester)

    detail = (
        str(detail or "").strip()
        or (
            "No fue posible procesar la solicitud "
            "en este momento.\n"
            "Intenta nuevamente más tarde."
        )
    )

    return (
        "⚠️ Servicio temporalmente no disponible\n"
        f"👤 {requester}\n\n"
        f"{detail}"
    )


def processing_error_message(
    *,
    requester: str | None,
    detail: str | None = None,
) -> str:
    requester = _clean_requester(requester)

    detail = (
        str(detail or "").strip()
        or "Intenta nuevamente."
    )

    return (
        "⚠️ No fue posible completar la solicitud\n"
        f"👤 {requester}\n\n"
        f"{detail}"
    )


def blocked_message(
    *,
    requester: str | None,
    detail: str,
) -> str:
    requester = _clean_requester(requester)

    return (
        "⚠️ Servicio no disponible\n"
        f"👤 {requester}\n\n"
        f"{str(detail or '').strip()}"
    )


def already_delivered_message(
    *,
    act_type: str | None,
    requester: str | None,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    return (
        f"✅ {act_type} · ya entregada\n"
        f"👤 {requester}\n"
        "No es necesario volver a enviarla."
    )


def attempt_limit_message(
    *,
    act_type: str | None,
    requester: str | None,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    return (
        f"⚠️ {act_type} · límite de intentos alcanzado\n"
        f"👤 {requester}\n\n"
        "Intenta nuevamente más tarde."
    )


def provider_busy_message(
    *,
    act_type: str | None,
    requester: str | None,
) -> str:
    act_type = _clean_act_type(act_type)
    requester = _clean_requester(requester)

    return (
        f"⏳ {act_type} · servicio saturado\n"
        f"👤 {requester}\n\n"
        "Intenta nuevamente en unos minutos."
    )
