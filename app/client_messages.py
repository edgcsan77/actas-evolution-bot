import re


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
        "NACIMIENTO FOLIO": "NACIMIENTO FOLIO",
        "MAT FOLIO": "MATRIMONIO FOLIO",
        "MATRIMONIO FOLIO": "MATRIMONIO FOLIO",
        "DEF FOLIO": "DEFUNCIÓN FOLIO",
        "DEFUNCION FOLIO": "DEFUNCIÓN FOLIO",
        "DEFUNCIÓN FOLIO": "DEFUNCIÓN FOLIO",
        "DIV FOLIO": "DIVORCIO FOLIO",
        "DIVORCIO FOLIO": "DIVORCIO FOLIO",
        "ACTAS": "ACTAS",
        "SERVICIO": "SERVICIO",
    }

    return aliases.get(value, value)


def _clean_dato(value: str | None) -> str:
    value = str(value or "").strip()
    return value or "SOLICITUD"


def _fallback_instance_label(instance_name: str | None) -> str:
    value = str(instance_name or "").strip()

    if not value:
        return "DOCU EXPRES"

    # Si no existe label en BD, hacer legible el nombre técnico.
    pretty = value

    if pretty.lower().startswith("docifybot8"):
        pretty = pretty[len("docifybot8"):]

    pretty = re.sub(r"[_\-.]+", " ", pretty).strip()

    if not pretty:
        pretty = "DOCIFY"

    return pretty.upper()


def _clean_bot_name(value: str | None) -> str:
    value = str(value or "").strip()

    # El encabezado agrega un solo 🚀.
    while value.startswith("🚀"):
        value = value[1:].strip()

    return value or "DOCU EXPRES"


def resolve_bot_name(
    instance_name: str | None,
    static_labels: dict | None = None,
) -> str:
    """
    Nombre visible al cliente.

    Prioridad:
    1. BotControl.label
    2. static_labels/BOT_LABELS
    3. nombre legible derivado de instance_name
    """
    instance = str(instance_name or "").strip()

    try:
        from app.db import SessionLocal
        from app.models import BotControl

        db = SessionLocal()

        try:
            row = (
                db.query(BotControl)
                .filter(BotControl.instance_name == instance)
                .first()
            )

            if row:
                label = str(getattr(row, "label", "") or "").strip()

                if label:
                    return _clean_bot_name(label)
        finally:
            db.close()

    except Exception:
        pass

    if static_labels and instance:
        label = str(static_labels.get(instance) or "").strip()

        if label:
            return _clean_bot_name(label)

    return _clean_bot_name(
        _fallback_instance_label(instance)
    )


def _message(
    *,
    bot_name: str | None,
    title: str,
    act_type: str | None,
    dato: str | None,
    status: str,
    detail: str | None = None,
) -> str:
    bot_name = _clean_bot_name(bot_name)
    act_type = _clean_act_type(act_type)
    dato = _clean_dato(dato)
    status = str(status or "").strip()
    detail = str(detail or "").strip()

    lines = [
        f"{bot_name}",
        title,
        f"_Tipo_: *{act_type}*",
        f"_Dato_: *{dato}*",
    ]

    if detail:
        lines.append("_Estatus_:")
        lines.append(f"*{status}*")

        for line in detail.splitlines():
            line = line.strip()

            if line:
                lines.append(f"*{line}*")
    else:
        lines.append(f"_Estatus_: *{status}*")

    return "\n".join(lines)


def received_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    count: int = 1,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Nueva solicitud!",
        act_type=act_type,
        dato=dato,
        status="PROCESANDO",
    )


def duplicate_processing_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    count: int = 1,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Estatus de solicitud!",
        act_type=act_type,
        dato=dato,
        status="YA SE ENCUENTRA EN PROCESO",
        detail="No es necesario volver a enviarla.",
    )


def no_record_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    count: int = 1,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Resultado de Busqueda!",
        act_type=act_type,
        dato=dato,
        status="No hay registros disponibles",
        detail="Verifica que la CURP esté certificada en RENAPO",
    )


def service_unavailable_message(
    *,
    requester: str | None = None,
    detail: str | None = None,
    act_type: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Resultado de Busqueda!",
        act_type=act_type,
        dato=dato,
        status="SERVICIO TEMPORALMENTE NO DISPONIBLE",
        detail=detail or "Intenta nuevamente más tarde.",
    )


def processing_error_message(
    *,
    requester: str | None = None,
    detail: str | None = None,
    act_type: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Resultado de Busqueda!",
        act_type=act_type,
        dato=dato,
        status="NO FUE POSIBLE COMPLETAR LA SOLICITUD",
        detail=detail or "Intenta nuevamente en unos minutos.",
    )


def blocked_message(
    *,
    requester: str | None = None,
    detail: str | None = None,
    act_type: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Estatus del servicio!",
        act_type=act_type or "SERVICIO",
        dato=dato or "GRUPO",
        status="SERVICIO PAUSADO",
        detail=detail or (
            "Este grupo tiene un pago pendiente.\n"
            "Contacta al administrador para reactivarlo."
        ),
    )


def already_delivered_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Estatus de solicitud!",
        act_type=act_type,
        dato=dato,
        status="YA FUE ENTREGADA",
        detail="No es necesario volver a enviarla.",
    )


def attempt_limit_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Estatus de solicitud!",
        act_type=act_type,
        dato=dato,
        status="LÍMITE DE INTENTOS ALCANZADO",
        detail="Intenta nuevamente más tarde.",
    )


def provider_busy_message(
    *,
    act_type: str | None,
    requester: str | None = None,
    bot_name: str | None = None,
    dato: str | None = None,
) -> str:
    return _message(
        bot_name=bot_name,
        title="¡Estatus de solicitud!",
        act_type=act_type,
        dato=dato,
        status="SERVICIO SATURADO",
        detail="Intenta nuevamente en unos minutos.",
    )
