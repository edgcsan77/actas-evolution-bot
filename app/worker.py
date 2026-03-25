from datetime import datetime
from app.db import SessionLocal
from app.models import RequestLog, ProviderSetting
from app.services.evolution import send_group_text
from app.config import settings
from app.utils.curp import provider_label_for_type
from app.utils.provider_format import provider2_command


def _get_or_create_provider(db, provider_name: str, default_enabled: bool):
    row = db.query(ProviderSetting).filter(ProviderSetting.provider_name == provider_name).first()
    if row:
        return row

    row = ProviderSetting(
        provider_name=provider_name,
        is_enabled=default_enabled,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _enabled_providers(db) -> list[str]:
    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)

    enabled = []
    if p1.is_enabled:
        enabled.append("PROVIDER1")
    if p2.is_enabled:
        enabled.append("PROVIDER2")

    return enabled


def _pick_provider_name(db, request_id: int) -> str:
    enabled = _enabled_providers(db)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    if len(enabled) == 1:
        return enabled[0]

    # round robin simple cuando ambos están activos
    idx = (request_id - 1) % len(enabled)
    return enabled[idx]


def _pick_provider1_group(act_type: str, request_id: int) -> str:
    act_type = (act_type or "").upper().strip()

    nacimiento_groups = [
        settings.PROVIDER_GROUP_NACIMIENTO_1,
        settings.PROVIDER_GROUP_NACIMIENTO_2,
        settings.PROVIDER_GROUP_NACIMIENTO_3,
    ]
    nacimiento_groups = [g for g in nacimiento_groups if g]

    especiales_group = settings.PROVIDER_GROUP_ESPECIALES

    if act_type == "NACIMIENTO":
        if not nacimiento_groups:
            raise RuntimeError("NO_BIRTH_PROVIDER_GROUPS_CONFIGURED")
        idx = (request_id - 1) % len(nacimiento_groups)
        return nacimiento_groups[idx]

    if not especiales_group:
        raise RuntimeError("NO_SPECIAL_PROVIDER_GROUP_CONFIGURED")

    return especiales_group


def _pick_provider_group(provider_name: str, act_type: str, request_id: int) -> str:
    if provider_name == "PROVIDER1":
        return _pick_provider1_group(act_type, request_id)

    if provider_name == "PROVIDER2":
        provider2_groups = [
            settings.PROVIDER2_GROUP_1,
            settings.PROVIDER2_GROUP_2,
        ]
        provider2_groups = [g for g in provider2_groups if g]

        if not provider2_groups:
            raise RuntimeError("PROVIDER2_GROUPS_NOT_CONFIGURED")

        idx = (request_id - 1) % len(provider2_groups)
        return provider2_groups[idx]

    raise RuntimeError("UNKNOWN_PROVIDER")


def _build_provider_message(provider_name: str, term: str, act_type: str) -> str:
    if provider_name == "PROVIDER1":
        provider_type = provider_label_for_type(act_type)
        return f"{term} {provider_type}"

    if provider_name == "PROVIDER2":
        return provider2_command(term, act_type)

    raise RuntimeError("UNKNOWN_PROVIDER")


def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        req.status = "PROCESSING"
        req.updated_at = datetime.utcnow()

        provider_name = _pick_provider_name(db, req.id)
        provider_group_id = _pick_provider_group(provider_name, req.act_type, req.id)
        text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

        req.provider_name = provider_name
        req.provider_group_id = provider_group_id
        req.provider_message = text_to_provider
        db.commit()

        print("WORKER_PROVIDER_NAME =", provider_name, flush=True)
        print("WORKER_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
        print("WORKER_TEXT_TO_PROVIDER =", text_to_provider, flush=True)

        send_group_text(provider_group_id, text_to_provider)

    except Exception as e:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if req:
            req.status = "ERROR"
            req.error_message = str(e)
            req.updated_at = datetime.utcnow()
            db.commit()
        raise
    finally:
        db.close()
