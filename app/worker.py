import base64
from datetime import datetime

from app.db import SessionLocal
from app.models import RequestLog, ProviderSetting, AppSetting
from app.services.evolution import send_group_text, send_document_base64, send_group_document_base64
from app.config import settings
from app.utils.curp import provider_label_for_type, is_chain
from app.utils.provider_format import provider2_command
from app.services.provider3 import Provider3Client, decode_pdf_base64


def provider3_keepalive_job():
    db = SessionLocal()

    try:
        phpsessid = _get_app_setting(
            db,
            "PROVIDER3_PHPSESSID",
            settings.PROVIDER3_PHPSESSID
        )

        if not phpsessid:
            print("KEEPALIVE_SKIP_NO_SID", flush=True)
            return {"ok": False}

        client = Provider3Client(phpsessid=phpsessid)

        result = client.keepalive()

        print("KEEPALIVE_OK", flush=True)

        return {"ok": True}

    except Exception as e:
        print("KEEPALIVE_ERROR", str(e), flush=True)
        return {"ok": False, "error": str(e)}

    finally:
        db.close()
        

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


def _get_app_setting(db, key: str, default: str = "") -> str:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if not row or row.value is None:
        return default
    return row.value.strip()
    

def _enabled_providers(db) -> list[str]:
    p1 = _get_or_create_provider(db, "PROVIDER1", True)
    p2 = _get_or_create_provider(db, "PROVIDER2", False)
    p3 = _get_or_create_provider(db, "PROVIDER3", False)

    enabled = []
    if p1.is_enabled:
        enabled.append("PROVIDER1")
    if p2.is_enabled:
        enabled.append("PROVIDER2")
    if p3.is_enabled:
        enabled.append("PROVIDER3")

    return enabled


def _pick_provider_name(db, request_id: int) -> str:
    enabled = _enabled_providers(db)

    if not enabled:
        raise RuntimeError("NO_PROVIDER_ENABLED")

    if len(enabled) == 1:
        return enabled[0]

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


def _pick_provider_group(provider_name: str, act_type: str, request_id: int) -> str | None:
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

    if provider_name == "PROVIDER3":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _build_provider_message(provider_name: str, term: str, act_type: str) -> str | None:
    if provider_name == "PROVIDER1":
        provider_type = provider_label_for_type(act_type)
        return f"{term} {provider_type}"

    if provider_name == "PROVIDER2":
        return provider2_command(term, act_type)

    if provider_name == "PROVIDER3":
        return None

    raise RuntimeError("UNKNOWN_PROVIDER")


def _provider3_flags(act_type: str) -> dict:
    act_type = (act_type or "").upper().strip()

    is_folio = "FOL" in act_type

    return {
        "folio1": is_folio,
        "folio2": False,
        "reverso": True,
        "margen": True,
    }


def _provider3_tipo_acta(act_type: str) -> str:
    act_type = (act_type or "").upper().strip()

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


def _process_provider3(req, db):
    phpsessid = _get_app_setting(db, "PROVIDER3_PHPSESSID", settings.PROVIDER3_PHPSESSID)

    masked = ""
    if phpsessid:
        if len(phpsessid) <= 8:
            masked = "*" * len(phpsessid)
        else:
            masked = phpsessid[:4] + ("*" * (len(phpsessid) - 8)) + phpsessid[-4:]

    print("PROVIDER3_PHPSESSID_MASKED =", masked, flush=True)

    client = Provider3Client(phpsessid=phpsessid)
    flags = _provider3_flags(req.act_type)

    if is_chain(req.curp):
        result = client.generar_por_cadena(
            cadena=req.curp,
            folio1=flags["folio1"],
            folio2=flags["folio2"],
            reverso=flags["reverso"],
            margen=flags["margen"],
        )
    else:
        tipo_acta = _provider3_tipo_acta(req.act_type)
        result = client.generar_por_curp(
            curp=req.curp,
            tipo_acta=tipo_acta,
            folio1=flags["folio1"],
            folio2=flags["folio2"],
            reverso=flags["reverso"],
            margen=flags["margen"],
        )

    pdf_b64 = result.get("pdf") or ""
    if not pdf_b64:
        raise RuntimeError(f"PROVIDER3_NO_PDF: {result}")

    pdf_bytes = decode_pdf_base64(pdf_b64)

    return {
        "remaining": result.get("remaining"),
        "raw_result": result,
        "pdf_bytes": pdf_bytes,
    }
    

def process_request(request_id: int):
    db = SessionLocal()
    try:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        if not req:
            return

        req.status = "PROCESSING"
        req.updated_at = datetime.utcnow()
        db.commit()

        provider_name = _pick_provider_name(db, req.id)
        provider_group_id = _pick_provider_group(provider_name, req.act_type, req.id)
        text_to_provider = _build_provider_message(provider_name, req.curp, req.act_type)

        req.provider_name = provider_name
        req.provider_group_id = provider_group_id
        req.provider_message = text_to_provider
        req.updated_at = datetime.utcnow()
        db.commit()

        print("WORKER_PROVIDER_NAME =", provider_name, flush=True)
        print("WORKER_PROVIDER_GROUP_ID =", provider_group_id, flush=True)
        print("WORKER_TEXT_TO_PROVIDER =", text_to_provider, flush=True)

        if provider_name in ("PROVIDER1", "PROVIDER2"):
            send_group_text(provider_group_id, text_to_provider)
            return

        if provider_name == "PROVIDER3":
            provider3_result = _process_provider3(req, db)
        
            pdf_bytes = provider3_result["pdf_bytes"]
            safe_media_b64 = base64.b64encode(pdf_bytes).decode()
        
            if req.source_group_id:
                send_group_document_base64(
                    req.source_group_id,
                    safe_media_b64,
                    filename=f"{req.curp}.pdf",
                    caption=""
                )
            else:
                send_document_base64(
                    req.requester_wa_id,
                    safe_media_b64,
                    filename=f"{req.curp}.pdf",
                    caption=""
                )
        
            req.provider_media_url = "BASE64_PROVIDER3"
            req.pdf_url = None
            req.status = "DONE"
            req.error_message = None
            req.updated_at = datetime.utcnow()
            db.commit()
        
            return

        raise RuntimeError("UNKNOWN_PROVIDER")

    except Exception as e:
        req = db.query(RequestLog).filter(RequestLog.id == request_id).first()
        err = str(e)
    
        if req:
            req.updated_at = datetime.utcnow()
    
            if err.startswith("PROVIDER3_NO_RECORD:"):
                req.status = "ERROR"
                req.error_message = err
                db.commit()
            
                msg = (
                    f"❌ No hay registros disponibles.\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Verificar que la CURP esté certificada en RENAPO"
                )
    
                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
                return

            if err.startswith("PROVIDER3_RATE_LIMIT"):
                req.status = "ERROR"
                req.error_message = err
                db.commit()

                msg = (
                    f"⏳ El proveedor está saturado por demasiadas solicitudes.\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Intenta nuevamente en unos minutos"
                )

                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
    
                return

            if err.startswith("PROVIDER3_SESSION_INVALID_OR_EXPIRED:"):
                req.status = "ERROR"
                req.error_message = err
                db.commit()
    
                msg = (
                    f"⚠️ Solicitud sin éxito en Registro Civil\n"
                    f"Dato: {req.curp}\n"
                    f"Tipo: {req.act_type}\n\n"
                    f"Reenviar nuevamente"
                )
    
                if req.source_group_id:
                    send_group_text(req.source_group_id, msg)
                else:
                    from app.services.evolution import send_text
                    send_text(req.requester_wa_id, msg)
    
                return

            req.status = "ERROR"
            req.error_message = err
            db.commit()
        raise
        
    finally:
        db.close()
