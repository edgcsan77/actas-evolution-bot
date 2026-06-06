import re
from datetime import datetime, timezone, timedelta

import boto3
from botocore.client import Config

from app.config import settings


def _utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_filename(value: str) -> str:
    value = (value or "").strip()
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = value.strip("._-")
    return value[:180] or "archivo.pdf"


def _r2_client():
    endpoint = (settings.R2_ENDPOINT or "").strip()

    if not endpoint and settings.R2_ACCOUNT_ID:
        endpoint = f"https://{settings.R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

    if not endpoint:
        raise RuntimeError("R2_ENDPOINT_NOT_CONFIGURED")

    if not settings.R2_ACCESS_KEY_ID or not settings.R2_SECRET_ACCESS_KEY:
        raise RuntimeError("R2_KEYS_NOT_CONFIGURED")

    if not settings.R2_BUCKET:
        raise RuntimeError("R2_BUCKET_NOT_CONFIGURED")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name=settings.R2_REGION or "auto",
        config=Config(signature_version="s3v4"),
    )


def save_request_pdf_to_r2(req, db, pdf_bytes: bytes, filename: str | None = None, origin: str = "") -> str:
    if not pdf_bytes:
        return ""

    now = _utc_now_naive()

    curp = _safe_filename(getattr(req, "curp", "") or "DATO")
    act_type = _safe_filename(getattr(req, "act_type", "") or "ACTA")
    provider = _safe_filename(getattr(req, "provider_name", "") or "PROVIDER")
    final_filename = _safe_filename(filename or f"{curp}.pdf")

    yyyy = now.strftime("%Y")
    mm = now.strftime("%m")
    dd = now.strftime("%d")

    key = f"pdfs/{yyyy}/{mm}/{dd}/{req.id}_{provider}_{act_type}_{final_filename}"

    client = _r2_client()

    client.put_object(
        Bucket=settings.R2_BUCKET,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        Metadata={
            "request_id": str(getattr(req, "id", "") or ""),
            "curp": str(getattr(req, "curp", "") or "")[:40],
            "act_type": str(getattr(req, "act_type", "") or "")[:30],
            "provider": str(getattr(req, "provider_name", "") or "")[:30],
            "origin": str(origin or "")[:80],
        },
    )

    req.pdf_storage_key = key
    req.pdf_filename = final_filename
    req.pdf_saved_at = now
    req.pdf_expires_at = now + timedelta(days=int(settings.PDF_RETENTION_DAYS or 30))

    db.commit()

    print("R2_PDF_SAVED =", {
        "req_id": getattr(req, "id", None),
        "key": key,
        "filename": final_filename,
        "origin": origin,
    }, flush=True)

    return key


def generate_r2_presigned_download_url(storage_key: str, filename: str | None = None, expires_sec: int = 300) -> str:
    if not storage_key:
        raise RuntimeError("R2_STORAGE_KEY_EMPTY")

    client = _r2_client()

    params = {
        "Bucket": settings.R2_BUCKET,
        "Key": storage_key,
    }

    if filename:
        safe_name = _safe_filename(filename)
        params["ResponseContentDisposition"] = f'attachment; filename="{safe_name}"'

    return client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=int(expires_sec or 300),
    )


def delete_pdf_from_r2(storage_key: str):
    if not storage_key:
        return

    client = _r2_client()

    client.delete_object(
        Bucket=settings.R2_BUCKET,
        Key=storage_key,
    )

    print("R2_PDF_DELETED =", storage_key, flush=True)
