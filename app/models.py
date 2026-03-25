from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from app.db import Base


class AuthorizedUser(Base):
    __tablename__ = "authorized_users"

    id = Column(Integer, primary_key=True)
    wa_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(150), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class AuthorizedGroup(Base):
    __tablename__ = "authorized_groups"

    id = Column(Integer, primary_key=True)
    group_jid = Column(String(120), unique=True, nullable=False, index=True)
    group_name = Column(String(150), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class RequestLog(Base):
    __tablename__ = "request_logs"

    id = Column(Integer, primary_key=True)
    request_key = Column(String(150), unique=True, nullable=False, index=True)

    curp = Column(String(40), nullable=False, index=True)
    act_type = Column(String(30), nullable=False, index=True)

    requester_wa_id = Column(String(50), nullable=False, index=True)
    requester_name = Column(String(150), nullable=True)

    source_chat_id = Column(String(120), nullable=False, index=True)
    source_group_id = Column(String(120), nullable=True, index=True)

    evolution_message_id = Column(String(120), nullable=True)

    provider_name = Column(String(30), nullable=True, index=True)   # PROVIDER1 / PROVIDER2
    provider_group_id = Column(String(120), nullable=True, index=True)
    provider_message = Column(Text, nullable=True)
    provider_media_url = Column(Text, nullable=True)

    pdf_url = Column(Text, nullable=True)

    status = Column(String(20), default="QUEUED", index=True)
    error_message = Column(Text, nullable=True)

    resent_from_history = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, index=True)
    expires_at = Column(DateTime, nullable=False)


class ProviderSetting(Base):
    __tablename__ = "provider_settings"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(30), unique=True, nullable=False, index=True)  # PROVIDER1 / PROVIDER2
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
