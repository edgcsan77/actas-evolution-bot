from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Float
from sqlalchemy.sql import func

from app.db import Base


class AuthorizedUser(Base):
    __tablename__ = "authorized_users"

    id = Column(Integer, primary_key=True)
    wa_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(150), nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class AuthorizedGroup(Base):
    __tablename__ = "authorized_groups"

    id = Column(Integer, primary_key=True)
    group_jid = Column(String(120), unique=True, nullable=False, index=True)
    group_name = Column(String(150), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    owner_instance = Column(String(50), nullable=True, index=True)
    is_hidden = Column(Boolean, default=False, nullable=False)
    hidden_in_main = Column(Boolean, default=False, nullable=False)


class ProviderSetting(Base):
    __tablename__ = "provider_settings"

    id = Column(Integer, primary_key=True, index=True)
    provider_name = Column(String, unique=True, index=True, nullable=False)
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(String, primary_key=True, index=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


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
    instance_name = Column(String(50), nullable=True, index=True)

    evolution_message_id = Column(String(120), nullable=True)

    provider_name = Column(String(30), nullable=True, index=True)
    provider_group_id = Column(String(120), nullable=True, index=True)
    provider_message = Column(Text, nullable=True)
    provider_message_id = Column(String(120), nullable=True, index=True)
    provider_media_url = Column(Text, nullable=True)

    provider_to_webhook_lag_s = Column(Float, nullable=True)
    t_total_provider1_relay = Column(Float, nullable=True)
    provider_processing_time = Column(Float, nullable=True)
    total_delivery_time = Column(Float, nullable=True)

    pdf_url = Column(Text, nullable=True)

    status = Column(String(20), default="QUEUED", nullable=False, index=True)
    error_message = Column(Text, nullable=True)

    resent_from_history = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), index=True)
    expires_at = Column(DateTime, nullable=False)


class GroupAlias(Base):
    __tablename__ = "group_aliases"

    id = Column(Integer, primary_key=True)
    group_jid = Column(String(120), unique=True, nullable=False, index=True)
    custom_name = Column(String(255), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    owner_instance = Column(String(50), nullable=True, index=True)
    is_hidden = Column(Boolean, default=False, nullable=False)
    hidden_in_main = Column(Boolean, default=False, nullable=False)


class GroupCategory(Base):
    __tablename__ = "group_categories"

    id = Column(Integer, primary_key=True, index=True)
    group_jid = Column(String, unique=True, index=True, nullable=False)
    category = Column(String, nullable=False, default="otro")
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    owner_instance = Column(String(50), nullable=True, index=True)


class GroupPromotion(Base):
    __tablename__ = "group_promotions"

    id = Column(Integer, primary_key=True)
    group_jid = Column(String(120), unique=True, nullable=False, index=True)
    promo_name = Column(String(150), nullable=True)

    total_actas = Column(Integer, nullable=False, default=0)
    used_actas = Column(Integer, nullable=False, default=0)
    
    price_per_piece = Column(String(30), nullable=True)

    is_credit = Column(Boolean, default=False, nullable=False)
    credit_abono = Column(Integer, nullable=False, default=0)
    credit_debe = Column(Integer, nullable=False, default=0)

    client_key = Column(String(255), nullable=True, index=True)
    shared_key = Column(String(255), nullable=True, index=True)

    shared_group_limit_actas = Column(Integer, nullable=True)
    shared_group_used_actas = Column(Integer, nullable=False, default=0, server_default="0")

    warning_sent_200 = Column(Boolean, default=False, nullable=False)
    warning_sent_100 = Column(Boolean, default=False, nullable=False)
    warning_sent_50 = Column(Boolean, default=False, nullable=False)
    warning_sent_10 = Column(Boolean, default=False, nullable=False)
    warning_sent_0 = Column(Boolean, default=False, nullable=False)
    owner_instance = Column(String(50), nullable=True, index=True)

    is_active = Column(Boolean, default=True, nullable=False)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class BotControl(Base):
    __tablename__ = "bot_control"

    id = Column(Integer, primary_key=True)

    instance_name = Column(String(50), unique=True, nullable=False, index=True)

    limit = Column(Integer, nullable=False, default=0)
    used = Column(Integer, nullable=False, default=0)

    recharges = Column(Integer, nullable=False, default=0)

    is_blocked = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
