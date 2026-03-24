from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "actas-evolution-bot"
    APP_ENV: str = "development"

    DATABASE_URL: str
    REDIS_URL: str

    EVOLUTION_BASE_URL: str
    EVOLUTION_API_KEY: str
    EVOLUTION_INSTANCE: str

    ADMIN_PHONE: str = ""

    PROVIDER_GROUP_NACIMIENTO_1: str = ""
    PROVIDER_GROUP_NACIMIENTO_2: str = ""
    PROVIDER_GROUP_NACIMIENTO_3: str = ""
    PROVIDER_GROUP_ESPECIALES: str = ""

    PROVIDER_NO_RECORD_TEXT: str = "NO SE ENCONTRO EL ACTA EN SISTEMA|NO SE ENCONTRÓ EL ACTA EN SISTEMA|SIN REGISTRO"

    HISTORY_DAYS: int = 7
    REQUEST_TIMEOUT_MINUTES: int = 10


settings = Settings()
