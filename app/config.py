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

    # PROVIDER 1
    PROVIDER_GROUP_NACIMIENTO_1: str = ""
    PROVIDER_GROUP_NACIMIENTO_2: str = ""
    PROVIDER_GROUP_NACIMIENTO_3: str = ""
    PROVIDER_GROUP_ESPECIALES: str = ""

    # PROVIDER 2
    PROVIDER2_GROUP_1: str = ""
    PROVIDER2_GROUP_2: str = ""

    # PROVIDER 3
    PROVIDER3_BASE_URL: str = ""
    PROVIDER3_EMAIL: str = ""
    PROVIDER3_PASSWORD: str = ""
    PROVIDER3_PHPSESSID: str = ""
    PROVIDER3_KEEPALIVE_SECRET: str = ""
    PROVIDER3_TIMEOUT_LOGIN: int = 60
    PROVIDER3_TIMEOUT_GENERATE: int = 120

    PROVIDER_NO_RECORD_TEXT: str = (
        "NO HAY REGISTROS DISPONIBLES|"
        "NO SE ENCONTRO EL ACTA EN SISTEMA|"
        "NO SE ENCONTRÓ EL ACTA EN SISTEMA|"
        "ACTA NO ENCONTRADA|"
        "SIN REGISTRO"
    )

    HISTORY_DAYS: int = 7
    REQUEST_TIMEOUT_MINUTES: int = 3


settings = Settings()
