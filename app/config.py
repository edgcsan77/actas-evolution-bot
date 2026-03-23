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

    PROVIDER_API_URL: str = ""
    PROVIDER_API_TOKEN: str = ""
    ADMIN_PHONE: str = ""

    HISTORY_DAYS: int = 7
    REQUEST_TIMEOUT_MINUTES: int = 10


settings = Settings()
