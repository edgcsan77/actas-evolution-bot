from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "actas-evolution-bot"
    APP_ENV: str = "development"

    DATABASE_URL: str
    REDIS_URL: str

    ADMIN_PANEL_TOKEN: str

    EVOLUTION_BASE_URL: str
    EVOLUTION_API_KEY: str
    EVOLUTION_INSTANCE: str
    EVOLUTION_PROVIDER_INSTANCE: str

    ADMIN_PHONE: str = ""

    SOPORTE_ACTAS_GROUP: str = ""

    # Cloudflare R2
    R2_ACCOUNT_ID: str = ""
    R2_ACCESS_KEY_ID: str = ""
    R2_SECRET_ACCESS_KEY: str = ""
    R2_BUCKET: str = ""
    R2_ENDPOINT: str = ""
    R2_REGION: str = "auto"
    PDF_RETENTION_DAYS: int = 30

    PROVIDER_API_URL: str = ""
    PROVIDER_API_TOKEN: str = ""

    # PROVIDER 1
    PROVIDER_GROUP_NACIMIENTO_1: str = ""
    PROVIDER_GROUP_NACIMIENTO_2: str = ""
    PROVIDER_GROUP_NACIMIENTO_3: str = ""
    PROVIDER_GROUP_NACIMIENTO_4: str = ""
    PROVIDER_GROUP_ESPECIALES: str = ""
    PROVIDER_GROUP_FOLIADAS: str = ""
    PROVIDER_GROUP_CADENA: str = ""

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
    PROVIDER3_TIMEOUT_GENERATE: int = 480

    # PROVIDER 5
    PROVIDER5_GROUP_NACIMIENTO: str = ""
    PROVIDER5_GROUP_ESPECIALES: str = ""

    # PROVIDER 6
    PROVIDER6_GROUP_1_NACIMIENTO: str = ""
    PROVIDER6_GROUP_2_NACIMIENTO: str = ""
    PROVIDER6_GROUP_ESPECIALES: str = ""
    PROVIDER6_GROUP_FOLIADAS: str = ""

    # PROVIDER 7
    PROVIDER7_ACCESS_TOKEN: str = ""
    PROVIDER7_JSESSIONID: str = ""
    PROVIDER7_OFICIALIA: str = ""
    PROVIDER7_RFC_USUARIO: str = ""

    # PROVIDER 8
    PROVIDER8_GROUP_1: str = ""
    PROVIDER8_GROUP_2: str = ""

    # PROVIDER 9
    PROVIDER9_GROUP_1: str = ""
    PROVIDER9_GROUP_2: str = ""

    # PROVIDER 12
    PROVIDER12_GROUP_NACIMIENTO: str = ""
    PROVIDER12_GROUP_ESPECIALES: str = ""

    # PROVIDER 13
    PROVIDER13_GROUP_NACIMIENTO_1: str = ""
    PROVIDER13_GROUP_FOLIO: str = ""
    PROVIDER13_GROUP_NACIMIENTO_2: str = ""
    PROVIDER13_GROUP_CADENA: str = ""
    PROVIDER13_GROUP_ESPECIALES: str = ""

    # PROVIDER 14
    PROVIDER14_PRIVATE_JID: str = ""
    PROVIDER14_STEP_DELAY_SECONDS: float = 2.0

    # PROVIDER 15
    PROVIDER15_USERNAME: str = ""
    PROVIDER15_PASSWORD: str = ""
    PROVIDER15_NODE_ENABLED: bool = False

    # MAYAPROVIDER
    MAYAPROVIDER_GROUP_1: str = ""
    MAYAPROVIDER_GROUP_2: str = ""

    PROVIDER_NO_RECORD_TEXT: str = (
        "NO HAY REGISTROS DISPONIBLES|"
        "NO SE ENCONTRO EL ACTA EN SISTEMA|"
        "NO SE ENCONTRÓ EL ACTA EN SISTEMA|"
        "ACTA NO ENCONTRADA|"
        "DOCUMENTO NO ENCONTRADO|"
        "SIN REGISTRO|"
        "NO ESTA|"
        "SIN|"
        "ERROR! CURP INVALIDA|"
        "ERROR!|"
        "NO SE HA ENCONTRADO INFORMACION|"
        "LO SIENTO, NO SE HA ENCONTRADO INFORMACION|"
        "NO SE ENCONTRARON REGISTROS"
    )

    HISTORY_DAYS: int = 30
    REQUEST_TIMEOUT_MINUTES: int = 8
    PROCESSING_HARD_TIMEOUT_MINUTES: int = 45
    WEB_REQUEST_TIMEOUT_MINUTES: int = 11


settings = Settings()
