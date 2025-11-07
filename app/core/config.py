from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/data_mapper"
    debug: bool = True
    date_default_dayfirst: bool = False
    log_level: str = "INFO"
    map_stage_timeout_seconds: int = 600
    b2_max_retries: int = 3
    
    # Authentication
    secret_key: str = "your-secret-key-change-in-production"
    
    # Backblaze B2 Configuration
    b2_application_key_id: str = ""
    b2_application_key: str = ""
    b2_bucket_name: str = ""

    # LangChain API Keys
    anthropic_api_key: str = ""
    google_api_key: str = ""  # For future Gemini support

    # Fixture controls
    enable_marketing_fixture_shortcuts: bool = True
    enable_auto_retry_failed_imports: bool = True

    model_config = ConfigDict(env_file=".env")


settings = Settings()
