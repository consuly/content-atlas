from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/data_mapper"
    debug: bool = True

    # Backblaze B2 Configuration
    b2_application_key_id: str = ""
    b2_application_key: str = ""
    b2_bucket_name: str = ""

    # LangChain API Keys
    anthropic_api_key: str = ""
    google_api_key: str = ""  # For future Gemini support

    class Config:
        env_file = ".env"


settings = Settings()
