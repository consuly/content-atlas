from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/data_mapper"
    debug: bool = True
    date_default_dayfirst: bool = False
    log_level: str = "INFO"
    map_stage_timeout_seconds: int = 600
    map_parallel_max_workers: int = 4  # Controls parallel mapping chunk workers
    upload_max_file_size_mb: int = 100
    b2_max_retries: int = 3
    
    # Authentication
    secret_key: str = "your-secret-key-change-in-production"
    
    # S3-Compatible Storage Configuration
    # Supports: Backblaze B2, AWS S3, MinIO, Wasabi, DigitalOcean Spaces, etc.
    storage_provider: str = "b2"  # "b2", "s3", "minio", "wasabi", etc.
    storage_endpoint_url: str = ""  # Required for B2/MinIO (e.g., https://s3.us-west-004.backblazeb2.com), leave empty for AWS S3
    storage_access_key_id: str = ""  # B2 Application Key ID or AWS Access Key ID
    storage_secret_access_key: str = ""  # B2 Application Key or AWS Secret Access Key
    storage_bucket_name: str = ""  # Bucket name
    storage_region: str = "us-west-004"  # Region (for B2 or AWS)

    # LangChain API Keys
    anthropic_api_key: str = ""
    google_api_key: str = ""  # For future Gemini support

    # Fixture controls
    enable_marketing_fixture_shortcuts: bool = True
    enable_auto_retry_failed_imports: bool = True

    model_config = ConfigDict(env_file=".env", extra="ignore")


settings = Settings()
