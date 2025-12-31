from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/content_atlas"
    debug: bool = True
    date_default_dayfirst: bool = False
    log_level: str = "INFO"
    log_timezone: str = "local"  # Options: "local" (server timezone), "UTC"
    map_stage_timeout_seconds: int = 600
    map_parallel_max_workers: int = 4  # Controls parallel mapping chunk workers
    upload_max_file_size_mb: int = 100
    b2_max_retries: int = 3
    
    # LLM Analysis Timeouts (in seconds)
    llm_api_timeout: int = 120  # Timeout for Claude API calls (increased from 90s default)
    llm_analysis_timeout: int = 180  # Overall analysis timeout (must be > llm_api_timeout)
    llm_max_retries: int = 2  # Number of retries on transient LLM failures
    
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

    # Query settings (for natural language query agent)
    query_row_limit: int = 2500           # Max rows for agent queries
    query_timeout_seconds: int = 60       # Agent query timeout in seconds

    # Export settings (for large file downloads via /api/export/query)
    export_row_limit: int = 100000        # Max rows for export endpoint
    export_timeout_seconds: int = 120     # Export query timeout in seconds

    model_config = ConfigDict(env_file=".env", extra="ignore")


settings = Settings()
