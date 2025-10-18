from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/data_mapper"
    debug: bool = True

    class Config:
        env_file = ".env"


settings = Settings()
