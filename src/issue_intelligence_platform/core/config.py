from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://localhost:5432/issue_intelligence"
    mlflow_tracking_uri: str = "http://mlflow:5000"
    mlflow_model_name: str = "issue-priority-classifier"
    mlflow_model_alias: str = "champion"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
