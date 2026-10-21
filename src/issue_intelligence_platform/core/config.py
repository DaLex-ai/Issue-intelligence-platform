from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://localhost:5432/issue_intelligence"
    mlflow_tracking_uri: str = "http://mlflow:5000"
    mlflow_model_name: str = "issue-priority-classifier"
    mlflow_model_alias: str = "champion"

    rabbit_url: str = "amqp://guest:guest@localhost:5672/"
    rabbit_queue: str = "issue.inference.tasks"
    rabbit_dlq: str = "issue.inference.dlq"
    rabbit_dlx: str = "issue.inference.dead"
    rabbit_dlq_routing_key: str = "failed"
    rabbit_max_length: int = 20
    enable_backpressure: bool = True
    publisher_confirm_timeout: float = 2.0
    api_publish_slots: int = 4
    api_retry_after_seconds: int = 0
    worker_concurrency: int = 2
    worker_prefetch_count: int = 2
    inference_delay_seconds: float = 0.5
    worker_max_attempts: int = 3
    worker_retry_delay_seconds: float = 0.25
    worker_graceful_timeout: float = 60.0
    worker_health_port: int = 8081
    batch_backoff_min_seconds: float = 0.05
    batch_backoff_max_seconds: float = 1.0

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
