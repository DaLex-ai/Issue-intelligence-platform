from pydantic import Field, model_validator
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
    rabbit_max_length: int = Field(default=20, gt=0)
    enable_backpressure: bool = True
    publisher_confirm_timeout: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    api_operation_timeout_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    api_publish_slots: int = Field(default=4, gt=0)
    api_retry_after_seconds: int = Field(default=0, ge=0)
    worker_concurrency: int = Field(default=2, gt=0)
    worker_prefetch_count: int = Field(default=2, ge=0)
    inference_delay_seconds: float = Field(default=0.5, ge=0, allow_inf_nan=False)
    worker_max_attempts: int = Field(default=3, gt=0)
    worker_retry_delay_seconds: float = Field(default=0.25, gt=0, allow_inf_nan=False)
    worker_graceful_timeout: float = Field(default=60.0, gt=0, allow_inf_nan=False)
    worker_health_port: int = Field(default=8081, ge=1, le=65535)
    batch_backoff_min_seconds: float = Field(default=0.05, gt=0, allow_inf_nan=False)
    batch_backoff_max_seconds: float = Field(default=1.0, gt=0, allow_inf_nan=False)

    worker_claim_timeout_seconds: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    worker_heartbeat_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    publication_timeout_seconds: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    publication_reconcile_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_intervals(self):
        if self.api_operation_timeout_seconds <= self.publisher_confirm_timeout:
            raise ValueError("API timeout must exceed publisher confirm timeout")
        if self.worker_claim_timeout_seconds <= 2 * self.worker_heartbeat_seconds:
            raise ValueError("claim timeout must exceed two heartbeat intervals")
        if self.batch_backoff_max_seconds < self.batch_backoff_min_seconds:
            raise ValueError("batch backoff maximum must not be below minimum")
        if self.publication_timeout_seconds <= self.publisher_confirm_timeout:
            raise ValueError(
                "publication timeout must exceed publisher confirm timeout"
            )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
