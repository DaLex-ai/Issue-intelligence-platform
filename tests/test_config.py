import pytest
from pydantic import ValidationError

from issue_intelligence_platform.core.config import Settings


@pytest.mark.parametrize(
    "values",
    [
        {"api_publish_slots": 0},
        {"worker_concurrency": 0},
        {"worker_prefetch_count": -1},
        {"worker_retry_delay_seconds": 0},
        {"inference_delay_seconds": -1},
        {"rabbit_max_length": 0},
        {"batch_backoff_min_seconds": 2, "batch_backoff_max_seconds": 1},
        {"publication_timeout_seconds": 1, "publisher_confirm_timeout": 2},
        {"worker_claim_timeout_seconds": 10, "worker_heartbeat_seconds": 5},
    ],
)
def test_invalid_runtime_configuration_is_rejected(values):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


def test_zero_prefetch_is_allowed_for_unbounded_benchmark():
    assert Settings(_env_file=None, worker_prefetch_count=0).worker_prefetch_count == 0
