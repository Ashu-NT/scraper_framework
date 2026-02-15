from __future__ import annotations

import random
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for HTTP retry behavior."""

    max_attempts: int = 3
    base_delay_s: float = 1.0
    jitter_s: float = 0.3
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


class RateLimiter:
    """Simple fixed-delay rate limiter."""

    def __init__(self, delay_ms: int):
        self.delay_s = max(0, delay_ms) / 1000.0

    def sleep(self) -> None:
        """Sleep for the configured delay."""
        if self.delay_s > 0:
            time.sleep(self.delay_s)


def backoff_sleep(policy: RetryPolicy, attempt_index: int) -> None:
    """Sleep with exponential backoff and jitter."""
    # exponential backoff with jitter
    delay = policy.base_delay_s * (2**attempt_index)
    delay += random.uniform(0, policy.jitter_s)
    time.sleep(delay)
