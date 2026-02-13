"""Circuit breaker utilities for upstream dependency resilience."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Coroutine, TypeVar

from ..utils import ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitBreakerOpenError(RuntimeError):
    """Raised when calls are rejected because circuit breaker is open."""


@dataclass(frozen=True)
class CircuitBreakerConfig:
    name: str
    failure_threshold: int
    recovery_timeout_seconds: float


class CircuitBreaker:
    """Simple thread-safe circuit breaker with closed/open/half-open states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self._state = self.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None
        self._lock = Lock()

    @property
    def name(self) -> str:
        return self.config.name

    def _transition_to(self, next_state: str) -> None:
        if self._state == next_state:
            return
        logger.warning(
            "Circuit breaker transition: %s %s -> %s",
            self.name,
            self._state,
            next_state,
        )
        self._state = next_state

    def _can_attempt_call(self) -> bool:
        now = time.monotonic()
        with self._lock:
            if self._state != self.OPEN:
                return True

            assert self._opened_at is not None
            if (now - self._opened_at) >= self.config.recovery_timeout_seconds:
                self._transition_to(self.HALF_OPEN)
                return True
            return False

    def _record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._opened_at = None
            self._transition_to(self.CLOSED)

    def _record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            if self._state == self.HALF_OPEN or self._failure_count >= self.config.failure_threshold:
                self._opened_at = time.monotonic()
                self._transition_to(self.OPEN)

    async def call(
        self,
        fn: Callable[[], T],
        should_trip: Callable[[Exception], bool],
    ) -> T:
        if not self._can_attempt_call():
            raise CircuitBreakerOpenError(
                f"{self.name} temporarily unavailable; circuit breaker is open"
            )

        try:
            result = fn()
        except Exception as exc:
            if should_trip(exc):
                self._record_failure()
            else:
                self._record_success()
            raise

        self._record_success()
        return result

    async def call_async(
        self,
        fn: Callable[[], "Coroutine[Any, Any, T]"],
        should_trip: Callable[[Exception], bool],
    ) -> T:
        if not self._can_attempt_call():
            raise CircuitBreakerOpenError(
                f"{self.name} temporarily unavailable; circuit breaker is open"
            )

        try:
            result = await fn()
        except Exception as exc:
            if should_trip(exc):
                self._record_failure()
            else:
                self._record_success()
            raise

        self._record_success()
        return result


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def is_non_retryable_error(exc: Exception) -> bool:
    """Errors from client-side validation and explicit 4xx should not trip breaker."""
    if isinstance(exc, (ValueError, ValidationError)):
        return True

    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and 400 <= status_code < 500:
        return True

    message = str(exc).lower()
    return any(token in message for token in ["validation", "invalid", "not found", "bad request"])


def should_trip_breaker(exc: Exception) -> bool:
    return not is_non_retryable_error(exc)


supabase_breaker = CircuitBreaker(
    CircuitBreakerConfig(
        name="supabase",
        failure_threshold=_env_int("SUPABASE_BREAKER_FAILURE_THRESHOLD", 5),
        recovery_timeout_seconds=_env_float("SUPABASE_BREAKER_RECOVERY_TIMEOUT_SECONDS", 30.0),
    )
)

external_dependency_breaker = CircuitBreaker(
    CircuitBreakerConfig(
        name="external_db_r2",
        failure_threshold=_env_int("EXTERNAL_DEP_BREAKER_FAILURE_THRESHOLD", 3),
        recovery_timeout_seconds=_env_float("EXTERNAL_DEP_BREAKER_RECOVERY_TIMEOUT_SECONDS", 20.0),
    )
)
