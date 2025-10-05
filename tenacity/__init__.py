"""Minimal tenacity stubs for retry decorators."""
from __future__ import annotations

from typing import Any, Callable


def retry(*args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        return func

    return decorator


def stop_after_attempt(attempts: int) -> int:  # pragma: no cover - placeholder
    return attempts


def wait_exponential(**kwargs: Any) -> dict[str, Any]:  # pragma: no cover - placeholder
    return kwargs


class RetryError(Exception):
    def __init__(self, last_attempt: Any = None) -> None:
        super().__init__("RetryError")
        self.last_attempt = last_attempt
