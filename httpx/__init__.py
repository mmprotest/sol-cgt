"""Very small subset of httpx required for tests."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict


class Response(SimpleNamespace):
    def json(self) -> Any:
        raise NotImplementedError

    def raise_for_status(self) -> None:  # pragma: no cover - only stubbed
        pass


class AsyncClient:
    def __init__(self, *_, **__):
        pass

    async def get(self, *_, **__):  # pragma: no cover - not used in tests
        return Response()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - not used in tests
        return False
