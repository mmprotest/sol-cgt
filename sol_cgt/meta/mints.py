"""Persistent cache for mint metadata."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Any, Optional

from .. import utils


DEFAULT_MINT_META_PATH = utils.ensure_cache_dir("meta") / "mint_meta.json"


def _parse_entry(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"decimals": None, "symbol": None, "ts": None}
    decimals = value.get("decimals")
    if decimals is not None:
        try:
            decimals = int(decimals)
        except (TypeError, ValueError):
            decimals = None
    symbol = value.get("symbol")
    if symbol is not None and not isinstance(symbol, str):
        symbol = None
    ts = value.get("ts")
    if ts is not None:
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            ts = None
    return {"decimals": decimals, "symbol": symbol, "ts": ts}


@dataclass
class MintMetaCache:
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path = DEFAULT_MINT_META_PATH) -> "MintMetaCache":
        if not path.exists():
            return cls()
        try:
            raw = utils.json_loads(path.read_text(encoding="utf-8"))
        except Exception:
            return cls()
        if not isinstance(raw, dict):
            return cls()
        entries = {mint: _parse_entry(meta) for mint, meta in raw.items() if isinstance(mint, str)}
        return cls(entries=entries)

    def save(self, path: Path = DEFAULT_MINT_META_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(utils.json_dumps(self.entries), encoding="utf-8")

    def get_decimals(self, mint: str) -> Optional[int]:
        entry = self.entries.get(mint)
        if not entry:
            return None
        decimals = entry.get("decimals")
        if decimals is None:
            return None
        try:
            return int(decimals)
        except (TypeError, ValueError):
            return None

    def set_decimals(self, mint: str, decimals: Optional[int]) -> None:
        entry = self.entries.get(mint, {})
        symbol = entry.get("symbol")
        self.entries[mint] = {
            "decimals": int(decimals) if decimals is not None else None,
            "symbol": symbol if isinstance(symbol, str) else None,
            "ts": int(time.time()),
        }
