"""Configuration loading and validation."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class APIKeys(BaseModel):
    """Container for optional API keys."""

    helius: Optional[str] = None
    birdeye: Optional[str] = None


class YAMLConfigSettingsSource(PydanticBaseSettingsSource):
    """Load settings from a YAML configuration file."""

    def __init__(self, settings_cls: type[BaseSettings], config_path: Optional[Path]):
        super().__init__(settings_cls)
        self.config_path = config_path

    def __call__(self) -> Dict[str, Any]:
        if self.config_path is None or not self.config_path.exists():
            return {}
        data = yaml.safe_load(self.config_path.read_text())
        if not isinstance(data, dict):
            return {}
        return data


class AppSettings(BaseSettings):
    """Application configuration resolved from CLI/env/YAML."""

    model_config = SettingsConfigDict(env_prefix="SOLCGT_", env_file=".env", extra="ignore")

    config_path: Optional[Path] = Field(default=None, exclude=True)

    wallets: list[str] = Field(default_factory=list)
    country: str = "AU"
    tz: str = "UTC"
    method: str = "FIFO"
    price_source: str = "auto"
    fx_source: str = "rba"
    airdrop_cost: str = "zero"
    treat_liquidity_as_disposal: bool = False
    api_keys: APIKeys = Field(default_factory=APIKeys)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        config_path = None
        if isinstance(init_settings, PydanticBaseSettingsSource):  # pragma: no cover
            pass
        # ``init_settings`` exposes ``init_kwargs`` attribute with the raw values passed
        init_kwargs = getattr(init_settings, "init_kwargs", {})  # type: ignore[attr-defined]
        config_path = init_kwargs.get("config_path")
        yaml_source = YAMLConfigSettingsSource(settings_cls, config_path)
        # Precedence: CLI (init) > environment > .env > YAML > file secrets
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
            file_secret_settings,
        )


def load_settings(config_path: Optional[Path] = None, overrides: Optional[Dict[str, Any]] = None) -> AppSettings:
    overrides = overrides or {}
    if config_path is not None:
        overrides.setdefault("config_path", config_path)
    return AppSettings(**overrides)
