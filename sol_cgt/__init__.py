"""Top level package for the sol_cgt project."""
from importlib import metadata


def get_version() -> str:
    """Return the installed package version.

    When running from a development checkout, ``importlib.metadata`` will
    resolve the version defined in ``pyproject.toml``.  The helper is small but
    keeps ``__version__`` available without importing ``metadata`` at module
    import time which speeds up CLI start-up.
    """

    try:
        return metadata.version("sol-cgt")
    except metadata.PackageNotFoundError:  # pragma: no cover - during tests
        return "0.0.0"


__all__ = ["get_version"]
