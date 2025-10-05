"""Lightweight stub of the pydantic interface used in tests."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


class ConfigDict(dict):
    """Simple mapping used as a configuration placeholder."""


@dataclass
class _FieldInfo:
    default: Any
    default_factory: Optional[Callable[[], Any]]


def Field(*, default: Any = ... , default_factory: Optional[Callable[[], Any]] = None, **_: Any) -> _FieldInfo:
    return _FieldInfo(default, default_factory)


class BaseModelMeta(type):
    def __new__(mcls, name, bases, namespace):
        validators: Dict[str, list[tuple[Callable[..., Any], str]]] = {}
        field_defaults: Dict[str, _FieldInfo] = {}
        for attr, value in list(namespace.items()):
            meta = getattr(value, "__pydantic_validator__", None)
            if meta:
                field, mode = meta
                validators.setdefault(field, []).append((value, mode))
            if isinstance(value, _FieldInfo):
                field_defaults[attr] = value
                namespace[attr] = None
        namespace["__validators__"] = validators
        namespace["__field_defaults__"] = field_defaults
        return super().__new__(mcls, name, bases, namespace)


class BaseModel(metaclass=BaseModelMeta):
    model_config: ConfigDict = ConfigDict()

    def __init__(self, **data: Any) -> None:
        values: Dict[str, Any] = {}
        annotations = getattr(self, "__annotations__", {})
        # populate defaults
        for field in annotations:
            if field in data:
                values[field] = data[field]
            else:
                default_info = self.__field_defaults__.get(field)
                if default_info:
                    if default_info.default_factory is not None:
                        values[field] = default_info.default_factory()
                    elif default_info.default is not ...:
                        values[field] = default_info.default
                    else:
                        values[field] = None
                else:
                    values[field] = getattr(self.__class__, field, None)
        # run before validators
        for field, funcs in self.__validators__.items():
            for func, mode in funcs:
                if mode == "before":
                    callable_obj = func.__func__ if isinstance(func, classmethod) else func
                    try:
                        values[field] = callable_obj(self.__class__, values.get(field), values)
                    except TypeError:
                        values[field] = callable_obj(self.__class__, values.get(field))
        # assign attributes
        for field, value in values.items():
            setattr(self, field, value)
        # run after validators
        for field, funcs in self.__validators__.items():
            for func, mode in funcs:
                if mode != "before":
                    current = getattr(self, field)
                    callable_obj = func.__func__ if isinstance(func, classmethod) else func
                    try:
                        new_value = callable_obj(self.__class__, current, values)
                    except TypeError:
                        new_value = callable_obj(self.__class__, current)
                    setattr(self, field, new_value)

    def model_dump(self) -> Dict[str, Any]:
        annotations = getattr(self, "__annotations__", {})
        return {field: getattr(self, field) for field in annotations}


def field_validator(field: str, *, mode: str = "after"):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func.__pydantic_validator__ = (field, mode)  # type: ignore[attr-defined]
        return func

    return decorator


__all__ = ["BaseModel", "ConfigDict", "Field", "field_validator"]
