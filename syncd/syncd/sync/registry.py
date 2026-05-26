from typing import Type

from syncd.sync.base import SyncProvider


class UnknownProviderError(Exception):
    pass


class DuplicateProviderError(Exception):
    pass


class ProviderRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, Type[SyncProvider]] = {}

    def register(self, provider_cls: Type[SyncProvider]) -> None:
        name = provider_cls.NAME
        if name in self._registry:
            raise DuplicateProviderError(f"Provider already registered: {name!r}")
        self._registry[name] = provider_cls

    def get(self, name: str) -> Type[SyncProvider]:
        if name not in self._registry:
            raise UnknownProviderError(f"Unknown provider: {name!r}")
        return self._registry[name]

    def list_names(self) -> list[str]:
        return list(self._registry.keys())


registry = ProviderRegistry()
