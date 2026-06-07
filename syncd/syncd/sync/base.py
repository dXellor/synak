from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class SyncContext:
    pair_id: str
    local: str
    direction: str
    interval: int
    provider_config: dict[str, Any]
    exclude: list[str] = field(default_factory=list)


@dataclass
class ProviderStatus:
    pair_id: str
    state: str
    last_sync: float
    error: str
    extra: dict[str, Any] = field(default_factory=dict)


class SyncProvider(ABC):
    NAME: ClassVar[str]
    SCHEMA: ClassVar[dict[str, Any]]

    @abstractmethod
    async def start(self, context: SyncContext) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def pause(self) -> None: ...

    @abstractmethod
    async def resume(self) -> None: ...

    @abstractmethod
    async def trigger(self) -> None: ...

    @abstractmethod
    async def status(self) -> ProviderStatus: ...
