from __future__ import annotations

from typing import Any


class VectorClock:
    """Vector clock following Parker et al. (1983) for causal ordering."""

    def __init__(self, node_id: str, clock: dict[str, int] | None = None) -> None:
        self.node_id = node_id
        self._clock: dict[str, int] = dict(clock) if clock else {}

    def increment(self) -> None:
        self._clock[self.node_id] = self._clock.get(self.node_id, 0) + 1

    def update(self, other: VectorClock) -> None:
        """Merge: take the component-wise maximum (Bayou receive rule)."""
        for node, ts in other._clock.items():
            self._clock[node] = max(self._clock.get(node, 0), ts)

    def happens_before(self, other: VectorClock) -> bool:
        """True if self → other: all components ≤ and at least one strictly <."""
        all_nodes = set(self._clock) | set(other._clock)
        at_least_one_less = False
        for node in all_nodes:
            s = self._clock.get(node, 0)
            o = other._clock.get(node, 0)
            if s > o:
                return False
            if s < o:
                at_least_one_less = True
        return at_least_one_less

    def concurrent_with(self, other: VectorClock) -> bool:
        """True if neither happens-before the other — i.e. a conflict."""
        return not self.happens_before(other) and not other.happens_before(self)

    def to_dict(self) -> dict[str, Any]:
        return {"node_id": self.node_id, "clock": dict(self._clock)}

    @classmethod
    def from_dict(cls, data: dict[str, Any], node_id: str) -> VectorClock:
        return cls(node_id=node_id, clock=data.get("clock", {}))

    def __repr__(self) -> str:
        return f"VectorClock({self.node_id!r}, {self._clock})"
