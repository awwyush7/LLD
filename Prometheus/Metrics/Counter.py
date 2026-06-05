from threading import Lock
from typing import Dict, Tuple


class Counter:
    """
    A counter that only goes up. Resets only on process restart.
    Supports labels: counter.labels(status="success").inc()
    """

    def __init__(self, name: str, help: str, label_names: Tuple[str, ...] = ()):
        self.name = name
        self.help = help
        self.label_names = label_names
        self._lock = Lock()
        # If no labels: single value. If labels: dict of label_values → count.
        self._values: Dict[Tuple, float] = {}

    def labels(self, **kwargs) -> "_CounterChild":
        """Return a child for the given label combination."""
        key = tuple(kwargs.get(l, "") for l in self.label_names)
        with self._lock:
            if key not in self._values:
                self._values[key] = 0.0
        return _CounterChild(self, key)

    def inc(self, amount: float = 1.0) -> None:
        """Increment with no labels. Use .labels(...).inc() when labels are defined."""
        self._inc((), amount)

    def _inc(self, key: Tuple, amount: float) -> None:
        if amount < 0:
            raise ValueError("Counter can only increase")
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def collect(self) -> Dict[Tuple, float]:
        with self._lock:
            return dict(self._values)


class _CounterChild:
    """A Counter bound to a specific set of label values."""

    def __init__(self, counter: Counter, key: Tuple):
        self._counter = counter
        self._key = key

    def inc(self, amount: float = 1.0) -> None:
        self._counter._inc(self._key, amount)