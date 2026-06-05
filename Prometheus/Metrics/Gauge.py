from threading import Lock
from typing import Dict, Tuple


class Gauge:
    """
    A gauge that can go up and down. Represents a current state.
    Supports labels: gauge.labels(topic="BookingTopic").set(5)
    """

    def __init__(self, name: str, help: str, label_names: Tuple[str, ...] = ()):
        self.name = name
        self.help = help
        self.label_names = label_names
        self._lock = Lock()
        self._values: Dict[Tuple, float] = {}

    def labels(self, **kwargs) -> "_GaugeChild":
        key = tuple(kwargs.get(l, "") for l in self.label_names)
        with self._lock:
            if key not in self._values:
                self._values[key] = 0.0
        return _GaugeChild(self, key)

    def set(self, value: float) -> None:
        """Set with no labels."""
        self._set((), value)

    def inc(self, amount: float = 1.0) -> None:
        self._inc((), amount)

    def dec(self, amount: float = 1.0) -> None:
        self._inc((), -amount)

    def _set(self, key: Tuple, value: float) -> None:
        with self._lock:
            self._values[key] = value

    def _inc(self, key: Tuple, amount: float) -> None:
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def collect(self) -> Dict[Tuple, float]:
        with self._lock:
            return dict(self._values)


class _GaugeChild:
    """A Gauge bound to a specific set of label values."""

    def __init__(self, gauge: Gauge, key: Tuple):
        self._gauge = gauge
        self._key = key

    def set(self, value: float) -> None:
        self._gauge._set(self._key, value)

    def inc(self, amount: float = 1.0) -> None:
        self._gauge._inc(self._key, amount)

    def dec(self, amount: float = 1.0) -> None:
        self._gauge._inc(self._key, -amount)