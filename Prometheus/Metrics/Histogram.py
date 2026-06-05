from threading import Lock
from typing import Dict, List, Tuple

# Default bucket boundaries in seconds (standard Prometheus defaults)
DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class Histogram:
    """
    Tracks the distribution of observed values (e.g., latency).
    Stores:
      - bucket counts: how many observations fell below each boundary
      - total count of all observations
      - total sum of all observed values

    Supports labels: histogram.labels(path="/book").observe(0.32)
    """

    def __init__(
        self,
        name: str,
        help: str,
        label_names: Tuple[str, ...] = (),
        buckets: Tuple[float, ...] = DEFAULT_BUCKETS,
    ):
        self.name = name
        self.help = help
        self.label_names = label_names
        # Always add +Inf bucket at the end
        self.buckets = tuple(sorted(buckets)) + (float("inf"),)
        self._lock = Lock()
        # key → {"buckets": [counts], "count": int, "sum": float}
        self._values: Dict[Tuple, dict] = {}

    def _init_key(self, key: Tuple) -> None:
        """Initialize storage for a new label combination."""
        if key not in self._values:
            self._values[key] = {
                "buckets": [0] * len(self.buckets),  # one count per boundary
                "count": 0,
                "sum": 0.0,
            }

    def labels(self, **kwargs) -> "_HistogramChild":
        key = tuple(kwargs.get(l, "") for l in self.label_names)
        with self._lock:
            self._init_key(key)
        return _HistogramChild(self, key)

    def observe(self, value: float) -> None:
        """Record an observation with no labels."""
        self._observe((), value)

    def _observe(self, key: Tuple, value: float) -> None:
        with self._lock:
            self._init_key(key)
            data = self._values[key]
            # Increment every bucket whose upper bound >= observed value
            for i, bound in enumerate(self.buckets):
                if value <= bound:
                    data["buckets"][i] += 1
            data["count"] += 1
            data["sum"] += value

    def collect(self) -> Dict[Tuple, dict]:
        with self._lock:
            return {
                key: {
                    "buckets": list(data["buckets"]),
                    "count": data["count"],
                    "sum": data["sum"],
                }
                for key, data in self._values.items()
            }


class _HistogramChild:
    """A Histogram bound to a specific set of label values."""

    def __init__(self, histogram: Histogram, key: Tuple):
        self._histogram = histogram
        self._key = key

    def observe(self, value: float) -> None:
        self._histogram._observe(self._key, value)