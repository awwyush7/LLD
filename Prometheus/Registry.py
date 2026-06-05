from threading import Lock
from typing import List, Union
from Prometheus.Metrics.Counter import Counter
from Prometheus.Metrics.Gauge import Gauge
from Prometheus.Metrics.Histogram import Histogram

Metric = Union[Counter, Gauge, Histogram]


class Registry:
    """
    Singleton registry that holds all metrics for this process.
    When /metrics is scraped, the registry serializes everything
    into Prometheus text exposition format.
    """

    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._metrics = []  # List[Metric]
                cls._instance._metrics_lock = Lock()
        return cls._instance

    def register(self, metric: Metric) -> Metric:
        """Register a metric and return it (allows inline registration)."""
        with self._metrics_lock:
            self._metrics.append(metric)
        return metric

    def serialize(self) -> str:
        """
        Convert all registered metrics to Prometheus text exposition format.

        Format per metric:
          # HELP <name> <help>
          # TYPE <name> <type>
          <name>{labels} <value>          ← Counter / Gauge
          <name>_bucket{labels,le="x"} n  ← Histogram buckets
          <name>_sum{labels} n
          <name>_count{labels} n
        """
        with self._metrics_lock:
            metrics = list(self._metrics)

        lines = []
        for metric in metrics:
            lines.extend(_serialize_metric(metric))
            lines.append("")  # blank line between metrics (Prometheus convention)

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal serialization helpers
# ---------------------------------------------------------------------------

def _label_str(label_names, key) -> str:
    """Build the {label="value",...} string. Returns '' if no labels."""
    if not label_names or not key:
        return ""
    parts = [f'{name}="{value}"' for name, value in zip(label_names, key)]
    return "{" + ",".join(parts) + "}"


def _serialize_metric(metric: Metric) -> List[str]:
    lines = []

    if isinstance(metric, Counter):
        lines.append(f"# HELP {metric.name} {metric.help}")
        lines.append(f"# TYPE {metric.name} counter")
        data = metric.collect()
        if not data:
            # Emit a zero line so the metric shows up even before first increment
            lines.append(f"{metric.name} 0")
        for key, value in data.items():
            ls = _label_str(metric.label_names, key)
            lines.append(f"{metric.name}{ls} {_fmt(value)}")

    elif isinstance(metric, Gauge):
        lines.append(f"# HELP {metric.name} {metric.help}")
        lines.append(f"# TYPE {metric.name} gauge")
        data = metric.collect()
        if not data:
            lines.append(f"{metric.name} 0")
        for key, value in data.items():
            ls = _label_str(metric.label_names, key)
            lines.append(f"{metric.name}{ls} {_fmt(value)}")

    elif isinstance(metric, Histogram):
        lines.append(f"# HELP {metric.name} {metric.help}")
        lines.append(f"# TYPE {metric.name} histogram")
        data = metric.collect()
        if not data:
            # Emit empty histogram skeleton
            for bound in metric.buckets:
                le = "+Inf" if bound == float("inf") else str(bound)
                lines.append(f'{metric.name}_bucket{{le="{le}"}} 0')
            lines.append(f"{metric.name}_sum 0")
            lines.append(f"{metric.name}_count 0")
        for key, obs in data.items():
            base_ls = _label_str(metric.label_names, key)
            # Strip closing } to append le= label
            for i, bound in enumerate(metric.buckets):
                le = "+Inf" if bound == float("inf") else str(bound)
                if base_ls:
                    bucket_ls = base_ls[:-1] + f',le="{le}"' + "}"
                else:
                    bucket_ls = '{' + f'le="{le}"' + '}'
                lines.append(f"{metric.name}_bucket{bucket_ls} {obs['buckets'][i]}")
            lines.append(f"{metric.name}_sum{base_ls} {_fmt(obs['sum'])}")
            lines.append(f"{metric.name}_count{base_ls} {obs['count']}")

    return lines


def _fmt(value: float) -> str:
    """Format a float: drop decimal point if it's a whole number."""
    return str(int(value)) if value == int(value) else str(value)