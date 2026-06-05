"""
Prometheus clone server — port 9100

  GET /dashboard  → live metrics UI (open this in browser)
  GET /scrape     → JSON snapshot scraped from all car-rental services
  GET /metrics    → own demo metrics in Prometheus text format

Run from C:\\Learning\\lld_new\\LLD:
    $env:PYTHONPATH = "C:\\Learning\\lld_new\\LLD"
    uvicorn Prometheus.server:app --port 9100
Then open: http://127.0.0.1:9100/dashboard
"""
import re
import time
import asyncio
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse

from Prometheus.Registry import Registry
from Prometheus.Metrics.Counter import Counter
from Prometheus.Metrics.Gauge import Gauge
from Prometheus.Metrics.Histogram import Histogram

# ---------------------------------------------------------------------------
# Scrape targets — one per service
# ---------------------------------------------------------------------------
SCRAPE_TARGETS = [
    {"job": "event-streamer",  "url": "http://127.0.0.1:8000/metrics"},
    {"job": "ticket-service",  "url": "http://127.0.0.1:8001/metrics"},
    {"job": "booking-service", "url": "http://127.0.0.1:8002/metrics"},
    {"job": "payment-service", "url": "http://127.0.0.1:8003/metrics"},
    # unified demo.py runs on 8001 — same entry as ticket-service covers it
]

# ---------------------------------------------------------------------------
# Prometheus text-format parser
# ---------------------------------------------------------------------------
def parse_prometheus_text(text: str) -> dict:
    """
    Parse Prometheus text exposition format into a structured dict keyed by
    base metric name.

    Each entry:
      { "help": str, "type": str,
        "samples": [{"labels": {k:v}, "value": float}],   # counter/gauge
        "buckets": [{"labels":{}, "le": str, "value": float}],
        "sum": float | None, "count": float | None }
    """
    metrics: dict = {}
    help_map: dict = {}
    type_map: dict = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("# HELP "):
            parts = line[7:].split(" ", 1)
            help_map[parts[0]] = parts[1] if len(parts) > 1 else ""
            continue
        if line.startswith("# TYPE "):
            parts = line[7:].split(" ", 1)
            type_map[parts[0]] = parts[1] if len(parts) > 1 else "untyped"
            continue
        if line.startswith("#"):
            continue

        m = re.match(
            r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+\-NaInf]+)',
            line,
        )
        if not m:
            continue

        raw_name, labels_str, value_str = m.group(1), m.group(2), m.group(3)
        try:
            value = float(value_str)
        except ValueError:
            continue

        labels: dict = {}
        if labels_str:
            for lm in re.finditer(r'(\w+)="([^"]*)"', labels_str):
                labels[lm.group(1)] = lm.group(2)

        # Detect histogram suffix
        base, suffix = raw_name, None
        for s in ("_bucket", "_sum", "_count"):
            if raw_name.endswith(s):
                base, suffix = raw_name[: -len(s)], s[1:]
                break

        if base not in metrics:
            metrics[base] = {
                "help": help_map.get(base, ""),
                "type": type_map.get(base, "untyped"),
                "samples": [],
                "buckets": [],
                "sum": None,
                "count": None,
            }

        if suffix == "bucket":
            le = labels.pop("le", "+Inf")
            metrics[base]["buckets"].append({"labels": dict(labels), "le": le, "value": value})
        elif suffix == "sum":
            metrics[base]["sum"] = value
        elif suffix == "count":
            metrics[base]["count"] = value
        else:
            metrics[base]["samples"].append({"labels": labels, "value": value})

    return metrics


# ---------------------------------------------------------------------------
# Async scraper
# ---------------------------------------------------------------------------
async def scrape_all() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    results = []

    async with httpx.AsyncClient(timeout=3.0) as client:
        async def fetch(target: dict) -> dict:
            try:
                resp = await client.get(target["url"])
                resp.raise_for_status()
                return {
                    "job": target["job"],
                    "url": target["url"],
                    "status": "up",
                    "error": None,
                    "metrics": parse_prometheus_text(resp.text),
                }
            except Exception as exc:
                return {
                    "job": target["job"],
                    "url": target["url"],
                    "status": "down",
                    "error": str(exc),
                    "metrics": {},
                }

        targets = await asyncio.gather(*[fetch(t) for t in SCRAPE_TARGETS])
        results.extend(targets)

    return {"scraped_at": now, "targets": results}


# ---------------------------------------------------------------------------
# Own registry (demo metrics for this server itself)
# ---------------------------------------------------------------------------
registry = Registry()
_scrape_counter = registry.register(
    Counter(name="dashboard_scrapes_total", help="Times /scrape was called")
)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Prometheus Dashboard")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/dashboard")


@app.get("/metrics", response_class=PlainTextResponse)
def metrics_own():
    return registry.serialize()


@app.get("/scrape")
async def scrape_endpoint():
    _scrape_counter.inc()
    return await scrape_all()


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Metrics Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    .fade { animation: fadeIn 0.3s ease-in; }
    @keyframes fadeIn { from { opacity: 0.4; } to { opacity: 1; } }
    .badge-up   { @apply bg-green-500 text-white text-xs px-2 py-0.5 rounded-full font-bold; }
    .badge-down { @apply bg-red-500   text-white text-xs px-2 py-0.5 rounded-full font-bold; }
  </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6 font-sans">

<div class="max-w-7xl mx-auto">

  <!-- Header -->
  <div class="flex items-center justify-between mb-6">
    <div>
      <h1 class="text-2xl font-bold tracking-tight">Metrics Dashboard</h1>
      <p class="text-gray-400 text-sm mt-0.5">Car Rental System — live Prometheus metrics</p>
    </div>
    <div class="flex items-center gap-4">
      <span id="lastUpdated" class="text-gray-500 text-xs">—</span>
      <label class="flex items-center gap-2 text-sm text-gray-400 cursor-pointer select-none">
        <input id="autoRefresh" type="checkbox" checked class="accent-blue-500"> Auto-refresh 5s
      </label>
      <button onclick="doScrape()" class="bg-blue-600 hover:bg-blue-700 text-white text-sm px-4 py-1.5 rounded transition-colors">
        Refresh
      </button>
    </div>
  </div>

  <!-- Service status pills -->
  <div id="serviceStatus" class="flex flex-wrap gap-3 mb-8"></div>

  <!-- Key metrics grid -->
  <h2 class="text-sm font-semibold text-gray-400 uppercase tracking-widest mb-3">Key Metrics</h2>
  <div id="keyMetrics" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8"></div>

  <!-- Latency section -->
  <h2 class="text-sm font-semibold text-gray-400 uppercase tracking-widest mb-3">Latency (from histograms)</h2>
  <div id="latencyMetrics" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8"></div>

  <!-- Per-service details -->
  <h2 class="text-sm font-semibold text-gray-400 uppercase tracking-widest mb-3">All Metrics</h2>
  <div id="serviceDetails" class="space-y-4"></div>

</div>

<script>
// -------------------------------------------------------------------------
// Percentile estimation from cumulative histogram buckets
// -------------------------------------------------------------------------
function estimatePercentile(buckets, p) {
  if (!buckets || !buckets.length) return null;
  const sorted = [...buckets].sort((a, b) => {
    const la = a.le === '+Inf' ? Infinity : parseFloat(a.le);
    const lb = b.le === '+Inf' ? Infinity : parseFloat(b.le);
    return la - lb;
  });
  const total = sorted[sorted.length - 1].value;
  if (total === 0) return 0;
  const target = p * total;
  let prevLe = 0, prevCount = 0;
  for (const b of sorted) {
    const le = b.le === '+Inf' ? Infinity : parseFloat(b.le);
    if (b.value >= target) {
      if (b.value === prevCount) return prevLe;
      const frac = (target - prevCount) / (b.value - prevCount);
      if (!isFinite(le)) return prevLe;
      return prevLe + frac * (le - prevLe);
    }
    prevLe = le; prevCount = b.value;
  }
  return prevLe;
}

function fmtMs(sec) {
  if (sec === null || sec === undefined) return '—';
  return (sec * 1000).toFixed(1) + ' ms';
}

// -------------------------------------------------------------------------
// Find a metric value across all targets
// -------------------------------------------------------------------------
function findSample(targets, metricName, labelFilter = {}) {
  for (const t of targets) {
    const m = t.metrics[metricName];
    if (!m) continue;
    for (const s of m.samples) {
      const match = Object.entries(labelFilter).every(([k,v]) => s.labels[k] === v);
      if (match) return s.value;
    }
  }
  return null;
}

function sumSamples(targets, metricName, labelFilter = {}) {
  let total = 0, found = false;
  for (const t of targets) {
    const m = t.metrics[metricName];
    if (!m) continue;
    for (const s of m.samples) {
      const match = Object.entries(labelFilter).every(([k,v]) => s.labels[k] === v);
      if (match) { total += s.value; found = true; }
    }
  }
  return found ? total : null;
}

function findHistogramBuckets(targets, metricName, labelFilter = {}) {
  for (const t of targets) {
    const m = t.metrics[metricName];
    if (!m || !m.buckets.length) continue;
    // Filter buckets matching labels (excluding 'le')
    const filtered = m.buckets.filter(b =>
      Object.entries(labelFilter).every(([k,v]) => b.labels[k] === v)
    );
    if (filtered.length) return filtered;
  }
  return [];
}

// -------------------------------------------------------------------------
// Render helpers
// -------------------------------------------------------------------------
function bigTile(label, value, sub = '', color = 'text-white') {
  const display = value === null ? '<span class="text-gray-600">—</span>'
                                 : `<span class="${color}">${value}</span>`;
  return `
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 fade">
      <p class="text-xs text-gray-500 mb-1 uppercase tracking-wide">${label}</p>
      <p class="text-3xl font-bold">${display}</p>
      ${sub ? `<p class="text-xs text-gray-500 mt-1">${sub}</p>` : ''}
    </div>`;
}

function latencyTile(label, p50, p95) {
  const c50 = p50 !== null && p50 < 0.1 ? 'text-green-400' : p50 < 0.5 ? 'text-yellow-400' : 'text-red-400';
  const c95 = p95 !== null && p95 < 0.5 ? 'text-green-400' : p95 < 1.0 ? 'text-yellow-400' : 'text-red-400';
  return `
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 fade">
      <p class="text-xs text-gray-500 mb-2 uppercase tracking-wide">${label}</p>
      <div class="flex gap-4">
        <div><p class="text-xs text-gray-500">P50</p><p class="text-lg font-semibold ${c50}">${fmtMs(p50)}</p></div>
        <div><p class="text-xs text-gray-500">P95</p><p class="text-lg font-semibold ${c95}">${fmtMs(p95)}</p></div>
      </div>
    </div>`;
}

function renderServiceDetails(targets) {
  return targets.map(t => {
    const metricRows = Object.entries(t.metrics).map(([name, m]) => {
      const rows = [];
      // samples (counter/gauge)
      for (const s of m.samples) {
        const lblStr = Object.entries(s.labels).map(([k,v]) => `<span class="text-purple-400">${k}</span>=<span class="text-green-300">"${v}"</span>`).join(' ');
        rows.push(`<tr class="border-t border-gray-800 hover:bg-gray-800/40">
          <td class="py-1.5 px-3 font-mono text-blue-300 text-xs">${name}</td>
          <td class="py-1.5 px-3 text-xs">${lblStr}</td>
          <td class="py-1.5 px-3 text-right font-mono text-yellow-300 text-xs">${s.value}</td>
        </tr>`);
      }
      // histogram summary
      if (m.buckets.length) {
        const p50 = estimatePercentile(m.buckets, 0.5);
        const p95 = estimatePercentile(m.buckets, 0.95);
        rows.push(`<tr class="border-t border-gray-800 hover:bg-gray-800/40">
          <td class="py-1.5 px-3 font-mono text-blue-300 text-xs">${name}</td>
          <td class="py-1.5 px-3 text-xs text-gray-400">histogram · count=${m.count ?? '—'} sum=${m.sum?.toFixed(4) ?? '—'}</td>
          <td class="py-1.5 px-3 text-right font-mono text-yellow-300 text-xs">P50=${fmtMs(p50)} P95=${fmtMs(p95)}</td>
        </tr>`);
      }
      return rows.join('');
    }).join('');

    const statusBadge = t.status === 'up'
      ? '<span class="badge-up">UP</span>'
      : `<span class="badge-down">DOWN</span>`;

    const errorMsg = t.status === 'down'
      ? `<p class="text-red-400 text-xs mt-1 ml-2">${t.error}</p>` : '';

    return `
      <details class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <summary class="flex items-center gap-3 p-4 cursor-pointer hover:bg-gray-800/50 select-none list-none">
          ${statusBadge}
          <span class="font-semibold">${t.job}</span>
          <span class="text-gray-500 text-xs">${t.url}</span>
          <span class="ml-auto text-gray-500 text-xs">${Object.keys(t.metrics).length} metrics ▾</span>
        </summary>
        ${errorMsg}
        <div class="overflow-x-auto">
          <table class="w-full text-sm min-w-[600px]">
            <thead><tr class="text-left text-gray-500 text-xs border-b border-gray-800">
              <th class="py-2 px-3">Metric</th><th class="py-2 px-3">Labels</th><th class="py-2 px-3 text-right">Value</th>
            </tr></thead>
            <tbody>${metricRows || '<tr><td colspan="3" class="py-4 px-3 text-gray-600 text-center">no samples yet</td></tr>'}</tbody>
          </table>
        </div>
      </details>`;
  }).join('');
}

// -------------------------------------------------------------------------
// Main render
// -------------------------------------------------------------------------
async function doScrape() {
  let data;
  try {
    const resp = await fetch('/scrape');
    data = await resp.json();
  } catch (e) {
    document.getElementById('lastUpdated').textContent = 'scrape error: ' + e.message;
    return;
  }

  const { targets, scraped_at } = data;
  const ts = new Date(scraped_at).toLocaleTimeString();
  document.getElementById('lastUpdated').textContent = 'Last updated: ' + ts;

  // Service status
  document.getElementById('serviceStatus').innerHTML = targets.map(t => {
    const cls = t.status === 'up'
      ? 'bg-green-900/50 border-green-700 text-green-300'
      : 'bg-red-900/50 border-red-700 text-red-400';
    const dot = t.status === 'up' ? 'bg-green-400' : 'bg-red-500';
    return `<div class="flex items-center gap-2 border rounded-full px-4 py-1.5 text-sm ${cls} fade">
      <span class="w-2 h-2 rounded-full ${dot} inline-block"></span>
      <span class="font-semibold">${t.job}</span>
      <span class="text-xs opacity-70">${t.status.toUpperCase()}</span>
    </div>`;
  }).join('');

  // Key metrics
  const bookingTotal    = sumSamples(targets, 'booking_requests_total') ?? 0;
  const ticketsTotal    = sumSamples(targets, 'tickets_confirmed_total') ?? 0;
  const paySuccess      = sumSamples(targets, 'payments_processed_total', {result: 'success'}) ?? 0;
  const payFail         = sumSamples(targets, 'payments_processed_total', {result: 'failure'}) ?? 0;
  const slotsPending    = sumSamples(targets, 'booking_slots_pending') ?? 0;
  const slotsBooked     = sumSamples(targets, 'booking_slots_booked') ?? 0;
  const qBook           = findSample(targets, 'streamer_queue_size', {topic: 'BookingTopic'});
  const qPay            = findSample(targets, 'streamer_queue_size', {topic: 'PaymentTopic'});

  document.getElementById('keyMetrics').innerHTML = [
    bigTile('Total Bookings',       bookingTotal, 'POST /book accepted',          'text-blue-400'),
    bigTile('Tickets Confirmed',    ticketsTotal, 'full saga completed',           'text-green-400'),
    bigTile('Payments Success',     paySuccess,  '',                              'text-green-400'),
    bigTile('Payments Failed',      payFail,     '',                              payFail > 0 ? 'text-red-400' : 'text-gray-400'),
    bigTile('Slots Pending',        slotsPending,'awaiting payment',              slotsPending > 0 ? 'text-yellow-400' : 'text-gray-400'),
    bigTile('Slots Booked',         slotsBooked, 'confirmed reservations',        'text-green-400'),
    bigTile('BookingTopic Queue',   qBook ?? '—','items waiting',                 qBook > 5 ? 'text-yellow-400' : 'text-gray-300'),
    bigTile('PaymentTopic Queue',   qPay  ?? '—','items waiting',                 qPay  > 5 ? 'text-yellow-400' : 'text-gray-300'),
  ].join('');

  // Latency
  const httpBookBuckets = findHistogramBuckets(targets, 'http_request_duration_seconds', {path: '/book'});
  const httpTickBuckets = findHistogramBuckets(targets, 'http_request_duration_seconds', {path: '/ticket'});
  const evtBookBuckets  = findHistogramBuckets(targets, 'event_processing_duration_seconds', {event_type: 'book'});
  const evtPayBuckets   = findHistogramBuckets(targets, 'event_processing_duration_seconds', {event_type: 'payment_success'});
  const payBuckets      = findHistogramBuckets(targets, 'payment_duration_seconds');
  const httpAllBuckets  = findHistogramBuckets(targets, 'http_request_duration_seconds');

  document.getElementById('latencyMetrics').innerHTML = [
    latencyTile('HTTP /book',             estimatePercentile(httpBookBuckets, 0.5),  estimatePercentile(httpBookBuckets, 0.95)),
    latencyTile('HTTP /ticket',           estimatePercentile(httpTickBuckets, 0.5),  estimatePercentile(httpTickBuckets, 0.95)),
    latencyTile('Event: book→pending',    estimatePercentile(evtBookBuckets, 0.5),   estimatePercentile(evtBookBuckets, 0.95)),
    latencyTile('Event: payment→booked',  estimatePercentile(evtPayBuckets, 0.5),    estimatePercentile(evtPayBuckets, 0.95)),
    latencyTile('Payment processing',     estimatePercentile(payBuckets, 0.5),        estimatePercentile(payBuckets, 0.95)),
    latencyTile('HTTP (all endpoints)',   estimatePercentile(httpAllBuckets, 0.5),    estimatePercentile(httpAllBuckets, 0.95)),
  ].join('');

  // Per-service details
  document.getElementById('serviceDetails').innerHTML = renderServiceDetails(targets);
}

// Auto-refresh
let timer = setInterval(doScrape, 5000);
document.getElementById('autoRefresh').addEventListener('change', e => {
  clearInterval(timer);
  if (e.target.checked) timer = setInterval(doScrape, 5000);
});

doScrape();
</script>
</body>
</html>"""


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML