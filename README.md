# Sensor Compliance Report Pipeline

A weekly batch pipeline that evaluates whether the control limits configured on
factory sensors are appropriate for how those sensors actually behave. It runs in
Kubernetes against a ~30 GB memory ceiling over roughly a month of
high-cardinality sensor data.

## Stages

`compliance_runner` drives three stages in order:

| Stage | File | Role |
|---|---|---|
| Spec | `fdc_compliance_spec.py` | Build the sensor-spec catalog: limits, patterns, grades |
| Fetch | `fdc_compliance_fetch.py` | Maintain a rolling 30-day cache of raw readings in S3 |
| Report | `fdc_compliance_report.py` | Per-tool statistics, then combine into the weekly report |

## Points of interest

**Rolling S3 cache** (`fdc_compliance_fetch.py`) — consecutive weekly runs reuse
23 of the same 30 days of readings, so the pipeline caches the window in object
storage and queries the database only for the new days. `purge_expired_eqp_raw`
trims what has rolled off the back of the window.

**Bounded producer/consumer pipeline** (`upload_eqp_raw_direct_to_s3`) — DB
fetchers serialize to spooled temp files and push onto a size-capped queue;
uploaders drain it. The bounded queue is the memory ceiling, and the progress
reporter instruments queue wait time to show whether the database or object
storage is the constraint.

**Upload verification** (`upload_parquet_item`) — object metadata records row
count, byte size, and whether the calendar day was already closed when the data
was read, all in the same atomic PUT as the data. Stored length is verified after
upload; a mismatch deletes the object so the next run re-fetches it.

**Memory reclamation** (`_release_memory_to_os`) — freed pages were retained at
two levels: Arrow's memory pool and glibc's malloc arenas. `gc.collect()` alone
does not trim either, so container RSS crept upward across batches until both
were released explicitly.

**Weighted continuous admission** (`collect_fdc_fixed_data_from_bde`) — the
fetch stage's previous design: work admitted continuously under a row budget
rather than in static batches behind a barrier. See *Design history* below.

**Streaming combine** (`generate_fixed_weekly_report`) — per-tool results are
scanned lazily and streamed to disk rather than collected into one frame, and
large tools are processed in row-budgeted batches.

## Design history

The fetch stage went through three designs. The second is kept in
`fdc_compliance_fixed.py` rather than deleted, so the progression stays readable
alongside the stage that replaced it.

**1 — Static batches behind a barrier.** Per-tool 30-day queries were sized from
the query planner's own row estimates, sorted largest-first, and bin-packed into
groups under a fixed row cap. Each group ran concurrently, but the next could not
start until every query in the current one had finished. That barrier let each
group's slowest query stall the capacity already freed by the queries that had
completed — a cost paid once per group.

**2 — Weighted continuous admission** (`collect_fdc_fixed_data_from_bde`). The
groups were removed entirely. Units are admitted as soon as budget frees up,
bounded by estimated rows in flight and a concurrency cap, which is the same
memory ceiling a single group enforced without the per-group tail. A per-unit CSV
records admission concurrency, rows in flight and container memory at every
completion; that log is what the budget and concurrency numbers were tuned
against.

**3 — Per-tool-day fetch into an object-storage cache** (`fdc_compliance_fetch.py`,
current). Profiling the second design showed database round-trip still dominated
total runtime, which is what motivated caching the window rather than re-querying
it. Once the unit of work became a single tool-day — small and roughly uniform —
row-budget weighting no longer earned its complexity, and a plain bounded queue
was sufficient. The admission control got simpler because the work became
uniform, not because the weighted version was wrong.

`collect_fdc_fixed_data_from_bde` is therefore not on the path the runner takes.
It is retained for comparison.

## Status

This repository is the pipeline's architecture: stage orchestration, concurrency
control, the object-storage cache, memory management and upload verification are
all implemented.

The domain computations are left as extension points, marked with `TODO` and a
docstring describing each one's contract:

- **Statistics and compliance rules** — outlier-bound derivation, control-limit
  formulas, per-pattern fail criteria and false-positive suppression. These are
  specific to the sensors and process being monitored.
- **Source queries** — table names, column projections and filter predicates,
  which depend on the warehouse schema behind them.
- **Thresholds** in `fdc_config.py` — placeholders; tune per process.

The data-access layer targets an internal warehouse client and an S3-compatible
object store, so the stages are written to be read and adapted rather than run
as-is.
