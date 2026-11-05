---
weight: 65
title: Deployment Telemetry
---

DJ exposes deployment metrics through
`datajunction_server.instrumentation.provider.MetricsProvider`. The default
provider does nothing; install a provider with `set_metrics_provider()` at server
startup to export these metrics to your monitoring backend.

## Execution outcomes and latency

| Metric | Type | Meaning |
| --- | --- | --- |
| `dj.deployments.execute` | Counter | Deployment executions that finished, failed, or were cancelled |
| `dj.deployments.execute_latency_ms` | Timer | Execution duration in milliseconds, including unsuccessful attempts |
| `dj.deployments.node_count` | Gauge | Number of node results collected during an execution that returned normally |
| `dj.deployments.create` | Counter | API submissions, tagged with the status observed at submission time |
| `dj.deployments.create_latency_ms` | Timer | Time spent submitting an asynchronous deployment |

The execution counter and timer carry these tags:

| Tag | Values |
| --- | --- |
| `namespace` | Deployment namespace |
| `source_type` | `git` or `local`; omitted sources count as `local` |
| `dry_run` | `True` or `False` |
| `status` | `success`, `failed`, or `cancelled` |

An execution is `failed` when it raises an exception or returns any failed
resource result. Results marked invalid, warning, or skipped follow the API's
existing success semantics. Cancellation records `cancelled` and propagates to
the caller. Execution timing includes validation, transaction commit, and
materialization scheduling performed by the orchestrator. It does not measure
the subsequent Spark or Druid workflow runtime.

## Dashboard and alert queries

Use the execution counter for outcome totals and failure rates. The submission
counter commonly reports `pending`, because the asynchronous deployment has not
finished when the API responds.

For production deployment panels, filter `dry_run=False`. Group execution counts
by `status` and optionally `namespace` or `source_type`. Compute a failure rate
over a fixed window as failed executions divided by success plus failed
executions. Display cancellations separately. Use the execution timer for
latency percentiles, with separate panels for successful and failed executions.
Choose alert thresholds and minimum traffic from observed deployment volume.

These metrics describe orchestrator executions. They are not a durable audit
log: abrupt process termination cannot emit a final metric, and the stale
deployment sweep can mark a deployment failed independently of its orchestrator.
Use deployment records to investigate interrupted deployments and reconcile
their persisted status. Request rejection before execution is visible through
HTTP request metrics.

Adding `status` and `source_type` to the existing execution timer splits its
series. Dashboards that aggregate across these tags keep the combined view;
comparisons across this change should account for failed attempts now being
included in the latency distribution.
