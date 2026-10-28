"""
Profiling harness for /sql/measures/v3/ and /sql/metrics/v3/.

Not collected by the normal suite (filename does not match ``*_test.py``).
Run explicitly:

    uv run pytest tests/construction/build_v3/profile_v3.py -s -p no:randomly
"""

import atexit
import cProfile
import pstats
import time
from collections import defaultdict
from io import StringIO

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

REPEATS = 5
REPORT_PATH = "/tmp/v3_profile.txt"
_REPORT = open(REPORT_PATH, "a")
atexit.register(_REPORT.close)


def print(*args):  # noqa: A001 - report to file, not to the log stream
    _REPORT.write(" ".join(str(a) for a in args) + "\n")
    _REPORT.flush()

SCENARIOS = [
    (
        "1 base metric, 1 dim",
        {
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.date.date_id"],
        },
    ),
    (
        "4 base metrics same fact, 3 dims",
        {
            "metrics": [
                "v3.total_revenue",
                "v3.total_quantity",
                "v3.order_count",
                "v3.customer_count",
            ],
            "dimensions": [
                "v3.date.date_id",
                "v3.product.category",
                "v3.order_details.status",
            ],
        },
    ),
    (
        "cross-fact derived, 2 dims",
        {
            "metrics": [
                "v3.conversion_rate",
                "v3.revenue_per_visitor",
                "v3.revenue_per_page_view",
            ],
            "dimensions": ["v3.date.date_id", "v3.product.category"],
        },
    ),
    (
        "multi-hop dims with roles",
        {
            "metrics": ["v3.total_revenue", "v3.order_count"],
            "dimensions": [
                "v3.date.week[order]",
                "v3.location.country[from]",
                "v3.location.city[to]",
                "v3.customer.name[customer]",
            ],
        },
    ),
    (
        "window metrics (period-over-period)",
        {
            "metrics": [
                "v3.wow_revenue_change",
                "v3.mom_revenue_change",
                "v3.wow_aov_change",
            ],
            "dimensions": ["v3.date.date_id[order]"],
        },
    ),
    (
        "wide: 8 base + 4 derived, 4 dims, filters",
        {
            "metrics": [
                "v3.total_revenue",
                "v3.total_quantity",
                "v3.order_count",
                "v3.customer_count",
                "v3.page_view_count",
                "v3.product_view_count",
                "v3.session_count",
                "v3.visitor_count",
                "v3.avg_order_value",
                "v3.pages_per_session",
                "v3.conversion_rate",
                "v3.revenue_per_visitor",
            ],
            "dimensions": [
                "v3.date.date_id",
                "v3.product.category",
                "v3.location.country",
            ],
            "filters": [
                "v3.date.date_id > 20200101",
                "v3.product.category = 'electronics'",
            ],
        },
    ),
]


class QueryRecorder:
    """Counts SQL statements and their wall time across the whole engine."""

    def __init__(self):
        self.count = 0
        self.total_ms = 0.0
        self.by_statement = defaultdict(lambda: [0, 0.0])
        self.enabled = False

    def start(self):
        self.count = 0
        self.total_ms = 0.0
        self.by_statement.clear()
        self.enabled = True

    def stop(self):
        self.enabled = False

    def before(self, conn, cursor, statement, parameters, context, executemany):
        if self.enabled:
            conn.info.setdefault("_prof_t0", []).append(time.perf_counter())

    def after(self, conn, cursor, statement, parameters, context, executemany):
        if not self.enabled:
            return
        stack = conn.info.get("_prof_t0")
        if not stack:  # pragma: no cover - defensive
            return
        elapsed = (time.perf_counter() - stack.pop()) * 1000
        self.count += 1
        self.total_ms += elapsed
        key = " ".join(statement.split())[:110]
        slot = self.by_statement[key]
        slot[0] += 1
        slot[1] += elapsed


RECORDER = QueryRecorder()
event.listen(Engine, "before_cursor_execute", RECORDER.before)
event.listen(Engine, "after_cursor_execute", RECORDER.after)


# tottime rollup buckets, matched against the function's filename.
BUCKETS = [
    ("DJ AST traversal (sql/parsing/ast.py)", ("sql/parsing/ast.py",)),
    ("ANTLR runtime", ("/antlr4/",)),
    ("DJ parse backend/visitor", ("sql/parsing/backends",)),
    ("build_v3.*", ("construction/build_v3/",)),
    ("construction (non-v3)", ("/construction/",)),
    ("database models / api", ("datajunction_server/database", "datajunction_server/api")),
    ("internal / other DJ", ("datajunction_server/",)),
    ("sqlalchemy", ("/sqlalchemy/",)),
    ("psycopg driver", ("/psycopg",)),
    ("asyncio wait (DB I/O)", ("/asyncio/", "/selectors.py")),
    ("deepcopy", ("/copy.py",)),
    ("dataclasses", ("/dataclasses.py",)),
    ("pydantic", ("/pydantic", "pydantic_core")),
    ("starlette/fastapi/httpx", ("/starlette/", "/fastapi/", "/httpx/", "/httpcore/")),
]

# Functions worth a cumtime callout, matched on name.
KEY_FUNCS = {
    "parse",
    "parse_rule",
    "parse_sql_with_sll_fallback",
    "visit",
    "cached_parse",
    "_cached_parse",
    "build_measures_sql",
    "build_metrics_sql",
    "build_grain_groups",
    "process_metric_group",
    "resolve_dimensions",
    "load_build_context",
    "deepcopy",
    "compile_node_ast",
    "to_sql",
    "transpile_sql",
}


def _top_functions(stats: pstats.Stats, limit: int = 25):
    rows = []
    for func, (_cc, nc, tt, ct, _callers) in stats.stats.items():
        filename, lineno, name = func
        rows.append((tt, ct, nc, f"{filename.split('/')[-1]}:{lineno}({name})"))
    rows.sort(reverse=True)
    return rows[:limit]


def _key_functions(stats: pstats.Stats):
    rows = []
    for func, (_cc, nc, tt, ct, _callers) in stats.stats.items():
        filename, lineno, name = func
        if name in KEY_FUNCS and "datajunction_server" in filename or name == "deepcopy":
            short = "/".join(filename.split("/")[-3:])
            rows.append((ct, tt, nc, f"{short}:{lineno}({name})"))
    rows.sort(reverse=True)
    return rows[:15]


def _callers_of(stats: pstats.Stats, target_name: str, limit: int = 8):
    """Who calls `target_name`, and how much cumtime each caller contributes."""
    rows = []
    for func, (_cc, _nc, _tt, _ct, callers) in stats.stats.items():
        if func[2] != target_name:
            continue
        for caller, entry in callers.items():
            # entry = (cc, nc, tt, ct) contributed by this caller
            nc, ct = entry[1], entry[3]
            short = "/".join(caller[0].split("/")[-2:])
            rows.append((ct, nc, f"{short}:{caller[1]}({caller[2]})"))
    merged = defaultdict(lambda: [0.0, 0])
    for ct, nc, name in rows:
        merged[name][0] += ct
        merged[name][1] += nc
    out = sorted(((v[0], v[1], k) for k, v in merged.items()), reverse=True)
    return out[:limit]


def _bucket_rollup(stats: pstats.Stats):
    """Sum *tottime* per bucket - additive, no double counting."""
    totals = defaultdict(float)
    unbucketed = 0.0
    for func, (_cc, _nc, tt, _ct, _callers) in stats.stats.items():
        filename = func[0]
        for label, prefixes in BUCKETS:
            if any(p in filename for p in prefixes):
                totals[label] += tt
                break
        else:
            unbucketed += tt
    totals["(builtins / other)"] = unbucketed
    return sorted(totals.items(), key=lambda kv: -kv[1])


async def _run(client, endpoint, params):
    response = await client.get(endpoint, params=params)
    assert response.status_code == 200, response.text
    return response


async def _profile_endpoint(client, endpoint, label, params):
    # Warm up (caches, lazy imports, connection pool).
    await _run(client, endpoint, params)

    timings = []
    RECORDER.start()
    for _ in range(REPEATS):
        t0 = time.perf_counter()
        await _run(client, endpoint, params)
        timings.append((time.perf_counter() - t0) * 1000)
    RECORDER.stop()
    queries, db_ms = RECORDER.count / REPEATS, RECORDER.total_ms / REPEATS
    top_queries = sorted(
        RECORDER.by_statement.items(),
        key=lambda kv: -kv[1][1],
    )[:5]

    profiler = cProfile.Profile()
    profiler.enable()
    await _run(client, endpoint, params)
    profiler.disable()
    stats = pstats.Stats(profiler, stream=StringIO())

    print(f"\n{'=' * 100}")
    print(f"{endpoint}  |  {label}")
    print(f"{'=' * 100}")
    timings.sort()
    print(
        f"wall: min={timings[0]:.1f}ms  median={timings[len(timings) // 2]:.1f}ms  "
        f"max={timings[-1]:.1f}ms  (n={REPEATS})",
    )
    print(f"db: {queries:.1f} queries/request, {db_ms:.1f}ms/request in the driver")
    print("\n-- cumulative tottime by area (profiled run) --")
    total = sum(v for _k, v in _bucket_rollup(stats))
    for area, secs in _bucket_rollup(stats):
        if secs * 1000 < 1:
            continue
        print(f"  {secs * 1000:8.1f}ms  {100 * secs / total:5.1f}%  {area}")
    print(f"  {total * 1000:8.1f}ms   100.0%  TOTAL (profiled, includes overhead)")
    print("\n-- top functions by tottime --")
    for tt, ct, nc, name in _top_functions(stats, 20):
        if tt * 1000 < 0.5:
            continue
        print(f"  tot={tt * 1000:7.1f}ms cum={ct * 1000:8.1f}ms n={nc:<7} {name}")
    print("\n-- key DJ functions by cumtime --")
    for ct, tt, nc, name in _key_functions(stats):
        print(f"  cum={ct * 1000:8.1f}ms tot={tt * 1000:7.1f}ms n={nc:<7} {name}")
    for target in ("parse", "deepcopy"):
        print(f"\n-- callers of {target}() by cumtime --")
        for ct, nc, name in _callers_of(stats, target):
            print(f"  cum={ct * 1000:8.1f}ms n={nc:<6} {name}")
    print("\n-- top SQL statements by total time --")
    for statement, (count, ms) in top_queries:
        print(f"  {ms / REPEATS:7.1f}ms  x{count / REPEATS:5.1f}  {statement}")


@pytest.mark.asyncio
async def test_profile_measures_v3(module__client_with_build_v3):
    for label, params in SCENARIOS:
        await _profile_endpoint(
            module__client_with_build_v3,
            "/sql/measures/v3/",
            label,
            params,
        )


@pytest.mark.asyncio
async def test_profile_metrics_v3(module__client_with_build_v3):
    for label, params in SCENARIOS:
        await _profile_endpoint(
            module__client_with_build_v3,
            "/sql/metrics/v3/",
            label,
            params,
        )


@pytest.mark.asyncio
async def test_parse_call_dedup(module__client_with_build_v3):
    """How many parse() calls per request, and how many are re-parses."""
    import sys

    from datajunction_server.sql.parsing.backends import antlr4 as antlr4_backend

    original = antlr4_backend.parse
    calls: list[tuple[str, str]] = []

    def traced(sql):
        import traceback

        frame = traceback.extract_stack(limit=2)[0]
        calls.append((f"{frame.filename.split('/')[-1]}:{frame.lineno}", sql or ""))
        return original(sql)

    patched = []
    for module in list(sys.modules.values()):
        if getattr(module, "parse", None) is original:
            module.parse = traced
            patched.append(module)
    antlr4_backend.parse = traced

    try:
        for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
            for label, params in SCENARIOS:
                calls.clear()
                await _run(module__client_with_build_v3, endpoint, params)
                distinct = {sql for _caller, sql in calls}
                print(f"\n{endpoint} | {label}")
                print(
                    f"  parse() calls={len(calls)}  distinct SQL strings={len(distinct)}"
                    f"  redundant={len(calls) - len(distinct)}",
                )
                per_site = defaultdict(lambda: [0, set()])
                for caller, sql in calls:
                    per_site[caller][0] += 1
                    per_site[caller][1].add(sql)
                for caller, (count, sqls) in sorted(
                    per_site.items(),
                    key=lambda kv: -kv[1][0],
                ):
                    print(f"    {count:4} calls ({len(sqls):3} distinct)  {caller}")
    finally:
        for module in patched:
            module.parse = original
        antlr4_backend.parse = original


@pytest.mark.asyncio
async def test_parse_memoization_upside(module__client_with_build_v3):
    """
    Upper/lower bound on what caching parse() would buy.

    - shared: memo returns the same AST object (unsafe if callers mutate) -> upper bound
    - copied: memo returns a deepcopy -> realistic, safe variant
    """
    import copy as copy_mod
    import sys

    from datajunction_server.sql.parsing.backends import antlr4 as antlr4_backend

    original = antlr4_backend.parse
    memo: dict[str, object] = {}
    mode = {"value": "off"}

    def traced(sql):
        if mode["value"] == "off":
            return original(sql)
        if sql not in memo:
            memo[sql] = original(sql)
            if mode["value"] == "shared":
                # Hand out a copy the first time too, so the memo stays pristine.
                return copy_mod.deepcopy(memo[sql])
        if mode["value"] == "shared":
            return memo[sql]
        return copy_mod.deepcopy(memo[sql])

    patched = [m for m in list(sys.modules.values()) if getattr(m, "parse", None) is original]
    for module in patched:
        module.parse = traced
    antlr4_backend.parse = traced

    async def timed(endpoint, params, n=5):
        try:
            response = await module__client_with_build_v3.get(endpoint, params=params)
        except Exception as exc:  # noqa: BLE001 - unsafe variants can blow up
            return float("nan"), f"RAISED {type(exc).__name__}"
        if response.status_code != 200:
            return float("nan"), f"FAILED {response.status_code}"
        out = []
        for _ in range(n):
            t0 = time.perf_counter()
            try:
                response = await module__client_with_build_v3.get(endpoint, params=params)
            except Exception as exc:  # noqa: BLE001
                return float("nan"), f"RAISED {type(exc).__name__}"
            out.append((time.perf_counter() - t0) * 1000)
        out.sort()
        return out[len(out) // 2], response.text

    try:
        for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
            for label, params in SCENARIOS:
                results = {}
                for variant in ("off", "copied", "shared"):
                    mode["value"] = variant
                    memo.clear()
                    results[variant] = await timed(endpoint, params)
                base_ms, base_body = results["off"]
                print(f"\n{endpoint} | {label}")
                for variant in ("off", "copied", "shared"):
                    ms, body = results[variant]
                    same = "same-sql" if body == base_body else "DIFFERENT-SQL"
                    delta = f"{100 * (ms - base_ms) / base_ms:+.0f}%"
                    print(f"  {variant:7} median={ms:7.1f}ms  {delta:>6}  {same}")
    finally:
        for module in patched:
            module.parse = original
        antlr4_backend.parse = original


@pytest.mark.asyncio
async def test_query_inventory(module__client_with_build_v3):
    """Every DB statement issued for one request, in order."""
    for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
        label, params = SCENARIOS[0]
        await _run(module__client_with_build_v3, endpoint, params)
        RECORDER.start()
        await _run(module__client_with_build_v3, endpoint, params)
        RECORDER.stop()
        print(f"\n{endpoint} | {label} | {RECORDER.count} statements, "
              f"{RECORDER.total_ms:.1f}ms")
        for statement, (count, ms) in sorted(
            RECORDER.by_statement.items(),
            key=lambda kv: -kv[1][1],
        ):
            print(f"  x{count} {ms:6.1f}ms  {statement[:100]}")


@pytest.mark.asyncio
async def test_parse_memo_with_working_deepcopy(module__client_with_build_v3):
    """
    Same memoization experiment, but with Function.__deepcopy__ (which returns
    ``self``) disabled so deepcopy actually clones the tree.
    """
    import copy as copy_mod
    import sys

    from datajunction_server.sql.parsing import ast as ast_mod
    from datajunction_server.sql.parsing.backends import antlr4 as antlr4_backend

    original = antlr4_backend.parse
    memo: dict[str, object] = {}
    mode = {"value": "off"}

    def traced(sql):
        if mode["value"] == "off":
            return original(sql)
        if sql not in memo:
            memo[sql] = original(sql)
        return copy_mod.deepcopy(memo[sql])

    patched = [m for m in list(sys.modules.values()) if getattr(m, "parse", None) is original]
    real_deepcopy = ast_mod.Function.__deepcopy__

    async def timed(endpoint, params, n=5):
        try:
            response = await module__client_with_build_v3.get(endpoint, params=params)
            if response.status_code != 200:
                return float("nan"), f"FAILED {response.status_code}"
            out = []
            for _ in range(n):
                t0 = time.perf_counter()
                response = await module__client_with_build_v3.get(endpoint, params=params)
                out.append((time.perf_counter() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            return float("nan"), f"RAISED {type(exc).__name__}"
        out.sort()
        return out[len(out) // 2], response.text

    try:
        for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
            for label, params in SCENARIOS:
                mode["value"] = "off"
                base_ms, base_body = await timed(endpoint, params)
                print(f"\n{endpoint} | {label}")
                print(f"  baseline               median={base_ms:7.1f}ms")

                # memo + deepcopy, Function.__deepcopy__ still returning self
                memo.clear()
                mode["value"] = "on"
                for module in patched:
                    module.parse = traced
                antlr4_backend.parse = traced
                ms, body = await timed(endpoint, params)
                same = "same-sql" if body == base_body else "DIFFERENT-SQL"
                print(
                    f"  memo, Function shared  median={ms:7.1f}ms "
                    f"{100 * (ms - base_ms) / base_ms:+5.0f}%  {same}",
                )

                # memo + deepcopy, Function actually cloned
                memo.clear()
                ast_mod.Function.__deepcopy__ = None
                ms, body = await timed(endpoint, params)
                ast_mod.Function.__deepcopy__ = real_deepcopy
                same = "same-sql" if body == base_body else "DIFFERENT-SQL"
                print(
                    f"  memo, Function cloned  median={ms:7.1f}ms "
                    f"{100 * (ms - base_ms) / base_ms:+5.0f}%  {same}",
                )

                # no memo, just Function cloned (cost of a correct deepcopy)
                mode["value"] = "off"
                for module in patched:
                    module.parse = original
                antlr4_backend.parse = original
                ast_mod.Function.__deepcopy__ = None
                ms, body = await timed(endpoint, params)
                ast_mod.Function.__deepcopy__ = real_deepcopy
                same = "same-sql" if body == base_body else "DIFFERENT-SQL"
                print(
                    f"  no memo, Function clone median={ms:6.1f}ms "
                    f"{100 * (ms - base_ms) / base_ms:+5.0f}%  {same}",
                )
    finally:
        for module in patched:
            module.parse = original
        antlr4_backend.parse = original
        ast_mod.Function.__deepcopy__ = real_deepcopy


@pytest.mark.asyncio
async def test_antlr_tree_memo(module__client_with_build_v3):
    """
    Cache only the ANTLR parse tree, still run the visitor on every call.

    The ANTLR tree is read-only; the mutable DJ AST is rebuilt each time, so
    this needs no cloning.
    """
    from datajunction_server.sql.parsing.backends import antlr4 as antlr4_backend

    original = antlr4_backend.parse_sql_with_sll_fallback
    memo: dict[tuple, object] = {}
    mode = {"value": "off"}

    def traced(string, rule, converter=None, debug=False):
        if mode["value"] == "off":
            return original(string, rule, converter, debug)
        key = (string, rule)
        if key not in memo:
            memo[key] = original(string, rule, converter, debug)
        return memo[key]

    async def timed(endpoint, params, n=5):
        try:
            response = await module__client_with_build_v3.get(endpoint, params=params)
            if response.status_code != 200:
                return float("nan"), f"FAILED {response.status_code}"
            out = []
            for _ in range(n):
                t0 = time.perf_counter()
                response = await module__client_with_build_v3.get(endpoint, params=params)
                out.append((time.perf_counter() - t0) * 1000)
        except Exception as exc:  # noqa: BLE001
            return float("nan"), f"RAISED {type(exc).__name__}"
        out.sort()
        return out[len(out) // 2], response.text

    antlr4_backend.parse_sql_with_sll_fallback = traced
    try:
        for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
            for label, params in SCENARIOS:
                mode["value"] = "off"
                base_ms, base_body = await timed(endpoint, params)
                memo.clear()
                mode["value"] = "on"
                ms, body = await timed(endpoint, params)
                same = "same-sql" if body == base_body else "DIFFERENT-SQL"
                print(f"\n{endpoint} | {label}")
                print(f"  baseline          median={base_ms:7.1f}ms")
                print(
                    f"  antlr-tree cached median={ms:7.1f}ms "
                    f"{100 * (ms - base_ms) / base_ms:+5.0f}%  {same}",
                )
    finally:
        antlr4_backend.parse_sql_with_sll_fallback = original


@pytest.mark.asyncio
async def test_reprofile_with_tree_cache(module__client_with_build_v3):
    """
    A/B the shipped ANTLR tree cache on this branch, and re-profile the
    cached path to see what the remaining time is spent on.
    """
    from datajunction_server.sql.parsing.backends import antlr4 as antlr4_backend

    cached = antlr4_backend.cached_antlr_tree

    def bypass(sql, rule):
        return antlr4_backend.parse_sql_with_sll_fallback(sql, rule)

    async def timed(endpoint, params, n=5):
        await _run(module__client_with_build_v3, endpoint, params)
        out = []
        for _ in range(n):
            t0 = time.perf_counter()
            await _run(module__client_with_build_v3, endpoint, params)
            out.append((time.perf_counter() - t0) * 1000)
        out.sort()
        return out[len(out) // 2]

    for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
        for label, params in SCENARIOS:
            antlr4_backend.cached_antlr_tree = bypass
            off = await timed(endpoint, params)

            antlr4_backend.cached_antlr_tree = cached
            cached.cache_clear()
            on = await timed(endpoint, params)

            print(f"\n{endpoint} | {label}")
            print(f"  cache bypassed median={off:7.1f}ms")
            print(
                f"  cache on       median={on:7.1f}ms "
                f"{100 * (on - off) / off:+5.0f}%",
            )

    # Full profile of the widest case with the cache live.
    label, params = SCENARIOS[-1]
    antlr4_backend.cached_antlr_tree = cached
    for endpoint in ("/sql/measures/v3/", "/sql/metrics/v3/"):
        await _profile_endpoint(module__client_with_build_v3, endpoint, label, params)
    print(f"\ncache stats after run: {cached.cache_info()}")
