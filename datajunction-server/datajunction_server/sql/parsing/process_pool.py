"""Parse many queries in a pool of worker processes."""

import asyncio
import atexit
import math
import multiprocessing
import pickle
import threading
from concurrent.futures import ProcessPoolExecutor

import datajunction_server.database  # noqa: F401  (must load before the parser)
from datajunction_server.sql.parsing import ast, cache

MAX_BATCH = 200
WARMUP_QUERIES = ("SELECT 1 AS a", "SELECT a, SUM(b) FROM t GROUP BY a")

_pool: ProcessPoolExecutor | None = None
_pool_size = 0
_pool_lock = threading.Lock()

# One result per query: the pickled tree, or an exception (pickled if
# possible) with its message.
Outcome = tuple[bytes] | tuple[None, bytes | None, str]


def _warm_up() -> None:
    """Load the parser in a new worker."""
    from datajunction_server.sql.parsing.backends.antlr4 import parse

    for query in WARMUP_QUERIES:
        parse(query)


def _parse_batch(queries: list[str], fast: bool) -> list[Outcome]:
    """Parse queries in a worker; failures come back as values."""
    from datajunction_server.sql.parsing.backends.antlr4 import parse

    token = ast._fast_parse.set(fast)
    outcomes: list[Outcome] = []
    try:
        for query in queries:
            try:
                outcomes.append((cache.dump(parse(query)),))
            except Exception as exc:
                try:
                    pickled = pickle.dumps(exc)
                except Exception:
                    pickled = None
                outcomes.append((None, pickled, str(exc)))
    finally:
        ast._fast_parse.reset(token)
    return outcomes


def _stop(pool: ProcessPoolExecutor) -> None:
    """Shut a pool down and end its workers."""
    workers = list(pool._processes.values())
    pool.shutdown(wait=False, cancel_futures=True)
    for worker in workers:
        worker.terminate()


def get_pool(size: int) -> ProcessPoolExecutor:
    """Return the shared pool, starting it on first use."""
    global _pool, _pool_size
    with _pool_lock:
        if _pool is None or _pool_size != size:
            if _pool is not None:
                _stop(_pool)
            _pool = ProcessPoolExecutor(
                max_workers=size,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_warm_up,
            )
            _pool_size = size
        return _pool


def shutdown_pool() -> None:
    """Stop the workers, if any."""
    global _pool
    with _pool_lock:
        pool, _pool = _pool, None
    if pool is not None:
        _stop(pool)


atexit.register(shutdown_pool)


def _decode(query: str, outcome: Outcome, fast: bool) -> ast.Query | Exception:
    """Turn a worker result into a tree or an exception."""
    if outcome[0] is not None:
        blob = outcome[0]
        cache.store(query, blob, fast)
        return pickle.loads(blob)
    _, pickled, message = outcome
    if pickled is not None:
        try:
            return pickle.loads(pickled)
        except Exception:
            pass
    return RuntimeError(message)


async def parse_in_pool(
    queries: dict[str, str],
    size: int,
) -> dict[str, ast.Query | Exception]:
    """
    Parse queries by name, using the cache and then the worker processes.

    Raises if the pool fails; the pool is dropped so the next call starts a new one.
    """
    fast = ast._fast_parse.get()
    results: dict[str, ast.Query | Exception] = {}
    misses: dict[str, str] = {}

    def split_hits() -> None:
        for name, query in queries.items():
            cached = cache.lookup(query)
            if cached is None:
                misses[name] = query
            else:
                results[name] = cached

    await asyncio.to_thread(split_hits)
    if not misses:
        return results

    names = list(misses)
    batch_size = max(1, min(MAX_BATCH, math.ceil(len(names) / (size * 4))))
    batches = [names[i : i + batch_size] for i in range(0, len(names), batch_size)]
    loop = asyncio.get_running_loop()
    try:
        pool = get_pool(size)
        futures = [
            loop.run_in_executor(
                pool,
                _parse_batch,
                [misses[name] for name in batch],
                fast,
            )
            for batch in batches
        ]
        outcomes = await asyncio.gather(*futures)
    except Exception:
        shutdown_pool()
        raise

    def decode_all() -> None:
        for batch, batch_outcomes in zip(batches, outcomes):
            for name, outcome in zip(batch, batch_outcomes):
                results[name] = _decode(misses[name], outcome, fast)

    await asyncio.to_thread(decode_all)
    return results
