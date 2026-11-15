"""A bounded cache of parsed queries."""

import pickle
import threading
from collections import OrderedDict

from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

# Pickled trees are about 4 KB each, against 3-4 ms to parse and 0.1 ms to load.
PARSE_CACHE_SIZE = 32768
_cache: OrderedDict[tuple[str, bool], bytes] = OrderedDict()
_lock = threading.Lock()


def dump(tree: ast.Query) -> bytes:
    """Pickle a tree for the cache."""
    return pickle.dumps(tree, protocol=pickle.HIGHEST_PROTOCOL)


def lookup(query: str) -> ast.Query | None:
    """Load a tree from the cache, or return None on a miss."""
    key = (query, ast._fast_parse.get())
    with _lock:
        cached = _cache.get(key)
        if cached is not None:
            _cache.move_to_end(key)
    return pickle.loads(cached) if cached is not None else None


def store(query: str, blob: bytes, fast: bool) -> None:
    """Add a pickled tree made by `dump` to the cache."""
    with _lock:
        _cache[(query, fast)] = blob
        while len(_cache) > PARSE_CACHE_SIZE:
            _cache.popitem(last=False)


def parse_cached(query: str) -> ast.Query:
    """
    Parse a SQL query. Thread-safe, suitable for a threadpool.

    Repeated text is loaded from the cache, so every caller gets its own tree.
    Trees built in fast mode, which have no parent links, are kept apart.
    """
    fast = ast._fast_parse.get()
    cached = lookup(query)
    if cached is not None:
        return cached
    tree = parse(query)
    store(query, dump(tree), fast)
    return tree
