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


def _fully_linked(tree: ast.Query) -> bool:
    """Whether every node below the root has a parent link."""
    return all(node.parent is not None for node in tree.flatten() if node is not tree)


def parse_cached(query: str) -> ast.Query:
    """
    Parse a SQL query. Thread-safe, suitable for a threadpool.

    Repeated text is loaded from the cache, so every caller gets its own tree.
    Trees built in fast mode, which have no parent links, are kept apart.
    """
    fast = ast._fast_parse
    key = (query, fast)
    with _lock:
        cached = _cache.get(key)
        if cached is not None:
            _cache.move_to_end(key)
    if cached is not None:
        return pickle.loads(cached)
    tree = parse(query)
    # Another thread can switch fast mode while this parse runs, so the tree
    # is checked instead of the flag.
    if fast or _fully_linked(tree):
        blob = pickle.dumps(tree, protocol=pickle.HIGHEST_PROTOCOL)
        with _lock:
            _cache[key] = blob
            while len(_cache) > PARSE_CACHE_SIZE:
                _cache.popitem(last=False)
    return tree
