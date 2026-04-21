"""Utils for handling GraphQL queries."""

import re
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.types import Info
from strawberry.types.nodes import FragmentSpread, InlineFragment

from datajunction_server.utils import get_session

CURSOR_SEPARATOR = "-"

T = TypeVar("T")


@asynccontextmanager
async def resolver_session(info: Info) -> AsyncIterator[AsyncSession]:
    """Create an independent database session for a resolver.

    Each GraphQL resolver must use its own session because strawberry resolves
    top-level fields concurrently.  Sharing a single AsyncSession across
    concurrent resolvers causes ``InvalidCachedStatementError`` /
    ``isce`` errors.

    In tests, ``request.state.test_session`` holds a shared session that
    all resolvers must use (so they see the same transaction's data).
    Tests run resolvers sequentially so the shared session is safe.

    In production, ``test_session`` is not set, so we create a fresh
    session per resolver via ``get_session()``.

    Usage::

        async with resolver_session(info) as session:
            ...
    """
    request = info.context["request"]

    # In tests, get_session is overridden via dependency_overrides to return a
    # shared test session. We call the override directly so resolvers see the
    # same transaction. The override is a sync function returning the session.
    app = request.app
    override = app.dependency_overrides.get(get_session)
    if override is not None:
        yield override()
        return
    else:  # pragma: no cover
        # Production: create a genuinely independent session per resolver.
        gen = get_session(request, session_label="graphql_resolver")
        session = await gen.__anext__()
        try:
            yield session
        finally:
            await gen.aclose()  # type: ignore


def convert_camel_case(name):
    """
    Convert from camel case to snake case
    """
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    name = pattern.sub("_", name).lower()
    return name


def _merge_fields(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    Merge ``src`` into ``dst`` in place, matching GraphQL's field-merging
    semantics: repeated selections of the same field at the same level combine
    their sub-selections rather than overwriting each other. Without this,
    ``{ current { mode } current { status } }`` — or two fragments whose
    selections overlap — would cause the second occurrence to clobber the
    first, and the eager-loader would miss fields.

    Known limitation: ``@skip`` / ``@include`` directives are ignored, so a
    conditionally-skipped field is still treated as requested. This only
    causes over-eager-loading, not wrong results. Inline-fragment
    ``type_condition`` is also not checked; if the schema grows polymorphic
    types, revisit to avoid loading for types the object isn't.
    """
    for name, sub in src.items():
        if name not in dst:
            dst[name] = sub
        elif isinstance(dst[name], dict) and isinstance(sub, dict):
            _merge_fields(dst[name], sub)


def _walk_selections(selections) -> Dict[str, Any]:
    """
    Flatten a list of GraphQL selections into a {field_name: subfields_or_None}
    dict. Fragment spreads (``...Frag``) and inline fragments (``... on Type``)
    are transparent: their selections are merged into the parent level, which
    matches how the GraphQL runtime executes them.
    """
    out: Dict[str, Any] = {}
    for sel in selections:
        if isinstance(sel, (FragmentSpread, InlineFragment)):
            _merge_fields(out, _walk_selections(sel.selections))
            continue
        field_name = convert_camel_case(sel.name)
        sub = _walk_selections(sel.selections) if sel.selections else None
        _merge_fields(out, {field_name: sub})
    return out


def extract_fields(query_fields) -> Dict[str, Any]:
    """
    Extract fields from GraphQL query input into a dictionary.

    Fragment spreads and inline fragments are flattened transparently, so a
    query that uses ``...NodeInfo`` produces the same dict as one that lists
    those fields inline. Repeated field selections are merged per GraphQL
    semantics.
    """
    fields: Dict[str, Any] = {}
    for query_field in query_fields.selected_fields:
        _merge_fields(fields, _walk_selections(query_field.selections))
    return fields


def dedupe_append(base: list[T], extras: list[T]) -> list[T]:
    """
    Append items from extras to base, ensuring no duplicates.
    """
    base_set = set(base)
    return base + [x for x in extras if x not in base_set]
