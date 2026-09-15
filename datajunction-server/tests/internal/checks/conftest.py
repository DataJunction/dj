"""
Shared vocabulary for the check-engine tests.

Deliberately invented and generic: the engine is indifferent to which metadata
keys, properties or tag types an installation registers, and hardcoding a real
governance vocabulary here would imply otherwise.
"""

from types import SimpleNamespace

# One custom-metadata key with three declared properties, as a registered schema
# would supply them.
DECLARED_PROPERTIES = {"sample": ("colour", "size", "shape")}


def make_node(
    name="default.widget",
    node_type="dimension",
    namespace="default",
    custom_metadata=None,
    description=None,
    mode="published",
    owners=(),
    tags=(),
    columns=(),
    primary_key=(),
    dimension_links=(),
):
    """A duck-typed stand-in for an eager-loaded node and its current revision."""
    return SimpleNamespace(
        name=name,
        type=node_type,
        namespace=namespace,
        owners=[SimpleNamespace(username=u, email=e) for u, e in owners],
        tags=[SimpleNamespace(name=n, tag_type=t) for n, t in tags],
        current=SimpleNamespace(
            custom_metadata=custom_metadata,
            description=description,
            mode=mode,
            columns=[SimpleNamespace(name=n, description=d) for n, d in columns],
            primary_key=lambda: [SimpleNamespace(name=n) for n in primary_key],
            dimension_links=list(dimension_links),
        ),
    )


def make_link(dimension_name="default.colour", join_type=None, default_value=None):
    """A dimension link pointing at a bare dimension node."""
    return SimpleNamespace(
        role=None,
        join_type=join_type,
        default_value=default_value,
        dimension=make_node(name=dimension_name),
    )
