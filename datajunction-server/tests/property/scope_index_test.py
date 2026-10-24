"""
`ScopeIndex` must grant exactly what scanning every scope grants.

The index is an optimization of `RBACAuthorizationService._matching_scope`, so
the scan is the oracle: any scopes, any request, same answer.
"""

from types import SimpleNamespace

from hypothesis import given, settings, strategies as st

from datajunction_server.internal.access.authorization.service import (
    RBACAuthorizationService,
    ScopeIndex,
)
from datajunction_server.models.access import (
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)
from tests.property.budget import examples

SEGMENTS = ["a", "b", "c"]

names = st.lists(st.sampled_from(SEGMENTS), min_size=1, max_size=4).map(".".join)

# Values the grammar accepts, and ones it rejects, which must grant nothing.
scope_values = st.one_of(
    names,
    names.map(lambda name: f"{name}.*"),
    st.sampled_from(["*", ".*", "**", "", "a..b", "a.", ".a", "a.*.b", "*.a"]),
)

scopes = st.builds(
    SimpleNamespace,
    action=st.sampled_from(list(ResourceAction)),
    scope_type=st.sampled_from(
        [ResourceType.NODE, ResourceType.NAMESPACE, ResourceType.NODE]
    ),
    scope_value=scope_values,
)

requests = st.builds(
    lambda verb, resource_type, name: ResourceRequest(
        verb=verb,
        access_object=Resource(name=name, resource_type=resource_type),
    ),
    st.sampled_from(list(ResourceAction)),
    st.sampled_from([ResourceType.NODE, ResourceType.NAMESPACE]),
    names,
)


@settings(max_examples=examples(300))
@given(
    scopes=st.lists(scopes, max_size=12),
    batch=st.lists(requests, min_size=1, max_size=20),
)
def test_index_grants_what_a_scan_grants(scopes, batch):
    index = ScopeIndex(scopes, RBACAuthorizationService.PERMISSION_HIERARCHY)
    for request in batch:
        scanned = RBACAuthorizationService._matching_scope(scopes, request) is not None
        assert index.grants(request) is scanned, (scopes, request)
