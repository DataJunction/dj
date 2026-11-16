"""approved_node_names must match authorize() for every name."""

import random
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from datajunction_server.internal.access.authorization import (
    AuthContext,
    AuthorizationService,
    PassthroughAuthorizationService,
    RBACAuthorizationService,
)
from datajunction_server.models.access import (
    AccessDecision,
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)

SERVICE_SETTINGS = "datajunction_server.internal.access.authorization.service.settings"

NAMES = [
    "a.x",
    "a.y",
    "a.b.x",
    "a.b.z",
    "a.b.c.x",
    "ab.x",
    "c.x",
    "c.d.x",
    "top",
    "",
]
SCOPE_VALUES = ["*", "a.*", "a.b.*", "a.b.x", "a.x", "c.*", "c.d.x", ".*", "top"]


def _scope(rng: random.Random) -> SimpleNamespace:
    return SimpleNamespace(
        action=rng.choice(list(ResourceAction)),
        scope_type=rng.choice(list(ResourceType)),
        scope_value=rng.choice(SCOPE_VALUES),
    )


def _rule(rng: random.Random) -> str:
    action = rng.choice(list(ResourceAction)).value
    scope_type = rng.choice(list(ResourceType)).value
    value = rng.choice([v for v in SCOPE_VALUES if v != ".*"])
    return f"{action}:{scope_type}:{value}"


def _context(rng: random.Random, is_admin: bool = False) -> AuthContext:
    assignments = [
        SimpleNamespace(
            expires_at=rng.choice(
                [None, datetime.now(UTC) + timedelta(days=1)],
            ),
            role=SimpleNamespace(
                scopes=[_scope(rng) for _ in range(rng.randint(0, 4))],
            ),
        )
        for _ in range(rng.randint(0, 3))
    ]
    assignments.append(
        SimpleNamespace(
            expires_at=datetime.now(UTC) - timedelta(days=1),
            role=SimpleNamespace(scopes=[_scope(rng) for _ in range(2)]),
        ),
    )
    return AuthContext(
        user_id=1,
        username="test-user",
        oauth_provider="basic",
        role_assignments=assignments,
        is_admin=is_admin,
        default_scopes=[_scope(rng) for _ in range(rng.randint(0, 2))],
    )


def _expected(
    service: AuthorizationService,
    context: AuthContext,
    names: list[str],
    verb: ResourceAction,
) -> list[str]:
    decisions = service.authorize(
        context,
        [
            ResourceRequest(
                verb=verb,
                access_object=Resource(name=name, resource_type=ResourceType.NODE),
            )
            for name in names
        ],
    )
    return [d.request.access_object.name for d in decisions if d.approved]


@pytest.mark.parametrize("policy", ["permissive", "restrictive"])
@pytest.mark.parametrize("verb", list(ResourceAction))
def test_rbac_matches_authorize(mocker, policy, verb):
    """Random scopes, rules and expiries give the same names as authorize()."""
    service_settings = mocker.patch(SERVICE_SETTINGS)
    service_settings.default_access_policy = policy
    service = RBACAuthorizationService()
    rng = random.Random(f"{policy}-{verb}")
    seen_approved = seen_denied = 0
    for _ in range(400):
        service_settings.restrictive_scopes = [
            _rule(rng) for _ in range(rng.randint(0, 3))
        ]
        context = _context(rng)
        names = rng.sample(NAMES, k=rng.randint(0, len(NAMES)))
        expected = _expected(service, context, names, verb)
        assert service.approved_node_names(context, names, verb) == expected
        seen_approved += len(expected)
        seen_denied += len(names) - len(expected)
    assert seen_approved > 0
    assert seen_denied > 0


@pytest.mark.parametrize(
    "policy,expected",
    [("permissive", ["a.x", "b.y"]), ("restrictive", [])],
)
def test_rbac_without_matching_scopes_uses_default_policy(mocker, policy, expected):
    """A caller with no relevant scopes or rules gets the default decision."""
    service_settings = mocker.patch(SERVICE_SETTINGS)
    service_settings.default_access_policy = policy
    service_settings.restrictive_scopes = ["write:namespace:a.*"]
    context = AuthContext(
        user_id=1,
        username="test-user",
        oauth_provider="basic",
        role_assignments=[],
    )
    assert RBACAuthorizationService().approved_node_names(
        context,
        ["a.x", "b.y"],
        ResourceAction.READ,
    ) == expected


def test_rbac_admin_approves_all_and_audits(mocker, caplog):
    """Admins skip matching and leave the same audit record as authorize()."""
    mocker.patch(SERVICE_SETTINGS).restrictive_scopes = []
    context = AuthContext(
        user_id=7,
        username="admin-user",
        oauth_provider="basic",
        role_assignments=[],
        is_admin=True,
    )
    names = [f"ns.n{i}" for i in range(25)]
    service = RBACAuthorizationService()
    with caplog.at_level("WARNING", logger="datajunction.audit.rbac"):
        approved = service.approved_node_names(context, names, ResourceAction.READ)
        batch_record = caplog.records[-1].getMessage()
        caplog.clear()
        _expected(service, context, names, ResourceAction.READ)
        authorize_record = caplog.records[-1].getMessage()
    assert approved == names
    assert batch_record == authorize_record
    assert "request_count=25" in batch_record
    assert "truncated=True" in batch_record


def test_passthrough_approves_all():
    """Passthrough approves every name."""
    context = AuthContext(
        user_id=1,
        username="test-user",
        oauth_provider="basic",
        role_assignments=[],
    )
    names = ["a.x", "b.y"]
    assert PassthroughAuthorizationService().approved_node_names(
        context,
        names,
        ResourceAction.READ,
    ) == names


def test_default_builds_requests_and_filters():
    """Providers without a batch method fall back to authorize()."""

    class DenyOdd(AuthorizationService):
        name = "test_deny_odd"

        def authorize(self, auth_context, requests):
            return [
                AccessDecision(
                    request=request,
                    approved=len(request.access_object.name) % 2 == 0,
                )
                for request in requests
            ]

    context = AuthContext(
        user_id=1,
        username="test-user",
        oauth_provider="basic",
        role_assignments=[],
    )
    assert DenyOdd().approved_node_names(
        context,
        ["ab", "abc", "abcd"],
        ResourceAction.READ,
    ) == ["ab", "abcd"]
