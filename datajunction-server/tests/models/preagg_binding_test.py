"""
Tests for the column bindings a pre-aggregation declares inline.

Two surfaces declare an externally-built pre-aggregation table: the ``kind: preagg``
YAML spec (``PreAggSpec``) and the body of ``POST /preaggs/register``
(``RegisterPreAggregationsRequest``). Both spell it as maps from reference to
physical column, and both enforce that through
``datajunction_server.models.preagg_binding``, so every rule below is asserted
against both models rather than once against one of them.

The messages are pinned in full. They are the migration path for anyone holding a
file or an API client written against the retired four-field form -- someone should
be able to fix their payload from the message alone -- so a reworded message is a
change worth noticing.
"""

import pytest
from pydantic import ValidationError

from datajunction_server.errors import (
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
)
from datajunction_server.models.deployment import PreAggSpec
from datajunction_server.models.preaggregation import RegisterPreAggregationsRequest

# The shape each surface shows, in the syntax that surface is authored in. Spelled
# out here rather than imported, so a change to the example is a change to a test.
YAML_SHAPE = """metrics:
  default.view_secs: view_secs_sum
  default.session_count: session_cnt
dimensions:
  default.page_d.page_id: page
  default.geo_country_d.country_iso_code: country_iso_code"""

JSON_SHAPE = """{
  "metrics": {
    "default.view_secs": "view_secs_sum",
    "default.session_count": "session_cnt"
  },
  "dimensions": {
    "default.page_d.page_id": "page",
    "default.geo_country_d.country_iso_code": "country_iso_code"
  }
}"""


def _spec_payload(**overrides) -> dict:
    """A valid `kind: preagg` spec, before any override under test."""
    payload = {
        "name": "p",
        "namespace": "ns",
        "metrics": {"${prefix}count": "cnt"},
        "dimensions": {"${prefix}d.attr": "phys_attr"},
        "catalog": "c",
        "schema": "s",
        "table": "t",
    }
    payload.update(overrides)
    return payload


def _request_payload(**overrides) -> dict:
    """A valid POST /preaggs/register body, before any override under test."""
    payload = {
        "metrics": {"ns.count": "cnt"},
        "dimensions": {"ns.d.attr": "phys_attr"},
        "table": {"catalog": "c", "schema": "s", "table": "t"},
    }
    payload.update(overrides)
    return payload


# Each surface: how to build a payload, how to validate it, the exception it
# raises, how its messages name the declaration, and the shape they show.
SURFACES = [
    pytest.param(
        _spec_payload,
        PreAggSpec.model_validate,
        DJInvalidDeploymentConfig,
        "Pre-aggregation 'p'",
        YAML_SHAPE,
        id="yaml-spec",
    ),
    pytest.param(
        _request_payload,
        RegisterPreAggregationsRequest.model_validate,
        DJInvalidInputException,
        "The POST /preaggs/register request body",
        JSON_SHAPE,
        id="register-request",
    ),
]

SURFACE_ARGS = "payload_for,validate,exception,subject,shape"


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
def test_map_form_is_accepted(payload_for, validate, exception, subject, shape):
    """The map form validates on both surfaces."""
    declaration = validate(payload_for())
    assert declaration.metrics == {list(payload_for()["metrics"])[0]: "cnt"}
    assert declaration.dimensions == {
        list(payload_for()["dimensions"])[0]: "phys_attr",
    }


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
@pytest.mark.parametrize(
    "retired,replacement,noun",
    [
        ("measure_columns", "metrics", "metric"),
        ("dimension_columns", "dimensions", "dimension"),
    ],
)
def test_rejects_retired_column_blocks(
    retired,
    replacement,
    noun,
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """
    The retired four-field form is refused rather than silently ignored: an
    unknown key would drop every binding it declared.
    """
    with pytest.raises(exception) as exc_info:
        validate(payload_for(**{retired: {"ns.count": "cnt"}}))
    assert exc_info.value.message == (
        f"{subject} declares `{retired}`, which is no longer a pre-aggregation "
        f"field. Declare each {noun} together with the physical column of the "
        f"external table that holds it, under `{replacement}`, and drop the "
        f"`{retired}` block. The map form looks like this:\n\n{shape}"
    )


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
@pytest.mark.parametrize(
    "field,noun",
    [("metrics", "metric"), ("dimensions", "dimension")],
)
def test_rejects_list_of_references(
    field,
    noun,
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """
    A bare list of references carries no bindings at all, so it earns the shape
    to write instead rather than a pydantic type error.
    """
    with pytest.raises(exception) as exc_info:
        validate(payload_for(**{field: ["ns.count"]}))
    assert exc_info.value.message == (
        f"{subject} declares `{field}` as a list, not a map. `{field}` maps each "
        f"{noun} reference to the physical column of the external table that "
        f"holds it. The map form looks like this:\n\n{shape}"
    )


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
def test_rejects_metric_without_column(
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """A measure's DJ-side name is hashed, so there is nothing to default to."""
    with pytest.raises(exception) as exc_info:
        validate(payload_for(metrics={"ns.count": None, "ns.total": "total_sum"}))
    assert exc_info.value.message == (
        f"{subject} leaves the physical column empty under `metrics` for "
        f"['ns.count']. A measure's DJ-side name is auto-generated with an "
        f"expression-hash suffix, so there is no name to fall back on. The map "
        f"form looks like this:\n\n{shape}"
    )


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
def test_rejects_dimension_without_column(
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """
    A dimension is held to the same rule even though its DJ column name would be
    a plausible default: a trailing colon is too easy to write by accident, and
    the written-out name documents the table.
    """
    with pytest.raises(exception) as exc_info:
        validate(payload_for(dimensions={"ns.d.attr": "phys_attr", "ns.d.same": None}))
    assert exc_info.value.message == (
        f"{subject} leaves the physical column empty under `dimensions` for "
        f"['ns.d.same']. Write the column out even when it matches the DJ column "
        f"name, so the declaration says what the table actually holds. The map "
        f"form looks like this:\n\n{shape}"
    )


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
def test_matching_column_still_has_to_be_written_out(
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """
    A dimension stored under the name DJ uses is written out like any other. The
    binding is kept as declared rather than collapsed to "same as default", so
    what the file says is what the table holds.
    """
    dimension = list(payload_for()["dimensions"])[0]
    attribute = dimension.rsplit(".", 1)[-1]
    declaration = validate(payload_for(dimensions={dimension: attribute}))
    assert declaration.dimensions == {dimension: attribute}


@pytest.mark.parametrize(SURFACE_ARGS, SURFACES)
def test_non_mapping_input_falls_through_to_pydantic(
    payload_for,
    validate,
    exception,
    subject,
    shape,
):
    """
    Input that isn't a mapping at all falls through the binding checks and gets
    pydantic's own error, rather than blowing up inside the validator.
    """
    with pytest.raises(ValidationError) as exc_info:
        validate(["not", "a", "declaration"])
    assert exc_info.value.errors()[0]["type"] == "model_type"
