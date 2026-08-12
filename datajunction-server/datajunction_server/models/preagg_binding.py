"""
Shared validation for the inline column bindings a pre-aggregation declares.

Two surfaces adopt an externally-built pre-aggregation table: the ``kind: preagg``
YAML spec (``PreAggSpec``) and the JSON body of ``POST /preaggs/register``
(``RegisterPreAggregationsRequest``). Both spell the declaration the same way --
every metric and every dimension is named together with the physical column of the
external table that holds it, as a map -- so both enforce the rules from here
rather than keeping a copy each. One concept, one spelling, one implementation.

The three rules are the same on both surfaces:

1. The retired four-field form (``metrics``/``dimensions`` as lists alongside
   separate ``measure_columns``/``dimension_columns`` maps) is refused.
2. A list where a map belongs is refused.
3. A key whose column is empty is refused -- including a dimension whose physical
   column happens to match its DJ column name, which is written out rather than
   left blank.

The surfaces differ only in wording: a message has to name what the author is
looking at (a spec file, which has a ``name``, versus a request body) and show the
shape in the syntax they are writing (YAML versus JSON). That is all
:class:`BindingSurface` carries. The messages are the migration path for anyone
holding an existing file or an existing API client, so each one says what was
found, what to write instead, and shows the shape concretely.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from datajunction_server.errors import (
    DJException,
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
)

# Each retired field and the map that replaced it.
REPLACED_FIELDS: dict[str, str] = {
    "measure_columns": "metrics",
    "dimension_columns": "dimensions",
}

# One entry of each map, singular, for prose.
AXIS_NOUN: dict[str, str] = {
    "metrics": "metric",
    "dimensions": "dimension",
}

# Why a missing column can't be defaulted, per axis.
UNBOUND_REASONS: dict[str, str] = {
    "metrics": (
        "A measure's DJ-side name is auto-generated with an expression-hash "
        "suffix, so there is no name to fall back on."
    ),
    "dimensions": (
        "Write the column out even when it matches the DJ column name, so the "
        "declaration says what the table actually holds."
    ),
}

# The same two metrics and two dimensions, spelled for each surface. A dimension
# stored under a different name (page) and one stored under the name DJ uses
# (country_iso_code) are both shown, since the second is the one people leave out.
YAML_EXAMPLE = """metrics:
  default.view_secs: view_secs_sum
  default.session_count: session_cnt
dimensions:
  default.page_d.page_id: page
  default.geo_country_d.country_iso_code: country_iso_code"""

JSON_EXAMPLE = """{
  "metrics": {
    "default.view_secs": "view_secs_sum",
    "default.session_count": "session_cnt"
  },
  "dimensions": {
    "default.page_d.page_id": "page",
    "default.geo_country_d.country_iso_code": "country_iso_code"
  }
}"""


@dataclass(frozen=True)
class BindingSurface:
    """
    How the rules below address one surface: what to call the declaration, the
    example that shows it the way the author writes it, and the exception to raise.
    """

    # Built from the raw input, since a spec names itself and a request body doesn't.
    subject: Callable[[dict[str, Any]], str]
    example: str
    exception: type[DJException]


SPEC_SURFACE = BindingSurface(
    subject=lambda data: f"Pre-aggregation '{data.get('name')}'",
    example=YAML_EXAMPLE,
    exception=DJInvalidDeploymentConfig,
)

REQUEST_SURFACE = BindingSurface(
    subject=lambda data: "The POST /preaggs/register request body",
    example=JSON_EXAMPLE,
    exception=DJInvalidInputException,
)


def validate_column_bindings(data: Any, surface: BindingSurface) -> Any:
    """
    Hold a pre-aggregation declaration to the map form, for use as a Pydantic
    ``model_validator(mode="before")`` on either surface. Returns ``data``
    untouched when it passes; raises ``surface.exception`` otherwise.
    """
    if not isinstance(data, dict):
        return data

    subject = surface.subject(data)

    for removed, replacement in REPLACED_FIELDS.items():
        if removed in data:
            raise surface.exception(
                message=(
                    f"{subject} declares `{removed}`, which is no longer a "
                    f"pre-aggregation field. Declare each "
                    f"{AXIS_NOUN[replacement]} together with the physical column "
                    f"of the external table that holds it, under `{replacement}`, "
                    f"and drop the `{removed}` block. The map form looks like "
                    f"this:\n\n{surface.example}"
                ),
            )

    for field, reason in UNBOUND_REASONS.items():
        value = data.get(field)
        if value is None:
            continue
        if not isinstance(value, dict):
            raise surface.exception(
                message=(
                    f"{subject} declares `{field}` as a {type(value).__name__}, "
                    f"not a map. `{field}` maps each {AXIS_NOUN[field]} reference "
                    f"to the physical column of the external table that holds it. "
                    f"The map form looks like this:\n\n{surface.example}"
                ),
            )
        unbound = [
            reference for reference, column in value.items() if column is None
        ]
        if unbound:
            raise surface.exception(
                message=(
                    f"{subject} leaves the physical column empty under `{field}` "
                    f"for {unbound}. {reason} The map form looks like "
                    f"this:\n\n{surface.example}"
                ),
            )
    return data
