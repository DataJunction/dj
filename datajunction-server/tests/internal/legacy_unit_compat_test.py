"""
End-to-end compatibility test for the legacy flat-string `unit:` form
(e.g. `unit: dollar`) — the shape every metric YAML used before the unit
lift to `column.unit`. Verifies:

  1. The YAML parses through `MetricSpec` without errors.
  2. The orchestrator's reconcile + derive helpers populate both the
     canonical `column.unit` (structured) and the legacy `metricmetadata.unit`
     (enum) consistently.
  3. The reverse direction — reading from DB state back into a spec —
     reproduces the original legacy `unit: dollar` shape (not the structured
     dict), preserving the user's authoring intent for round-trip stability.
  4. `node_spec_to_yaml` emits `unit: dollar` (legacy string), not a
     structured dict, when the value is legacy-expressible.
"""

from unittest.mock import MagicMock

import pytest
import yaml

from datajunction_server.database.column import Column
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
)
from datajunction_server.internal.namespaces import node_spec_to_yaml
from datajunction_server.models.deployment import MetricSpec
from datajunction_server.models.node import MetricUnit
from datajunction_server.models.unit import (
    legacy_unit_to_structured,
    structured_to_legacy_unit_name,
)


# A representative legacy metric YAML — flat-string `unit:` at the metric
# spec top level, no `columns:` block, no structured unit. This is the
# shape every existing metric YAML uses.
LEGACY_METRIC_YAML = """\
name: legacy_compat.total_revenue
node_type: metric
display_name: Total Revenue (Legacy)
description: Sum of all transaction amounts (authored with legacy unit field).
direction: higher_is_better
unit: dollar
significant_digits: 2
query: SELECT SUM(amount) FROM legacy_compat.transactions
"""


def _make_col(unit: dict | None = None) -> Column:
    """Minimal Column constructed without a session (matches existing helper tests)."""
    return Column(name="value", type=None, order=0, unit=unit)


class TestLegacyYamlEndToEnd:
    """
    End-to-end: legacy YAML → MetricSpec → orchestrator helpers → DB state
    → MetricSpec (round-trip via to_spec equivalent) → YAML export.
    """

    @pytest.fixture
    def spec(self) -> MetricSpec:
        return MetricSpec(**yaml.safe_load(LEGACY_METRIC_YAML))

    def test_parses_into_unit_enum_not_structured(self, spec: MetricSpec):
        """Legacy YAML routes to unit_enum; structured field remains None."""
        assert spec.unit_enum == MetricUnit.DOLLAR
        assert spec.unit_structured is None
        # `spec.unit` returns the legacy string form for back-compat output.
        assert spec.unit == "dollar"

    def test_other_metric_fields_preserved(self, spec: MetricSpec):
        """Non-unit metric fields still parse correctly alongside the legacy unit."""
        assert spec.rendered_name == "legacy_compat.total_revenue"
        assert spec.direction is not None
        assert spec.direction.value == "higher_is_better"
        assert spec.significant_digits == 2

    def test_orchestrator_reconciles_legacy_onto_column(self, spec: MetricSpec):
        """The reconcile step writes the structured value to columns[0].unit."""
        col = _make_col()
        DeploymentOrchestrator._reconcile_metric_unit(MagicMock(), spec, col)
        # The legacy DOLLAR gets translated into the structured form on the column.
        assert col.unit == {"kind": "currency", "code": "USD"}

    def test_orchestrator_derives_legacy_for_storage(self, spec: MetricSpec):
        """After reconcile, the derive step returns MetricUnit.DOLLAR so the legacy
        DB column is dual-written. This is what protects API consumers reading
        `metric_metadata.unit`."""
        col = _make_col()
        DeploymentOrchestrator._reconcile_metric_unit(MagicMock(), spec, col)
        legacy_enum = DeploymentOrchestrator._derive_legacy_unit_for_storage(
            MagicMock(),
            spec,
            col,
        )
        assert legacy_enum == MetricUnit.DOLLAR

    def test_translation_round_trips(self, spec: MetricSpec):
        """Legacy → structured → legacy is the identity for every value in the
        migration table — guarantees both halves of the bridge agree."""
        structured = legacy_unit_to_structured(spec.unit_enum)
        assert structured == {"kind": "currency", "code": "USD"}
        legacy_name = structured_to_legacy_unit_name(structured)
        assert legacy_name == "DOLLAR"
        assert MetricUnit[legacy_name] == spec.unit_enum

    def test_yaml_export_emits_legacy_string(self, spec: MetricSpec):
        """node_spec_to_yaml emits `unit: dollar` (string), not a structured
        dict — preserves the author's original YAML shape on re-export when
        the value is legacy-expressible."""
        output = node_spec_to_yaml(spec)
        assert "unit: dollar" in output
        # No structured form leaks into the export.
        assert "kind:" not in output
        assert "code:" not in output

    def test_yaml_export_does_not_introduce_columns_block(self, spec: MetricSpec):
        """A legacy metric YAML has no `columns:` block. After all the unit
        plumbing, re-export must NOT introduce one — that would be a churn
        diff on every legacy metric file."""
        output = node_spec_to_yaml(spec)
        assert "columns:" not in output


class TestAllLegacyUnitValuesRoundTrip:
    """
    Every legacy `MetricUnit` value that exists in production data
    (per the per-revision count we measured) must successfully:
      (a) parse from the legacy YAML form,
      (b) translate forward into a structured dict, and
      (c) translate back via the reverse table.

    BIT and BYTE are intentionally absent from the translation table (0 rows
    in production) — they map to None on forward translation, which is
    documented behavior.
    """

    @pytest.mark.parametrize(
        "legacy_str",
        [
            "unitless",
            "percentage",
            "proportion",
            "dollar",
            "millisecond",
            "second",
            "minute",
            "hour",
            "day",
            "week",
            "month",
            "year",
        ],
    )
    def test_legacy_value_round_trips(self, legacy_str: str):
        spec = MetricSpec(
            name="legacy_compat.m",
            query="SELECT 1",
            unit=legacy_str,
        )
        assert spec.unit_enum == MetricUnit[legacy_str.upper()]
        assert spec.unit_structured is None
        # Forward translation produces a structured value.
        structured = legacy_unit_to_structured(spec.unit_enum)
        assert structured is not None
        # Reverse translation returns the same enum member.
        assert structured_to_legacy_unit_name(structured) == legacy_str.upper()

    def test_unknown_legacy_value_rejected(self):
        """Typos in legacy YAML still raise a clear error, not a silent pass."""
        from datajunction_server.errors import DJInvalidInputException

        with pytest.raises(DJInvalidInputException, match="Invalid metric unit"):
            MetricSpec(
                name="legacy_compat.m",
                query="SELECT 1",
                unit="dollars",  # plural typo
            )

    def test_resolve_metric_unit_for_spec_no_column_unit(self):
        """Helper: no structured column unit → emit only legacy enum."""
        from datajunction_server.database.node import _resolve_metric_unit_for_spec

        legacy, structured = _resolve_metric_unit_for_spec(
            col_unit=None,
            legacy_from_md=MetricUnit.DOLLAR,
        )
        assert legacy == MetricUnit.DOLLAR
        assert structured is None

    def test_resolve_metric_unit_for_spec_legacy_expressible_keeps_legacy(self):
        """Helper: structured is legacy-expressible → keep legacy field, structured stays None.

        Preserves round-trip stability: an author who wrote `unit: dollar` sees
        `unit: dollar` back, not `unit: {kind: currency, code: USD}`.
        """
        from datajunction_server.database.node import _resolve_metric_unit_for_spec

        legacy, structured = _resolve_metric_unit_for_spec(
            col_unit={"kind": "currency", "code": "USD"},
            legacy_from_md=MetricUnit.DOLLAR,
        )
        assert legacy == MetricUnit.DOLLAR
        assert structured is None

    @pytest.mark.parametrize(
        "col_unit",
        [
            {"kind": "currency", "code": "EUR"},
            {"kind": "data_size", "code": "MB"},
            {"kind": "count", "code": "clicks"},
            {
                "numerator": {"kind": "count"},
                "denominator": {"kind": "time", "code": "s"},
            },
        ],
    )
    def test_resolve_metric_unit_for_spec_structured_only(self, col_unit):
        """Helper: structured NOT legacy-expressible → populate structured, null legacy.

        Covers the round-trip path for EUR / compound / data_size /
        count-with-code, which have no MetricUnit enum equivalent. Without
        this branch, a metric authored with `unit: {kind: currency, code: EUR}`
        would lose its unit on re-export (since the legacy field would be
        null and the structured wouldn't be populated).
        """
        from datajunction_server.database.node import _resolve_metric_unit_for_spec

        legacy, structured = _resolve_metric_unit_for_spec(
            col_unit=col_unit,
            legacy_from_md=None,
        )
        assert legacy is None
        assert structured == col_unit

    def test_uppercase_legacy_input_accepted(self):
        """Legacy parser accepts case-insensitive input (existing behavior)."""
        spec = MetricSpec(
            name="legacy_compat.m",
            query="SELECT 1",
            unit="DOLLAR",
        )
        assert spec.unit_enum == MetricUnit.DOLLAR
