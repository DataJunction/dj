"""
Tests for ``datajunction_server.models.unit``.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from datajunction_server.models.deployment import ColumnSpec
from datajunction_server.models.unit import (
    AtomicUnit,
    CompoundUnit,
    Unit,
    UnitKind,
)

_unit_adapter: TypeAdapter[Unit] = TypeAdapter(Unit)


def _validate(value: dict) -> Unit:
    return _unit_adapter.validate_python(value)


class TestAtomicUnit:
    def test_currency_with_supported_code(self) -> None:
        u = AtomicUnit(kind=UnitKind.CURRENCY, code="USD")
        assert u.kind == UnitKind.CURRENCY
        assert u.code == "USD"
        assert u.abbreviation() == "$"
        assert u.label() == "USD"

    def test_currency_without_code_is_allowed(self) -> None:
        u = AtomicUnit(kind=UnitKind.CURRENCY, code=None)
        assert u.code is None
        assert u.label() == "Currency"

    def test_currency_rejects_unsupported_code(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AtomicUnit(kind=UnitKind.CURRENCY, code="USDX")
        assert "Unsupported currency code" in str(exc_info.value)

    def test_time_requires_supported_code(self) -> None:
        AtomicUnit(kind=UnitKind.TIME, code="ms")
        AtomicUnit(kind=UnitKind.TIME, code="s")
        AtomicUnit(kind=UnitKind.TIME, code="h")
        with pytest.raises(ValidationError):
            AtomicUnit(kind=UnitKind.TIME, code="seconds")
        with pytest.raises(ValidationError):
            AtomicUnit(kind=UnitKind.TIME, code=None)

    def test_count_accepts_free_form_code(self) -> None:
        u = AtomicUnit(kind=UnitKind.COUNT, code="clicks")
        assert u.code == "clicks"
        assert u.label() == "clicks"
        # None is also fine for count.
        AtomicUnit(kind=UnitKind.COUNT)

    @pytest.mark.parametrize(
        "kind",
        [UnitKind.PERCENTAGE, UnitKind.PROPORTION, UnitKind.UNITLESS],
    )
    def test_dimensionless_kinds_reject_code(self, kind: UnitKind) -> None:
        AtomicUnit(kind=kind)
        with pytest.raises(ValidationError) as exc_info:
            AtomicUnit(kind=kind, code="anything")
        assert "does not accept a code" in str(exc_info.value)

    def test_percentage_abbreviation_and_label(self) -> None:
        u = AtomicUnit(kind=UnitKind.PERCENTAGE)
        assert u.abbreviation() == "%"
        assert u.label() == "Percentage"

    def test_proportion_label(self) -> None:
        u = AtomicUnit(kind=UnitKind.PROPORTION)
        assert u.label() == "Proportion"

    def test_unitless_label(self) -> None:
        u = AtomicUnit(kind=UnitKind.UNITLESS)
        assert u.label() == "Unitless"


class TestCompoundUnit:
    def test_qps_compound(self) -> None:
        c = CompoundUnit(
            numerator=AtomicUnit(kind=UnitKind.COUNT),
            denominator=AtomicUnit(kind=UnitKind.TIME, code="s"),
        )
        assert c.label() == "Count per Second"

    def test_ctr_compound_with_labels(self) -> None:
        c = CompoundUnit(
            numerator=AtomicUnit(kind=UnitKind.COUNT, code="clicks"),
            denominator=AtomicUnit(kind=UnitKind.COUNT, code="impressions"),
        )
        assert c.label() == "clicks per impressions"
        assert c.abbreviation() == "clicks/impressions"

    def test_compound_validates_inner_units(self) -> None:
        # Numerator violates currency code rules — should fail at construction.
        with pytest.raises(ValidationError):
            CompoundUnit(
                numerator=AtomicUnit(kind=UnitKind.CURRENCY, code="USDX"),
                denominator=AtomicUnit(kind=UnitKind.TIME, code="s"),
            )


class TestUnitDiscriminator:
    def test_atomic_dict_dispatches_to_atomic(self) -> None:
        u = _validate({"kind": "currency", "code": "USD"})
        assert isinstance(u, AtomicUnit)
        assert u.kind == UnitKind.CURRENCY

    def test_compound_dict_dispatches_to_compound(self) -> None:
        u = _validate(
            {
                "numerator": {"kind": "count", "code": "clicks"},
                "denominator": {"kind": "time", "code": "s"},
            },
        )
        assert isinstance(u, CompoundUnit)
        assert u.numerator.code == "clicks"

    def test_atomic_without_code(self) -> None:
        u = _validate({"kind": "percentage"})
        assert isinstance(u, AtomicUnit)
        assert u.code is None

    def test_invalid_kind_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate({"kind": "not_a_kind"})


class TestColumnSpecUnit:
    def test_column_spec_accepts_unit_dict(self) -> None:
        spec = ColumnSpec(
            name="amount",
            type="DOUBLE",
            unit={"kind": "currency", "code": "USD"},
        )
        assert isinstance(spec.unit, AtomicUnit)
        assert spec.unit.code == "USD"

    def test_column_spec_accepts_compound_unit_dict(self) -> None:
        spec = ColumnSpec(
            name="qps",
            type="DOUBLE",
            unit={
                "numerator": {"kind": "count"},
                "denominator": {"kind": "time", "code": "s"},
            },
        )
        assert isinstance(spec.unit, CompoundUnit)

    def test_column_spec_unit_default_none(self) -> None:
        spec = ColumnSpec(name="x", type="INT")
        assert spec.unit is None

    def test_column_spec_eq_compares_unit(self) -> None:
        a = ColumnSpec(name="x", type="DOUBLE", unit={"kind": "percentage"})
        b = ColumnSpec(name="x", type="DOUBLE", unit={"kind": "percentage"})
        c = ColumnSpec(name="x", type="DOUBLE", unit={"kind": "proportion"})
        d = ColumnSpec(name="x", type="DOUBLE")
        assert a == b
        assert a != c
        assert a != d

    def test_column_spec_round_trip_via_dump(self) -> None:
        spec = ColumnSpec(
            name="rev",
            type="DOUBLE",
            unit={"kind": "currency", "code": "USD"},
        )
        dumped = spec.model_dump()
        assert dumped["unit"] == {"kind": "currency", "code": "USD"}
        roundtripped = ColumnSpec.model_validate(dumped)
        assert roundtripped == spec
