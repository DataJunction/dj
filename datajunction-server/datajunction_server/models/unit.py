"""
Column-level unit model.

Supports atomic units (a kind with an optional code, e.g. currency/USD,
time/ms, percentage) and compound units (a numerator atomic unit over a
denominator atomic unit, e.g. clicks per second).

Stored as JSONB on `column.unit`. Validated at the Pydantic layer.
"""

from enum import Enum
from typing import Annotated, Any, Union

from pydantic import BaseModel, Discriminator, Tag, model_validator


class UnitKind(str, Enum):
    """
    The kind of an atomic unit. Kept small and curated: only kinds DJ either
    has behavior for or is likely to want behavior for.
    """

    CURRENCY = "currency"
    TIME = "time"
    PERCENTAGE = "percentage"
    PROPORTION = "proportion"
    COUNT = "count"
    UNITLESS = "unitless"


# Closed sets used by AtomicUnit validators. Extending these is a one-line
# code change and should not require a migration.
CURRENCY_CODES: frozenset[str] = frozenset(
    {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY"},
)

TIME_CODES: frozenset[str] = frozenset(
    {"ms", "s", "min", "h", "d", "wk", "mo", "yr"},
)

# Display helpers — abbreviation and label per (kind, code).
_TIME_LABELS: dict[str, tuple[str, str]] = {
    "ms": ("ms", "Millisecond"),
    "s": ("s", "Second"),
    "min": ("m", "Minute"),
    "h": ("h", "Hour"),
    "d": ("d", "Day"),
    "wk": ("w", "Week"),
    "mo": ("mo", "Month"),
    "yr": ("y", "Year"),
}

_CURRENCY_SYMBOLS: dict[str, str] = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CAD": "CA$",
    "AUD": "A$",
    "CHF": "CHF",
    "CNY": "¥",
}


class AtomicUnit(BaseModel):
    """
    A single unit: a kind, with an optional code that further specifies the
    instance (currency code, time scale, count label, etc.).
    """

    kind: UnitKind
    code: str | None = None

    @model_validator(mode="after")
    def _validate_code_for_kind(self) -> "AtomicUnit":
        if self.kind == UnitKind.CURRENCY:
            if self.code is None:
                # Currency with no code is allowed — represents
                # "denomination unknown" or row-typed by a sibling column.
                return self
            if self.code not in CURRENCY_CODES:
                raise ValueError(
                    f"Unsupported currency code {self.code!r}. "
                    f"Supported: {sorted(CURRENCY_CODES)}",
                )
        elif self.kind == UnitKind.TIME:
            if self.code is None or self.code not in TIME_CODES:
                raise ValueError(
                    f"Time unit requires a code in {sorted(TIME_CODES)}; "
                    f"got {self.code!r}",
                )
        elif self.kind == UnitKind.COUNT:
            # code is free-form: e.g. "clicks", "impressions". May be None.
            pass
        else:
            # percentage, proportion, unitless — code must be None.
            if self.code is not None:
                raise ValueError(
                    f"Unit kind {self.kind.value!r} does not accept a code; "
                    f"got {self.code!r}",
                )
        return self

    def abbreviation(self) -> str:
        """Short symbol for display (e.g. '$', '%', 'ms', 'clicks')."""
        if self.kind == UnitKind.CURRENCY:
            return _CURRENCY_SYMBOLS.get(self.code or "", self.code or "")
        if self.kind == UnitKind.TIME and self.code in _TIME_LABELS:
            return _TIME_LABELS[self.code][0]
        if self.kind == UnitKind.PERCENTAGE:
            return "%"
        if self.kind == UnitKind.PROPORTION:
            return ""
        if self.kind == UnitKind.COUNT:
            return self.code or ""
        return ""

    def label(self) -> str:
        """Human-readable label for display."""
        if self.kind == UnitKind.CURRENCY:
            return self.code or "Currency"
        if self.kind == UnitKind.TIME and self.code in _TIME_LABELS:
            return _TIME_LABELS[self.code][1]
        if self.kind == UnitKind.PERCENTAGE:
            return "Percentage"
        if self.kind == UnitKind.PROPORTION:
            return "Proportion"
        if self.kind == UnitKind.COUNT:
            return self.code or "Count"
        if self.kind == UnitKind.UNITLESS:
            return "Unitless"
        return ""


class CompoundUnit(BaseModel):
    """
    A unit shaped as numerator / denominator, for rate-like quantities
    (CTR = clicks/impressions, QPS = queries/second, throughput = bytes/second).
    """

    numerator: AtomicUnit
    denominator: AtomicUnit

    def abbreviation(self) -> str:
        return (
            f"{self.numerator.abbreviation()}/{self.denominator.abbreviation()}".strip(
                "/",
            )
        )

    def label(self) -> str:
        return f"{self.numerator.label()} per {self.denominator.label()}"


def _unit_discriminator(value: Any) -> str:
    """Pick the Unit variant based on the presence of `numerator`."""
    if isinstance(value, dict):
        return "compound" if "numerator" in value else "atomic"
    return "compound" if isinstance(value, CompoundUnit) else "atomic"


Unit = Annotated[
    Union[
        Annotated[AtomicUnit, Tag("atomic")],
        Annotated[CompoundUnit, Tag("compound")],
    ],
    Discriminator(_unit_discriminator),
]
