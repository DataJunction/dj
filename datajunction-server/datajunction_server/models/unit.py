"""
Column-level unit model.

Supports atomic units (a kind with an optional code, e.g. currency/USD,
time/ms, percentage) and compound units (a numerator atomic unit over a
denominator atomic unit, e.g. clicks per second).

Stored as JSONB on `column.unit`. Validated at the Pydantic layer.
"""

import re
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, Union

from pydantic import BaseModel, Discriminator, Tag, model_validator

if TYPE_CHECKING:
    from datajunction_server.models.node import MetricUnit


class UnitKind(str, Enum):
    """
    The kind of an atomic unit. Kept small and curated: only kinds DJ either
    has behavior for or is likely to want behavior for.
    """

    CURRENCY = "currency"
    TIME = "time"
    DATA_SIZE = "data_size"
    PERCENTAGE = "percentage"
    PROPORTION = "proportion"
    COUNT = "count"
    UNITLESS = "unitless"


# Currency codes follow ISO 4217 (3 uppercase letters). DJ does not own the
# currency vocabulary — any conforming code is accepted; the regex catches
# typos (lowercase, wrong length, non-letters) without keeping a frozen list
# that would need updates per release.
_CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")

# Closed sets where the universe is small and inputs are easy to mistype.
TIME_CODES: frozenset[str] = frozenset(
    {"ms", "s", "min", "h", "d", "wk", "mo", "yr"},
)

# Data size codes: base-10 (KB, MB, GB, TB, PB) and base-2 (KiB, MiB, GiB, TiB).
DATA_SIZE_CODES: frozenset[str] = frozenset(
    {"B", "KB", "MB", "GB", "TB", "PB", "KiB", "MiB", "GiB", "TiB"},
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

_DATA_SIZE_LABELS: dict[str, str] = {
    "B": "Byte",
    "KB": "Kilobyte",
    "MB": "Megabyte",
    "GB": "Gigabyte",
    "TB": "Terabyte",
    "PB": "Petabyte",
    "KiB": "Kibibyte",
    "MiB": "Mebibyte",
    "GiB": "Gibibyte",
    "TiB": "Tebibyte",
}

# Symbols for common currency codes. Anything not listed falls back to the
# code itself (e.g. "SEK", "INR") which is the standard rendering when no
# locale-specific symbol is available.
_CURRENCY_SYMBOLS: dict[str, str] = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CAD": "CA$",
    "AUD": "A$",
    "CHF": "CHF",
    "CNY": "¥",
    "INR": "₹",
    "KRW": "₩",
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
            if not _CURRENCY_CODE_RE.match(self.code):
                raise ValueError(
                    f"Currency code {self.code!r} must be ISO 4217 "
                    "(three uppercase letters, e.g. 'USD').",
                )
        elif self.kind == UnitKind.TIME:
            if self.code is None or self.code not in TIME_CODES:
                raise ValueError(
                    f"Time unit requires a code in {sorted(TIME_CODES)}; "
                    f"got {self.code!r}",
                )
        elif self.kind == UnitKind.DATA_SIZE:
            if self.code is None or self.code not in DATA_SIZE_CODES:
                raise ValueError(
                    f"Data size unit requires a code in {sorted(DATA_SIZE_CODES)}; "
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
        if self.kind == UnitKind.DATA_SIZE:
            return self.code or ""
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
        if self.kind == UnitKind.DATA_SIZE:
            return _DATA_SIZE_LABELS.get(self.code or "", self.code or "Data size")
        if self.kind == UnitKind.PERCENTAGE:
            return "Percentage"
        if self.kind == UnitKind.PROPORTION:
            return "Proportion"
        if self.kind == UnitKind.COUNT:
            return self.code or "Count"
        if self.kind == UnitKind.UNITLESS:
            return "Unitless"
        return ""  # pragma: no cover  (unreachable; every UnitKind handled above)


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


# -------------------------------------------------------------------------
# Legacy <-> structured translation.
#
# The legacy `MetricUnit` enum (datajunction_server.models.node.MetricUnit)
# was a flat one-per-(kind, denomination) enumeration applied only to metric
# nodes. The new structured `Unit` lives on every column. These functions
# translate between the two so that:
#   - existing YAML / API input using the legacy `unit: <flat string>` keeps
#     working (PR 2 wires this on the input side).
#   - the legacy `metricmetadata.unit` DB column can be dual-written from
#     `column.unit` for rollback safety (PR 2 wires this on the storage side).
#   - the legacy `metric_metadata.unit` API field can be derived from
#     `column.unit` for downstream consumers (PR 4 wires this on the output
#     side; the reverse function lands here so it lives next to its inverse).
#
# Translation is intentionally lossy in the reverse direction: structured
# values the legacy enum can't represent (non-USD currencies, compound
# units, data sizes, count-with-code) map to None. The legacy column simply
# isn't populated for those.
# -------------------------------------------------------------------------

# Keyed by MetricUnit.name (not the enum member itself) so this module can
# avoid importing node.py at module load. Callers translate to/from the enum
# at the call site.
_LEGACY_NAME_TO_STRUCTURED: dict[str, dict | None] = {
    "UNKNOWN": None,
    "UNITLESS": {"kind": "unitless"},
    "PERCENTAGE": {"kind": "percentage"},
    "PROPORTION": {"kind": "proportion"},
    "DOLLAR": {"kind": "currency", "code": "USD"},
    "MILLISECOND": {"kind": "time", "code": "ms"},
    "SECOND": {"kind": "time", "code": "s"},
    "MINUTE": {"kind": "time", "code": "min"},
    "HOUR": {"kind": "time", "code": "h"},
    "DAY": {"kind": "time", "code": "d"},
    "WEEK": {"kind": "time", "code": "wk"},
    "MONTH": {"kind": "time", "code": "mo"},
    "YEAR": {"kind": "time", "code": "yr"},
    # BIT, BYTE intentionally omitted — unused in production data.
}


def legacy_unit_to_structured(
    legacy: "MetricUnit | None",
) -> dict | None:
    """
    Translate a legacy `MetricUnit` enum value into a structured `Unit` dict.

    Returns None for `MetricUnit.UNKNOWN` and for `None`, since both mean
    "no unit set." Returns `{kind: unitless}` for `MetricUnit.UNITLESS`,
    preserving the distinction between "explicitly no unit" and "not set."
    """
    if legacy is None:
        return None
    return _LEGACY_NAME_TO_STRUCTURED.get(legacy.name)


def structured_to_legacy_unit_name(unit: dict | None) -> str | None:
    """
    Translate a structured `Unit` dict back to the legacy `MetricUnit.name`
    when expressible. Returns None when the structured value has no legacy
    equivalent (non-USD currencies, compound units, data sizes, count with
    code, etc.) — callers should treat that as "don't populate the legacy
    column."

    Returns the enum member name (e.g. "DOLLAR"), not a MetricUnit instance,
    so this module stays import-free of node.py. Callers do
    `MetricUnit[name]` at the call site.
    """
    if unit is None:
        return None
    # Compound units have no legacy equivalent.
    if "numerator" in unit:
        return None
    kind = unit.get("kind")
    code = unit.get("code")
    if kind == "unitless":
        return "UNITLESS"
    if kind == "percentage":
        return "PERCENTAGE"
    if kind == "proportion":
        return "PROPORTION"
    if kind == "currency":
        return "DOLLAR" if code == "USD" else None
    if kind == "time":
        # Reverse of _LEGACY_NAME_TO_STRUCTURED for the time kind.
        for legacy_name, structured in _LEGACY_NAME_TO_STRUCTURED.items():
            if structured == {"kind": "time", "code": code}:
                return legacy_name
        return None
    # count, data_size — no legacy equivalent.
    return None
