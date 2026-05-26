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

from pydantic import BaseModel, ConfigDict, Discriminator, Tag, model_validator
from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB

if TYPE_CHECKING:
    from pydantic import TypeAdapter

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

    # Frozen so shared singletons (e.g., the legacy translation table) are
    # safe to hand out without defensive copies.
    model_config = ConfigDict(frozen=True)

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

    model_config = ConfigDict(frozen=True)

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


def unit_to_dict(unit: "AtomicUnit | CompoundUnit | dict | None") -> dict | None:
    """
    Canonical JSON-friendly dict form of a Unit. Used by every storage and
    comparison path so the on-disk shape is stable regardless of input:

      - `mode="json"` so UnitKind enum members render as plain strings.
      - `exclude_none=True` so `code: None` doesn't appear on dimensionless
        kinds (unitless / percentage / proportion / count-without-code),
        keeping shapes consistent with the legacy translation table.

    Accepts either a Pydantic model or an already-dict input — the dict
    path is a no-op pass-through after a defensive copy.
    """
    if unit is None:
        return None
    if isinstance(unit, BaseModel):
        return unit.model_dump(mode="json", exclude_none=True)
    # Already a dict; normalize by stripping any None values one level deep
    # so a hand-rolled dict matches the canonical shape.
    if isinstance(unit, dict):
        return _strip_none(unit)
    raise TypeError(  # pragma: no cover
        f"unit_to_dict expected Unit | dict | None, got {type(unit).__name__}",
    )


def _strip_none(value: Any) -> Any:
    """Drop None-valued keys recursively from a dict; pass through otherwise."""
    if isinstance(value, dict):
        return {k: _strip_none(v) for k, v in value.items() if v is not None}
    return value


# Type adapter used by the SQLAlchemy TypeDecorator to validate JSONB rows
# into Unit instances on read. Built once at module load.
_UNIT_ADAPTER: "TypeAdapter[Unit]" = None  # type: ignore[assignment]


def _get_unit_adapter():
    """Lazy accessor — defer TypeAdapter construction until first use so
    importing `unit.py` stays cheap and avoids any circular-import risk."""
    global _UNIT_ADAPTER
    if _UNIT_ADAPTER is None:
        from pydantic import TypeAdapter

        _UNIT_ADAPTER = TypeAdapter(Unit)
    return _UNIT_ADAPTER


class UnitTypeDecorator(TypeDecorator):
    """
    SQLAlchemy TypeDecorator that bridges the structured ``Unit`` pydantic
    model and the underlying JSONB storage for ``column.unit``.

    On write: accepts a ``Unit`` instance or a raw dict and normalizes to
    the canonical JSONB shape via ``unit_to_dict``.

    On read: validates the JSONB dict into a ``Unit`` (``AtomicUnit`` or
    ``CompoundUnit``) so callers get typed access — ``.kind``, ``.code``,
    ``.label()`` — without stringly-typed dict lookups.

    Mirrors the existing ``ColumnTypeDecorator`` pattern. Cheaper than the
    SQL-type variant because the decoded value is small and Pydantic 2's
    Rust-backed validator handles it in microseconds.
    """

    impl = JSONB
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return unit_to_dict(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return _get_unit_adapter().validate_python(value)


# -------------------------------------------------------------------------
# Legacy <-> structured translation.
#
# The legacy `MetricUnit` enum (datajunction_server.models.node.MetricUnit)
# was a flat one-per-(kind, denomination) enumeration applied only to metric
# nodes. The new structured `Unit` lives on every column. These functions
# translate between the two so that:
#   - existing YAML / API input using the legacy `unit: <flat string>` keeps
#     parsing into the canonical `column.unit` storage.
#   - the legacy `metricmetadata.unit` DB column can be dual-written from
#     `column.unit` to keep API consumers reading the legacy field happy.
#   - the legacy `metric_metadata.unit` API field can be derived from
#     `column.unit` for downstream consumers (the reverse function lands here
#     so it lives next to its inverse).
#
# Translation is intentionally lossy in the reverse direction: structured
# values the legacy enum can't represent (non-USD currencies, compound
# units, data sizes, count-with-code) map to None. The legacy column simply
# isn't populated for those.
# -------------------------------------------------------------------------

# Keyed by MetricUnit.name (not the enum member itself) so this module can
# avoid importing node.py at module load. Callers translate to/from the enum
# at the call site. Values are frozen Unit instances — safe to hand out
# without defensive copies.
_LEGACY_NAME_TO_STRUCTURED: "dict[str, AtomicUnit | None]" = {
    "UNKNOWN": None,
    "UNITLESS": AtomicUnit(kind=UnitKind.UNITLESS),
    "PERCENTAGE": AtomicUnit(kind=UnitKind.PERCENTAGE),
    "PROPORTION": AtomicUnit(kind=UnitKind.PROPORTION),
    "DOLLAR": AtomicUnit(kind=UnitKind.CURRENCY, code="USD"),
    "MILLISECOND": AtomicUnit(kind=UnitKind.TIME, code="ms"),
    "SECOND": AtomicUnit(kind=UnitKind.TIME, code="s"),
    "MINUTE": AtomicUnit(kind=UnitKind.TIME, code="min"),
    "HOUR": AtomicUnit(kind=UnitKind.TIME, code="h"),
    "DAY": AtomicUnit(kind=UnitKind.TIME, code="d"),
    "WEEK": AtomicUnit(kind=UnitKind.TIME, code="wk"),
    "MONTH": AtomicUnit(kind=UnitKind.TIME, code="mo"),
    "YEAR": AtomicUnit(kind=UnitKind.TIME, code="yr"),
    "BYTE": AtomicUnit(kind=UnitKind.DATA_SIZE, code="B"),
    # BIT has no entry in DATA_SIZE_CODES (which uses byte-based units like
    # B, KB, MB, ... and their binary cousins KiB, MiB). Bits are atypical
    # in BI / data-platform metrics; if a user appears, add "b" to
    # DATA_SIZE_CODES and {"BIT": AtomicUnit(kind=DATA_SIZE, code="b")} here.
}


def legacy_unit_to_structured(
    legacy: "MetricUnit | None",
) -> "AtomicUnit | None":
    """
    Translate a legacy `MetricUnit` enum value into a structured `Unit`.

    Returns None for `MetricUnit.UNKNOWN` and for `None`, since both mean
    "no unit set." Returns `AtomicUnit(kind=unitless)` for
    `MetricUnit.UNITLESS`, preserving the distinction between "explicitly
    no unit" and "not set."
    """
    if legacy is None:
        return None
    return _LEGACY_NAME_TO_STRUCTURED.get(legacy.name)


def structured_to_legacy_unit(
    unit: "AtomicUnit | CompoundUnit | dict | None",
) -> "MetricUnit | None":
    """
    Translate a structured `Unit` (Pydantic model or already-dict) back to
    the legacy `MetricUnit` enum value when expressible. Returns None when
    the structured value has no legacy equivalent (non-USD currencies,
    compound units, data sizes other than BYTE, count with code, etc.) —
    callers should treat that as "don't populate the legacy column."

    `MetricUnit` is imported locally to keep `models/unit.py` free of a
    module-top dependency on `models/node.py`, where `MetricUnit` lives.
    """
    # Local import: `models/node.py` imports `unit_to_dict` from this module
    # at module-load, so a top-level reverse import would cycle.
    from datajunction_server.models.node import MetricUnit

    if unit is None:
        return None
    # Normalize dict input to a Unit instance for typed attribute access.
    # A malformed dict (e.g., an unknown time code that slipped through a
    # direct DB write) has no legacy equivalent — treat it as unmapped.
    if isinstance(unit, dict):
        try:
            unit = _get_unit_adapter().validate_python(unit)
        except Exception:
            return None
    if isinstance(unit, CompoundUnit):
        return None
    assert isinstance(unit, AtomicUnit)  # narrowed by isinstance above + adapter
    kind = unit.kind
    code = unit.code
    if kind == UnitKind.UNITLESS:
        return MetricUnit.UNITLESS
    if kind == UnitKind.PERCENTAGE:
        return MetricUnit.PERCENTAGE
    if kind == UnitKind.PROPORTION:
        return MetricUnit.PROPORTION
    if kind == UnitKind.CURRENCY:
        return MetricUnit.DOLLAR if code == "USD" else None
    if kind in (UnitKind.TIME, UnitKind.DATA_SIZE):
        # Reverse of _LEGACY_NAME_TO_STRUCTURED — direct equality on
        # frozen Unit instances.
        for legacy_name, structured in _LEGACY_NAME_TO_STRUCTURED.items():
            if structured == unit:
                return MetricUnit[legacy_name]
        return None
    # count — free-form code, no legacy equivalent.
    return None
