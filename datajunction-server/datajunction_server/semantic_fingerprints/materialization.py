"""Planner-derived materialization compatibility contracts."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import ValidationError

from datajunction_server.models.cube_materialization import (
    DruidCubeConfig,
    DruidCubeV3Config,
)
from datajunction_server.models.decompose import MetricComponent
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_value,
)
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.types import ColumnType

MATERIALIZATION_CONTRACT_VERSION = 1
MATERIALIZATION_PLANNER_EPOCH = 1


@dataclass(frozen=True)
class MaterializationContract:
    """Storage shape and aggregate state emitted by the cube planner."""

    version: int
    planner_epoch: int
    digest: str

    def to_dict(self) -> dict[str, int | str]:
        """Serialize the compact manifest stored with a materialization."""
        return {
            "version": self.version,
            "planner_epoch": self.planner_epoch,
            "digest": self.digest,
        }

    @classmethod
    def from_value(cls, value: object) -> MaterializationContract | None:
        """Read a stored manifest, treating malformed and legacy data as unknown."""
        if not isinstance(value, Mapping):
            return None
        version = value.get("version")
        planner_epoch = value.get("planner_epoch")
        digest = value.get("digest")
        if (
            set(value) != {"version", "planner_epoch", "digest"}
            or isinstance(version, bool)
            or not isinstance(version, int)
            or isinstance(planner_epoch, bool)
            or not isinstance(planner_epoch, int)
            or not isinstance(digest, str)
            or not _is_sha256_digest(digest)
        ):
            return None
        return cls(version=version, planner_epoch=planner_epoch, digest=digest)


@dataclass(frozen=True)
class MaterializationFingerprint:
    """Known semantic and storage fingerprints for one materialization plan."""

    semantic: SemanticFingerprint
    storage: MaterializationContract

    def __post_init__(self) -> None:
        if self.semantic.version != 2:
            raise ValueError("Materialization fingerprints require semantic v2")
        if not _current_storage_contract(self.storage):
            raise ValueError("Materialization storage contract is unknown")

    def to_dict(self) -> dict[str, object]:
        """Serialize the complete known manifest stored with a materialization."""
        if self.semantic.version != 2 or not _current_storage_contract(self.storage):
            raise ValueError("Materialization fingerprint is no longer current")
        return {
            "semantic": self.semantic.model_dump(mode="json"),
            "storage": self.storage.to_dict(),
        }

    @classmethod
    def from_value(cls, value: object) -> MaterializationFingerprint | None:
        """Read only complete, current v2 manifests; legacy data is unknown."""
        if not isinstance(value, Mapping) or set(value) != {"semantic", "storage"}:
            return None
        semantic_value = value.get("semantic")
        semantic_version = (
            semantic_value.get("version")
            if isinstance(semantic_value, Mapping)
            else None
        )
        if (
            not isinstance(semantic_value, Mapping)
            or set(semantic_value) != {"version", "digest"}
            or isinstance(semantic_version, bool)
            or not isinstance(semantic_version, int)
            or semantic_version != 2
        ):
            return None
        try:
            semantic = SemanticFingerprint.model_validate(semantic_value)
        except (ValidationError, TypeError, ValueError):
            return None
        storage = MaterializationContract.from_value(value.get("storage"))
        if storage is None or not _current_storage_contract(storage):
            return None
        return cls(semantic=semantic, storage=storage)


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _current_storage_contract(
    storage: MaterializationContract | None,
) -> bool:
    return bool(
        storage is not None
        and storage.version == MATERIALIZATION_CONTRACT_VERSION
        and storage.planner_epoch == MATERIALIZATION_PLANNER_EPOCH
        and _is_sha256_digest(storage.digest)
    )


def _normalized_type(value: str) -> str:
    return str(ColumnType.validate(value))


def _column_payload(column: ColumnMetadata) -> dict[str, str | None]:
    return {
        "name": column.name,
        "semantic_entity": column.semantic_entity,
        "semantic_type": column.semantic_type,
        "type": _normalized_type(column.type),
    }


def _component_payload(component: MetricComponent) -> dict[str, object]:
    return {
        "name": component.name,
        "aggregation": component.aggregation,
        "merge": component.merge,
        "params": normalize_value(component.params or {}),
        "merge_args": component.merge_args,
        "serialize": component.serialize,
        "serialize_targets": sorted(
            target.value for target in component.serialize_targets
        ),
        "serialize_type": (
            _normalized_type(component.serialize_type)
            if component.serialize_type
            else None
        ),
        "aggregability": component.rule.type.value,
        "level": sorted(component.rule.level or []),
        "fixed_grain": sorted(component.rule.fixed_grain or []),
        "reaggregate": normalize_value(
            component.rule.reaggregate.model_dump(mode="json")
        )
        if component.rule.reaggregate
        else None,
    }


def _stage_payload(
    *,
    role: str,
    columns: list[ColumnMetadata],
    grain: list[str],
    dimensions: list[str],
    measures: list[MetricComponent],
    timestamp_column: str | None,
    timestamp_format: str | None,
    granularity: Granularity | None,
) -> dict[str, object]:
    column_payloads = [_column_payload(column) for column in columns]
    column_names = [column["name"] for column in column_payloads]
    if len(set(column_names)) != len(column_names):
        raise ValueError("Materialization output has duplicate column names")
    return {
        "role": role,
        "columns": sorted(column_payloads, key=canonical_json),
        "grain": sorted(set(grain)),
        "dimensions": sorted(set(dimensions)),
        "measures": sorted(
            (_component_payload(measure) for measure in measures),
            key=canonical_json,
        ),
        "timestamp_column": timestamp_column,
        "timestamp_format": timestamp_format,
        "granularity": granularity.value if granularity is not None else None,
    }


def materialization_contract(
    config: DruidCubeConfig | DruidCubeV3Config,
    *,
    target: str = "druid",
) -> MaterializationContract | None:
    """Project a generated cube materialization config to its storage contract.

    The contract uses the planner's emitted grain and columns. V3 configs describe
    the final cube artifact; pre-aggregation reuse needs its own stored contract.
    Revision IDs, generated table names, SQL text and presentation fields are omitted.
    """
    try:
        if isinstance(config, DruidCubeConfig):
            if config.druid or any(stage.druid_overrides for stage in config.combiners):
                return None
            stages = [
                _stage_payload(
                    role="measure",
                    columns=stage.columns,
                    grain=stage.grain,
                    dimensions=stage.dimensions,
                    measures=stage.measures,
                    timestamp_column=stage.timestamp_column,
                    timestamp_format=stage.timestamp_format,
                    granularity=stage.granularity,
                )
                for stage in config.measures_materializations
            ]
            stages.extend(
                _stage_payload(
                    role="combiner",
                    columns=stage.columns,
                    grain=stage.grain,
                    dimensions=stage.dimensions,
                    measures=stage.measures,
                    timestamp_column=stage.timestamp_column,
                    timestamp_format=stage.timestamp_format,
                    granularity=stage.granularity,
                )
                for stage in config.combiners
            )
        elif isinstance(config, DruidCubeV3Config):
            druid_spec = getattr(config, "druid_spec", None)
            if not isinstance(druid_spec, Mapping):
                return None
            data_schema = druid_spec.get("dataSchema")
            if not isinstance(data_schema, Mapping):
                return None
            granularity_spec = data_schema.get("granularitySpec")
            parser = data_schema.get("parser")
            if (
                not isinstance(granularity_spec, Mapping)
                or not isinstance(granularity_spec.get("segmentGranularity"), str)
                or not isinstance(data_schema.get("dataSource"), str)
                or not isinstance(parser, Mapping)
                or not isinstance(parser.get("parseSpec"), Mapping)
                or not isinstance(data_schema.get("metricsSpec"), list)
            ):
                return None
            data_schema_payload = dict(data_schema)
            data_schema_payload.pop("dataSource", None)
            granularity_payload = dict(granularity_spec)
            granularity_payload.pop("intervals", None)
            data_schema_payload["granularitySpec"] = granularity_payload
            stages = [
                _stage_payload(
                    role="cube",
                    columns=config.combined_columns,
                    grain=config.combined_grain,
                    dimensions=config.combined_grain,
                    measures=config.measure_components,
                    timestamp_column=config.timestamp_column,
                    timestamp_format=config.timestamp_format,
                    granularity=None,
                ),
            ]
            stages[0]["component_aliases"] = {
                key: config.component_aliases[key]
                for key in sorted(config.component_aliases)
            }
            stages[0]["druid_data_schema"] = normalize_value(data_schema_payload)
        else:  # pragma: no cover
            return None
        if not stages or any(
            not stage["columns"] for stage in stages if "columns" in stage
        ):
            return None
        payload = {
            "contract_version": MATERIALIZATION_CONTRACT_VERSION,
            "planner_epoch": MATERIALIZATION_PLANNER_EPOCH,
            "target": target,
            "stages": sorted(stages, key=canonical_json),
        }
        digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    except (AttributeError, DJParseException, TypeError, ValueError, KeyError):
        return None
    return MaterializationContract(
        version=MATERIALIZATION_CONTRACT_VERSION,
        planner_epoch=MATERIALIZATION_PLANNER_EPOCH,
        digest=digest,
    )


def materialization_artifact_matches(
    config: Mapping[str, object],
    table: str,
) -> bool:
    """Check the callback table against a persisted planner target."""
    if not table:
        return False
    if config.get("version") == "v3":
        return config.get("druid_datasource") == table

    combiners = config.get("combiners")
    if not isinstance(combiners, list) or not combiners:
        return False
    data_sources = []
    for combiner in combiners:
        if not isinstance(combiner, Mapping):
            return False
        druid_spec = combiner.get("druid_spec")
        if not isinstance(druid_spec, Mapping):
            return False
        data_schema = druid_spec.get("dataSchema")
        if not isinstance(data_schema, Mapping):
            return False
        data_source = data_schema.get("dataSource")
        if not isinstance(data_source, str):
            return False
        data_sources.append(data_source)
    return len(set(data_sources)) == 1 and data_sources[0] == table
