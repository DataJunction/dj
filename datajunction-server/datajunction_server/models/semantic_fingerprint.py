"""Models and version constants for semantic fingerprints."""

from typing import Literal, TypeAlias

from pydantic import BaseModel, Field, field_validator


LATEST_SEMANTIC_FINGERPRINT_VERSION = 1
SUPPORTED_SEMANTIC_FINGERPRINT_VERSIONS = frozenset(
    {LATEST_SEMANTIC_FINGERPRINT_VERSION},
)
UNKNOWN_SEMANTIC_FINGERPRINT: Literal["unknown"] = "unknown"


class SemanticFingerprint(BaseModel):
    """A versioned digest of a node's semantic definition."""

    version: int = LATEST_SEMANTIC_FINGERPRINT_VERSION
    digest: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]+$",
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, version: int) -> int:
        if version not in SUPPORTED_SEMANTIC_FINGERPRINT_VERSIONS:
            raise ValueError(f"Unsupported semantic fingerprint version: {version}")
        return version


SemanticFingerprintValue: TypeAlias = SemanticFingerprint | Literal["unknown"]
