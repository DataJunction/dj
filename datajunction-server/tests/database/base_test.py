"""Tests for database base utilities."""

from typing import Optional

from pydantic import BaseModel

from datajunction_server.database.base import PydanticListType


class SampleModel(BaseModel):
    """Simple Pydantic model for testing PydanticListType."""

    name: str
    value: int
    optional_field: Optional[str] = None


class TestPydanticListType:
    """Tests for PydanticListType TypeDecorator."""

    def test_cache_ok_is_true(self):
        """Test that cache_ok is set to True for SQLAlchemy caching."""
        type_decorator = PydanticListType(SampleModel)
        assert type_decorator.cache_ok is True

    def test_process_bind_param_serializes_pydantic_models(self):
        """Test that Pydantic models are serialized to dicts for storage."""
        type_decorator = PydanticListType(SampleModel)
        models = [
            SampleModel(name="foo", value=1),
            SampleModel(name="bar", value=2, optional_field="extra"),
        ]

        result = type_decorator.process_bind_param(models, dialect=None)

        assert result == [
            {"name": "foo", "value": 1, "optional_field": None},
            {"name": "bar", "value": 2, "optional_field": "extra"},
        ]

    def test_process_bind_param_handles_none(self):
        """Test that None input returns None."""
        type_decorator = PydanticListType(SampleModel)

        result = type_decorator.process_bind_param(None, dialect=None)

        assert result is None

    def test_process_bind_param_handles_empty_list(self):
        """Test that empty list returns empty list."""
        type_decorator = PydanticListType(SampleModel)

        result = type_decorator.process_bind_param([], dialect=None)

        assert result == []

    def test_process_bind_param_handles_dict_passthrough(self):
        """Test that dicts in the list are passed through unchanged."""
        type_decorator = PydanticListType(SampleModel)
        # Sometimes data might already be dicts (e.g., from JSON)
        mixed = [
            SampleModel(name="model", value=1),
            {"name": "dict", "value": 2, "optional_field": None},
        ]

        result = type_decorator.process_bind_param(mixed, dialect=None)

        assert result == [
            {"name": "model", "value": 1, "optional_field": None},
            {"name": "dict", "value": 2, "optional_field": None},
        ]

    def test_process_result_value_deserializes_to_pydantic_models(self):
        """Test that dicts are deserialized to Pydantic models on read."""
        type_decorator = PydanticListType(SampleModel)
        dicts = [
            {"name": "foo", "value": 1, "optional_field": None},
            {"name": "bar", "value": 2, "optional_field": "extra"},
        ]

        result = type_decorator.process_result_value(dicts, dialect=None)

        assert len(result) == 2
        assert all(isinstance(item, SampleModel) for item in result)
        assert result[0].name == "foo"
        assert result[0].value == 1
        assert result[1].name == "bar"
        assert result[1].optional_field == "extra"

    def test_process_result_value_handles_none(self):
        """Test that None input returns None."""
        type_decorator = PydanticListType(SampleModel)

        result = type_decorator.process_result_value(None, dialect=None)

        assert result is None

    def test_process_result_value_handles_empty_list(self):
        """Test that empty list returns empty list."""
        type_decorator = PydanticListType(SampleModel)

        result = type_decorator.process_result_value([], dialect=None)

        assert result == []

    def test_roundtrip_preserves_data(self):
        """Test that serialize -> deserialize produces equal objects."""
        type_decorator = PydanticListType(SampleModel)
        original = [
            SampleModel(name="foo", value=1),
            SampleModel(name="bar", value=2, optional_field="extra"),
        ]

        # Simulate DB roundtrip
        serialized = type_decorator.process_bind_param(original, dialect=None)
        deserialized = type_decorator.process_result_value(serialized, dialect=None)

        assert len(deserialized) == len(original)
        for orig, deser in zip(original, deserialized):
            assert orig == deser
