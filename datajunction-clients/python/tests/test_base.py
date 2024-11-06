"""
Test serializable mixin for dict to dataclass conversion
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from datajunction._base import SerializableMixin
from datajunction._internal import DJClient


@dataclass
class DataClassSimple(SerializableMixin):
    """Simple dataclass"""

    name: str
    version: str
    other: Optional[int]
    created: datetime


@dataclass
class DataClassNested(SerializableMixin):
    """Nested dataclass"""

    name: str
    dj_client: Optional[DJClient]
    dc_list: List[DataClassSimple]
    dc_list_optional: Optional[List[DataClassSimple]]
    dc_optional: Optional[DataClassSimple]
    dc_str_list: List[str]


class OtherClass(SerializableMixin):  # pylint: disable=too-few-public-methods
    """Non-dataclass test case"""

    def __init__(self, name: str, version: Optional[str]):
        self.name = name
        self.version = version


class TestSerializableMixin:
    """
    Tests for the SerializableMixin base class
    """

    def test_non_dataclass(self):
        """
        Test the mixin with non-dataclasses
        """
        other_class = OtherClass.from_dict(
            dj_client=None,
            data={"name": "Name", "version": "v1"},
        )
        assert other_class.name == "Name"
        assert other_class.version == "v1"

    def test_serialize_simple(self):
        """
        Serialize simple dataclass
        """
        now = datetime.now()
        dc_simple = DataClassSimple.from_dict(
            dj_client=None,
            data={
                "name": "Name",
                "version": "v1",
                "other": 123,
                "created": now,
            },
        )
        assert dc_simple.name == "Name"
        assert dc_simple.version == "v1"
        assert dc_simple.other == 123
        assert dc_simple.created == now

    def test_serialize_nested(self):
        """
        Serialize nested dataclass
        """
        now = datetime.now()
        dj_client = DJClient()
        dc_nested = DataClassNested.from_dict(
            dj_client=dj_client,
            data={
                "name": "Name",
                "dc_list": [
                    {"name": "N1", "version": "v1", "other": 123, "created": now},
                    {"name": "N2", "version": "v2", "other": None, "created": now},
                ],
                "dc_list_optional": [
                    {"name": "N1", "version": "v1", "other": 123, "created": now},
                ],
                "dc_optional": {
                    "name": "N1",
                    "version": "v1",
                    "other": 123,
                    "created": now,
                },
                "dc_str_list": ["a", "b", "c"],
            },
        )
        assert dc_nested == DataClassNested(
            name="Name",
            dj_client=dj_client,
            dc_list=[
                DataClassSimple(
                    name="N1",
                    version="v1",
                    other=123,
                    created=now,
                ),
                DataClassSimple(
                    name="N2",
                    version="v2",
                    other=None,
                    created=now,
                ),
            ],
            dc_list_optional=[
                DataClassSimple(
                    name="N1",
                    version="v1",
                    other=123,
                    created=now,
                ),
            ],
            dc_optional=DataClassSimple(
                name="N1",
                version="v1",
                other=123,
                created=now,
            ),
            dc_str_list=["a", "b", "c"],
        )
        dc_nested = DataClassNested.from_dict(
            dj_client=None,
            data={
                "name": "Name",
                "dc_list": [
                    {"name": "N1", "version": "v1", "other": 123, "created": now},
                    {"name": "N2", "version": "v2", "other": None, "created": now},
                ],
                "dc_list_optional": None,
                "dc_optional": None,
                "dc_str_list": ["a", "b", "c"],
            },
        )
        assert dc_nested == DataClassNested(
            name="Name",
            dj_client=None,
            dc_list=[
                DataClassSimple(
                    name="N1",
                    version="v1",
                    other=123,
                    created=now,
                ),
                DataClassSimple(
                    name="N2",
                    version="v2",
                    other=None,
                    created=now,
                ),
            ],
            dc_list_optional=None,
            dc_optional=None,
            dc_str_list=["a", "b", "c"],
        )
