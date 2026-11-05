from datetime import date, datetime
from decimal import Decimal

from djqs.engine import serialize_for_json


def test_serialize_date():
    d = date(2023, 4, 5)
    assert serialize_for_json(d) == "2023-04-05"


def test_serialize_datetime():
    dt = datetime(2023, 4, 5, 14, 30, 15)
    assert serialize_for_json(dt) == "2023-04-05T14:30:15"


def test_serialize_list():
    data = [date(2023, 1, 1), "string", 123]
    expected = ["2023-01-01", "string", 123]
    assert serialize_for_json(data) == expected


def test_serialize_dict():
    data = {
        "some_date": date(2022, 12, 31),
        "nested": {"datetime": datetime(2022, 12, 31, 23, 59), "value": 42},
    }
    expected = {
        "some_date": "2022-12-31",
        "nested": {"datetime": "2022-12-31T23:59:00", "value": 42},
    }
    assert serialize_for_json(data) == expected


def test_serialize_other_types():
    assert serialize_for_json(123) == 123
    assert serialize_for_json("abc") == "abc"
    assert serialize_for_json(None) is None


def test_serialize_decimal_values_in_query_rows():
    rows = [(Decimal("42"), Decimal("12.25"), date(2023, 4, 5))]
    assert serialize_for_json(rows) == [[42, "12.25", "2023-04-05"]]
    assert serialize_for_json(Decimal("0.123456789012345678901")) == (
        "0.123456789012345678901"
    )
