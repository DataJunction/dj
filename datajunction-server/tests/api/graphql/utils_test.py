import pytest
from datajunction_server.api.graphql.utils import convert_camel_case, dedupe_append


@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("camelCase", "camel_case"),
        ("CamelCase", "camel_case"),
        ("HttpRequest", "http_request"),
        ("getUrlFromHtml", "get_url_from_html"),
        ("already_snake_case", "already_snake_case"),
        ("single", "single"),
        ("", ""),
    ],
)
def test_convert_camel_case(input_str, expected):
    assert convert_camel_case(input_str) == expected


@pytest.mark.parametrize(
    "base, extras, expected",
    [
        (["a", "b"], ["c", "d"], ["a", "b", "c", "d"]),  # no duplicates
        (["a", "b"], ["b", "c"], ["a", "b", "c"]),  # some duplicates
        (["a", "b"], ["a", "b"], ["a", "b"]),  # all duplicates
        ([], ["x", "y"], ["x", "y"]),  # empty base
        (["x", "y"], [], ["x", "y"]),  # empty extras
        ([], [], []),  # both empty
    ],
)
def test_dedupe_append(base, extras, expected):
    assert dedupe_append(base, extras) == expected
