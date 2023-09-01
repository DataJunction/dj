import pytest

from datajunction_server.errors import DJPluginNotFoundException
from datajunction_server.models.engine import Dialect
from datajunction_server.transpilation import get_transpilation_plugin


def test_get_transpilation_plugin() -> None:
    """
    Test ``get_transpilation_plugin``
    """
    # Will raise an error with for an unknown transpilation library
    with pytest.raises(DJPluginNotFoundException) as excinfo:
        get_transpilation_plugin("somepackage")
    assert "No SQL transpilation plugin found for package `somepackage`!" in str(
        excinfo,
    )

    package_name = "sqlglot"
    known = get_transpilation_plugin(package_name)
    assert known.package_name == package_name
    assert known.transpile_sql("1") == "1"
    assert (
        known.transpile_sql(
            "1",
            input_dialect=Dialect.SPARK,
            output_dialect=Dialect.TRINO,
        )
        == "1"
    )
    assert known.transpile_sql("1", output_dialect=Dialect.TRINO) == "1"
