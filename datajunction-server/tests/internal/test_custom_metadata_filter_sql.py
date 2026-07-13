"""Tests for custom_metadata_clause SQL translation (no DB required)."""

from sqlalchemy.dialects import postgresql

from datajunction_server.database.node import NodeRevision
from datajunction_server.internal.custom_metadata import custom_metadata_clause
from datajunction_server.models.custom_metadata import (
    CustomMetadataFilter,
    CustomMetadataOp,
)

_PG_DIALECT = postgresql.dialect()


def _sql(clause):
    return str(clause.compile(dialect=_PG_DIALECT))


def test_eq_uses_containment():
    f = CustomMetadataFilter(key="table_group", op=CustomMetadataOp.EQ, value="ads")
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "@>" in sql  # GIN-served containment
    assert "CAST" not in sql.upper()  # no cast — GIN index must be usable


def test_ne_uses_negated_containment():
    f = CustomMetadataFilter(key="table_group", op=CustomMetadataOp.NE, value="ads")
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "@>" in sql
    assert "NOT" in sql
    assert "CAST" not in sql.upper()


def test_exists_uses_question_operator():
    f = CustomMetadataFilter(key="grain", op=CustomMetadataOp.EXISTS)
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "?" in sql
    assert "CAST" not in sql.upper()


def test_gt_uses_text_extraction_cast():
    f = CustomMetadataFilter(key="threshold", op=CustomMetadataOp.GT, value=5)
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "->>" in sql
    assert " > " in sql
    assert " >= " not in sql


def test_gte_uses_text_extraction_cast():
    f = CustomMetadataFilter(key="threshold", op=CustomMetadataOp.GTE, value=5)
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "->>" in sql
    assert " >= " in sql


def test_lt_uses_text_extraction_cast():
    f = CustomMetadataFilter(key="threshold", op=CustomMetadataOp.LT, value=10)
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "->>" in sql
    assert " < " in sql
    assert " <= " not in sql


def test_lte_uses_text_extraction_cast():
    f = CustomMetadataFilter(key="threshold", op=CustomMetadataOp.LTE, value=10)
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "->>" in sql
    assert " <= " in sql


def test_contains_uses_jsonb_containment_on_element():
    f = CustomMetadataFilter(key="tags", op=CustomMetadataOp.CONTAINS, value=["a", "b"])
    sql = _sql(custom_metadata_clause(NodeRevision.custom_metadata, f))
    assert "@>" in sql
    assert "CAST" not in sql.upper()
