"""
Tests for building nodes and extracting dependencies
"""
# pylint: disable=too-many-lines
import pytest
from sqlmodel import Session, select

from dj.construction.extract import (
    ColumnDependencies,
    CompoundBuildException,
    DimensionJoinException,
    InvalidSQLException,
    MissingColumnException,
    NodeTypeException,
    UnknownNodeException,
    extract_dependencies,
    extract_dependencies_from_query,
    get_dj_node,
    make_name,
)
from dj.models import Column
from dj.models.node import Node, NodeType
from dj.sql.parsing.ast import Alias, BinaryOp, BinaryOpKind
from dj.sql.parsing.ast import Column as ASTColumn
from dj.sql.parsing.ast import (
    From,
    Join,
    JoinKind,
    Name,
    Namespace,
    Select,
    String,
    Table,
)
from dj.sql.parsing.backends.sqloxide import parse
from dj.typing import ColumnType


@pytest.mark.parametrize(
    "namespace,name,expected_make_name",
    [
        (Namespace([Name("a"), Name("b"), Name("c")]), "d", "a.b.c.d"),
        (Namespace([Name("a"), Name("b")]), "node-name", "a.b.node-name"),
        (Namespace([]), "node-[name]", "node-[name]"),
        (None, "node-[name]", "node-[name]"),
        (Namespace([Name("a"), Name("b"), Name("c")]), None, "a.b.c"),
        (
            Namespace([Name("a"), Name("b"), Name("c")]),
            "node&(name)",
            "a.b.c.node&(name)",
        ),
        (Namespace([Name("a"), Name("b"), Name("c")]), "+d", "a.b.c.+d"),
        (Namespace([Name("a"), Name("b"), Name("c")]), "-d", "a.b.c.-d"),
        (Namespace([Name("a"), Name("b"), Name("c")]), "~~d", "a.b.c.~~d"),
    ],
)
def test_make_name(namespace: str, name: str, expected_make_name: str):
    """
    Test making names from a namespace and a name
    """
    assert make_name(namespace, name) == expected_make_name


def test_invalid_sql_exception():
    """
    Test raising an InvalidSQLException
    """
    assert "This is an exception message `foo`" in str(
        InvalidSQLException("This is an exception message", Name("foo")),
    )
    assert "This is an exception message `foo` from `bar`" in str(
        InvalidSQLException("This is an exception message", Name("foo"), Table("bar")),
    )


def test_dimension_join_exception():
    """
    Test raising an DimensionJoinException
    """
    assert "This is an exception message `foo`" in str(
        DimensionJoinException("This is an exception message", "foo"),
    )
    assert "This is an exception message `foo`" in str(
        DimensionJoinException("This is an exception message", "foo", Name("bar")),
    )


def test_missing_column_exception():
    """
    Test raising a MissingColumnException
    """
    assert "This is an exception message `foo`" in str(
        MissingColumnException("This is an exception message", ASTColumn("foo")),
    )
    assert "This is an exception message `foo` from `bar`" in str(
        MissingColumnException(
            "This is an exception message",
            ASTColumn("foo"),
            Table("bar"),
        ),
    )


def test_node_type_exception():
    """
    Test raising a NodeTypeException
    """
    assert "This is an exception message `foo`" in str(
        NodeTypeException("This is an exception message", Name("foo")),
    )
    assert "This is an exception message `foo` from `bar`" in str(
        NodeTypeException("This is an exception message", Name("foo"), Table("bar")),
    )


def test_unknown_node_exception():
    """
    Test raising an UnknownNodeException
    """
    assert "This is an exception message `foo`" in str(
        UnknownNodeException("This is an exception message", Name("foo")),
    )
    assert "This is an exception message `foo` from `bar`" in str(
        UnknownNodeException("This is an exception message", Name("foo"), Table("bar")),
    )


def test_compound_build_exception():
    """
    Test raising a CompoundBuildException
    """
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(False)  # pylint: disable=protected-access
    with CompoundBuildException().catch:  # pylint: disable=protected-access
        raise InvalidSQLException("This SQL is invalid.", node=Name("foo"))

    assert len(CompoundBuildException().errors) == 1
    assert isinstance(CompoundBuildException().errors[0], InvalidSQLException)

    CompoundBuildException().clear()
    assert CompoundBuildException().errors == []
    assert CompoundBuildException()._raise is False  # pylint: disable=protected-access

    CompoundBuildException().reset()
    assert CompoundBuildException()._raise is True  # pylint: disable=protected-access


class TestExtractingDependencies:  # pylint: disable=too-many-public-methods
    """A class for testing extracting dependencies from DJ queries"""

    @pytest.fixture
    def session(self, session: Session) -> Session:
        """
        Add some source nodes and transform nodes to facilitate testing of extracting dependencies
        """
        purchases = Node(
            name="purchases",
            type=NodeType.SOURCE,
            columns=[
                Column(name="transaction_id", type=ColumnType.INT),
                Column(name="transaction_time", type=ColumnType.DATETIME),
                Column(name="transaction_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="system_version", type=ColumnType.STR),
            ],
        )
        customer_events = Node(
            name="customer_events",
            type=NodeType.SOURCE,
            columns=[
                Column(name="event_id", type=ColumnType.INT),
                Column(name="event_time", type=ColumnType.DATETIME),
                Column(name="event_type", type=ColumnType.STR),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="message", type=ColumnType.STR),
            ],
        )
        returns = Node(
            name="returns",
            type=NodeType.SOURCE,
            columns=[
                Column(name="transaction_id", type=ColumnType.INT),
                Column(name="transaction_time", type=ColumnType.DATETIME),
                Column(name="purchase_transaction_id", type=ColumnType.INT),
            ],
        )

        eligible_purchases = Node(
            name="eligible_purchases",
            query="""
                    SELECT transaction_id, transaction_time, transaction_amount, customer_id, system_version
                    FROM purchases
                    WHERE transaction_amount > 100.0
                """,
            type=NodeType.TRANSFORM,
            columns=[
                Column(name="transaction_id", type=ColumnType.INT),
                Column(name="transaction_time", type=ColumnType.DATETIME),
                Column(name="transaction_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="system_version", type=ColumnType.STR),
            ],
        )

        eligible_purchases_new_system = Node(
            name="eligible_purchases_new_system",
            query="""
                    SELECT transaction_id, transaction_time, transaction_amount, customer_id
                    FROM eligible_purchases
                    WHERE transaction_amount > 100.0
                    AND system_version = 'v2'
                """,
            type=NodeType.TRANSFORM,
            columns=[
                Column(name="transaction_id", type=ColumnType.INT),
                Column(name="transaction_time", type=ColumnType.DATETIME),
                Column(name="transaction_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="system_version", type=ColumnType.STR),
            ],
        )

        returned_transactions = Node(
            name="returned_transactions",
            query="""
                    SELECT transaction_id, transaction_time, transaction_amount, customer_id
                    FROM purchases p
                    LEFT JOIN returns r
                    ON p.transaction_id = r.purchase_transaction_id
                    WHERE r.purchase_transaction_id is not null
                """,
            type=NodeType.TRANSFORM,
            columns=[
                Column(name="transaction_id", type=ColumnType.INT),
                Column(name="transaction_time", type=ColumnType.DATETIME),
                Column(name="transaction_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
            ],
        )

        event_type = Node(
            name="event_type",
            type=NodeType.DIMENSION,
            query="SELECT DISTINCT event_type FROM customer_events",
            columns=[
                Column(name="event_type", type=ColumnType.STR),
            ],
        )
        event_type_id = Node(
            name="event_type_id",
            type=NodeType.DIMENSION,
            query="SELECT DISTINCT event_id, event_type FROM customer_events",
            columns=[
                Column(name="event_id", type=ColumnType.INT),
                Column(name="event_type", type=ColumnType.STR),
            ],
        )
        customer_events2 = Node(
            name="customer_events2",
            type=NodeType.SOURCE,
            columns=[
                Column(
                    name="event_id",
                    type=ColumnType.INT,
                    dimension=event_type_id,
                    dimension_column="event_id",
                ),
                Column(name="event_time", type=ColumnType.DATETIME),
                Column(name="event_type", type=ColumnType.STR),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="message", type=ColumnType.STR),
            ],
        )

        session.add(purchases)
        session.add(customer_events)
        session.add(returns)
        session.add(eligible_purchases)
        session.add(eligible_purchases_new_system)
        session.add(returned_transactions)
        session.add(event_type)
        session.add(customer_events2)
        session.add(event_type_id)
        session.commit()
        return session

    def test_simplest_select(self, session: Session):
        """
        Test a simplest select
        """
        query = parse(
            "select 1",
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert len(list(dependencies.all_tables)) == 0
        assert len(list(dependencies.all_node_dependencies)) == 0
        assert dependencies.columns == ColumnDependencies(
            projection=[],
            group_by=[],
            filters=[],
            ons=[],
        )

        assert len(dependencies.tables) == 0

    def test_select_with_filter_dimension(self, session: Session):
        """
        Test a select with a dimension filter
        """
        query = parse(
            "select event_type from customer_events2 where event_type_id.event_type='an_event'",
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select
        assert len(list(dependencies.all_tables)) == 1
        assert len(list(dependencies.all_node_dependencies)) == 2
        assert len(dependencies.columns.filters) == 1
        assert dependencies.columns.filters[0][0].compare(
            ASTColumn(
                name=Name(name="event_type", quote_style=""),
                namespace=Namespace(names=[Name(name="event_type_id", quote_style="")]),
            ),
        )

    def test_select_with_having_dimension(self, session: Session):
        """
        Test a select with a dimension having
        """
        query = parse(
            "select event_type from customer_events2 "
            "group by event_type_id.event_type having event_type_id.event_type='an_event'",
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select
        assert len(list(dependencies.all_tables)) == 1
        assert len(list(dependencies.all_node_dependencies)) == 2
        assert len(dependencies.columns.filters) == 1
        assert dependencies.columns.filters[0][0].compare(
            ASTColumn(
                name=Name(name="event_type", quote_style=""),
                namespace=Namespace(names=[Name(name="event_type_id", quote_style="")]),
            ),
        )

    def test_select_with_having_without_groupby_raises(self, session: Session):
        """
        Test a select with a dimension having
        """
        query = parse(
            "select event_type from customer_events2 having event_type_id.event_type='an_event'",
            "hive",
        )
        with pytest.raises(InvalidSQLException):
            extract_dependencies_from_query(session, query)

    def test_select_with_dimension_in_improper_place(self, session: Session):
        """
        Test a select with a dimension in an invalid place
        """
        query = parse(
            """
            select a.event_type from customer_events2 a
            left join customer_events2 b
            on a.event_type=b.event_type and event_type_id.event_type='an_event'
            """,
            "hive",
        )
        with pytest.raises(InvalidSQLException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert "Cannot reference a dimension here." in str(exc_info.value)

    def test_select_with_dimension_in_improper_place_raise_false(
        self,
        session: Session,
    ):
        """
        Test a select with an invalid dimension where the exception is stored but not raised
        """
        query = parse(
            """
            select a.event_type from customer_events2 a
            left join customer_events2 b
            on a.event_type=b.event_type and event_type_id.event_type='an_event'
            """,
            "hive",
        )

        CompoundBuildException().reset()
        CompoundBuildException().set_raise(
            False,
        )  # Configure CompoundBuildException to just accumulate errors
        extract_dependencies_from_query(session, query)
        assert len(CompoundBuildException().errors) == 2
        assert isinstance(CompoundBuildException().errors[0], MissingColumnException)
        assert isinstance(CompoundBuildException().errors[1], InvalidSQLException)
        CompoundBuildException().reset()  # Reset the singleton

    def test_select_with_dimension_unjoinable(self, session: Session):
        """
        Test a select with a dimension that cannot be joined
        """
        query = parse(
            """
            select event_type from customer_events
            where event_type_id.event_type='an_event'
            """,
            "hive",
        )
        with pytest.raises(DimensionJoinException):
            extract_dependencies_from_query(session, query)

    def test_no_such_namespaced_column_in_existing_node(self, session: Session):
        """
        Test a with a nonexistent column
        """
        query = parse(
            "select a.event_type2 from customer_events2 a",
            "hive",
        )
        with pytest.raises(MissingColumnException):
            extract_dependencies_from_query(session, query)

    def test_no_such_column_in_existing_node(self, session: Session):
        """
        Test a with a nonexistent column
        """
        query = parse(
            "select event_type2 from customer_events2 ",
            "hive",
        )
        with pytest.raises(MissingColumnException):
            extract_dependencies_from_query(session, query)

    def test_dupe_column_refs(self, session: Session):
        """
        Test a column that can come from multiple places
        """
        query = parse(
            "select event_type from customer_events2, event_type_id",
            "hive",
        )
        with pytest.raises(InvalidSQLException):
            extract_dependencies_from_query(session, query)

    def test_simple_select_from_single_transform(self, session: Session):
        """
        Test a simple select from a transform
        """
        query = parse(
            "select transaction_id, transaction_amount from purchases",
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert len(list(dependencies.all_tables)) == 1
        assert len(list(dependencies.all_node_dependencies)) == 1
        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
                (
                    ASTColumn(
                        name=Name(name="transaction_amount", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            group_by=[],
            filters=[],
        )

        assert list(dependencies.columns.all_columns) == [
            (
                ASTColumn(
                    name=Name(name="transaction_id", quote_style=""),
                    namespace=None,
                ),
                Table(name=Name(name="purchases", quote_style=""), namespace=None),
            ),
            (
                ASTColumn(
                    name=Name(name="transaction_amount", quote_style=""),
                    namespace=None,
                ),
                Table(name=Name(name="purchases", quote_style=""), namespace=None),
            ),
        ]

        assert len(dependencies.tables) == 1
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="purchases",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "purchases"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=1,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=2,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=3,
                dimension_id=None,
                type=ColumnType.FLOAT,
                name="transaction_amount",
                dimension_column=None,
            ),
            Column(
                id=4,
                dimension_id=None,
                type=ColumnType.INT,
                name="customer_id",
                dimension_column=None,
            ),
            Column(
                id=5,
                dimension_id=None,
                type=ColumnType.STR,
                name="system_version",
                dimension_column=None,
            ),
        ]

    def test_simple_agg_from_single_transform(self, session: Session):
        """
        Test an aggregation of a column in a transform
        """
        query = parse("SELECT sum(transaction_amount) FROM eligible_purchases", "hive")
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_amount", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="eligible_purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            group_by=[],
            filters=[],
        )

        assert len(dependencies.tables) == 1
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="eligible_purchases",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "eligible_purchases"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=14,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=15,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=16,
                dimension_id=None,
                type=ColumnType.FLOAT,
                name="transaction_amount",
                dimension_column=None,
            ),
            Column(
                id=17,
                dimension_id=None,
                type=ColumnType.INT,
                name="customer_id",
                dimension_column=None,
            ),
            Column(
                id=18,
                dimension_id=None,
                type=ColumnType.STR,
                name="system_version",
                dimension_column=None,
            ),
        ]

    def test_group_by(self, session: Session):
        """
        Test an aggregation with a group by
        """
        query = parse(
            "SELECT sum(transaction_amount) FROM eligible_purchases GROUP BY customer_id",
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_amount", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="eligible_purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            group_by=[
                (
                    ASTColumn(
                        name=Name(name="customer_id", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="eligible_purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            filters=[],
        )

        assert len(dependencies.tables) == 1
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="eligible_purchases",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "eligible_purchases"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=14,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=15,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=16,
                dimension_id=None,
                type=ColumnType.FLOAT,
                name="transaction_amount",
                dimension_column=None,
            ),
            Column(
                id=17,
                dimension_id=None,
                type=ColumnType.INT,
                name="customer_id",
                dimension_column=None,
            ),
            Column(
                id=18,
                dimension_id=None,
                type=ColumnType.STR,
                name="system_version",
                dimension_column=None,
            ),
        ]

    def test_joining_table_to_itself(self, session: Session):
        """
        Test extracting dependencies from joining a table to itself
        """
        query = parse(
            """
        SELECT r2.transaction_id as matched_id
        FROM returns r1
        LEFT JOIN returns r2
        ON r1.purchase_transaction_id = r2.transaction_id
        WHERE r2.transaction_id is not null
        """,
            "hive",
        )
        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select
        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=Namespace(names=[Name(name="r2", quote_style="")]),
                    ),
                    Table(name=Name(name="returns", quote_style=""), namespace=None),
                ),
            ],
            group_by=[],
            filters=[
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=Namespace(names=[Name(name="r2", quote_style="")]),
                    ),
                    Table(name=Name(name="returns", quote_style=""), namespace=None),
                ),
            ],
            ons=[
                (
                    ASTColumn(
                        name=Name(name="purchase_transaction_id", quote_style=""),
                        namespace=Namespace(names=[Name(name="r1", quote_style="")]),
                    ),
                    Table(name=Name(name="returns", quote_style=""), namespace=None),
                ),
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=Namespace(names=[Name(name="r2", quote_style="")]),
                    ),
                    Table(name=Name(name="returns", quote_style=""), namespace=None),
                ),
            ],
        )

        assert len(dependencies.tables) == 2
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="returns",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "returns"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=11,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=12,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=13,
                dimension_id=None,
                type=ColumnType.INT,
                name="purchase_transaction_id",
                dimension_column=None,
            ),
        ]

        assert dependencies.tables[1][0].name == Name(  # type: ignore
            name="returns",
            quote_style="",
        )
        assert dependencies.tables[1][1].name == "returns"
        assert dependencies.tables[1][1].columns == [
            Column(
                id=11,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=12,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=13,
                dimension_id=None,
                type=ColumnType.INT,
                name="purchase_transaction_id",
                dimension_column=None,
            ),
        ]

    def test_expression_in_select(self, session: Session):
        """
        Test extracting dependencies when an expression is used in the select clause
        """
        query = parse(
            """
        SELECT transaction_id, (transaction_amount * 0.10) as reimbursement
        FROM purchases
        """,
            "hive",
        )

        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=None,
                    ),
                    Table(name=Name(name="purchases", quote_style=""), namespace=None),
                ),
                (
                    ASTColumn(
                        name=Name(name="transaction_amount", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="purchases", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            group_by=[],
            filters=[],
        )

        assert len(dependencies.tables) == 1
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="purchases",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "purchases"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=1,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=2,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=3,
                dimension_id=None,
                type=ColumnType.FLOAT,
                name="transaction_amount",
                dimension_column=None,
            ),
            Column(
                id=4,
                dimension_id=None,
                type=ColumnType.INT,
                name="customer_id",
                dimension_column=None,
            ),
            Column(
                id=5,
                dimension_id=None,
                type=ColumnType.STR,
                name="system_version",
                dimension_column=None,
            ),
        ]

    def test_joining_to_a_dimension(self, session: Session):
        """
        Test extracting dependencies when joining to a dimension
        """
        query = parse(
            """
            SELECT event_id, event_time, message
            FROM customer_events ce
            LEFT JOIN event_type et
            ON ce.event_type = et.event_type
            """,
            "hive",
        )

        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="event_id", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="customer_events", quote_style=""),
                        namespace=None,
                    ),
                ),
                (
                    ASTColumn(
                        name=Name(name="event_time", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="customer_events", quote_style=""),
                        namespace=None,
                    ),
                ),
                (
                    ASTColumn(
                        name=Name(name="message", quote_style=""),
                        namespace=None,
                    ),
                    Table(
                        name=Name(name="customer_events", quote_style=""),
                        namespace=None,
                    ),
                ),
            ],
            group_by=[],
            filters=[],
            ons=[
                (
                    ASTColumn(
                        name=Name(name="event_type", quote_style=""),
                        namespace=Namespace(names=[Name(name="ce", quote_style="")]),
                    ),
                    Table(
                        name=Name(name="customer_events", quote_style=""),
                        namespace=None,
                    ),
                ),
                (
                    ASTColumn(
                        name=Name(name="event_type", quote_style=""),
                        namespace=Namespace(names=[Name(name="et", quote_style="")]),
                    ),
                    Table(name=Name(name="event_type", quote_style=""), namespace=None),
                ),
            ],
        )

    def test_ambiguous_column_name(self, session: Session):
        """
        Test extracting dependencies when column name is ambiguous
        """
        query = parse(
            """
            SELECT transaction_id
            FROM returns r
            LEFT JOIN purchases p
            ON r.purchase_transaction_id = p.transaction_id
            """,
            "hive",
        )

        with pytest.raises(InvalidSQLException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert (
            "`transaction_id` appears in multiple references and so must be namespaced."
        ) in str(exc_info.value)

    def test_implicit_inner_join(self, session: Session):
        """
        Test extracting dependencies from an implicit inner join
        """
        query = parse(
            """
            SELECT purchases.transaction_id
            FROM returns, purchases
            WHERE returns.purchase_transaction_id = purchases.transaction_id
            """,
            "hive",
        )

        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=Namespace(
                            names=[Name(name="purchases", quote_style="")],
                        ),
                    ),
                    Table(name=Name(name="purchases", quote_style=""), namespace=None),
                ),
            ],
            group_by=[],
            filters=[
                (
                    ASTColumn(
                        name=Name(name="purchase_transaction_id", quote_style=""),
                        namespace=Namespace(
                            names=[Name(name="returns", quote_style="")],
                        ),
                    ),
                    Table(name=Name(name="returns", quote_style=""), namespace=None),
                ),
                (
                    ASTColumn(
                        name=Name(name="transaction_id", quote_style=""),
                        namespace=Namespace(
                            names=[Name(name="purchases", quote_style="")],
                        ),
                    ),
                    Table(name=Name(name="purchases", quote_style=""), namespace=None),
                ),
            ],
        )

        assert len(dependencies.tables) == 2
        assert dependencies.tables[0][0].name == Name(  # type: ignore
            name="returns",
            quote_style="",
        )
        assert dependencies.tables[0][1].name == "returns"
        assert dependencies.tables[0][1].columns == [
            Column(
                id=11,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=12,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=13,
                dimension_id=None,
                type=ColumnType.INT,
                name="purchase_transaction_id",
                dimension_column=None,
            ),
        ]

        assert dependencies.tables[1][0].name == Name(  # type: ignore
            name="purchases",
            quote_style="",
        )
        assert dependencies.tables[1][1].name == "purchases"
        assert dependencies.tables[1][1].columns == [
            Column(
                id=1,
                dimension_id=None,
                type=ColumnType.INT,
                name="transaction_id",
                dimension_column=None,
            ),
            Column(
                id=2,
                dimension_id=None,
                type=ColumnType.DATETIME,
                name="transaction_time",
                dimension_column=None,
            ),
            Column(
                id=3,
                dimension_id=None,
                type=ColumnType.FLOAT,
                name="transaction_amount",
                dimension_column=None,
            ),
            Column(
                id=4,
                dimension_id=None,
                type=ColumnType.INT,
                name="customer_id",
                dimension_column=None,
            ),
            Column(
                id=5,
                dimension_id=None,
                type=ColumnType.STR,
                name="system_version",
                dimension_column=None,
            ),
        ]

    def test_raise_on_joining_with_unamed_subquery(self, session: Session):
        """
        Test raising an error when attempting an implicit join to an unnamed subquery
        """
        query = parse(
            """
            SELECT
            min(event_time) as first_event_time,
            max(event_time) as last_event_time
            FROM event_type, (
              SELECT event_time
              FROM (
                SELECT event_id, event_time, message
                FROM customer_events ce
                LEFT JOIN purchases et
                ON ce.event_id = et.transaction_id
                WHERE ce.event_type = 'TRANSACTION'
              )
            )
            """,
            "hive",
        )

        with pytest.raises(InvalidSQLException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert "You may only use an unnamed subquery alone" in str(exc_info.value)

    def test_raise_on_duplicate_aliases(self, session: Session):
        """
        Test raising when an alias is used more than once
        """
        query = parse(
            """SELECT event_id, event_time, message
            FROM customer_events ce
            LEFT JOIN purchases ce
            ON ce.event_id = ce.transaction_id
            """,
            "hive",
        )

        with pytest.raises(InvalidSQLException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert "Duplicate name `ce` for table. `purchases AS ce`" in str(exc_info.value)

    def test_raise_on_unaliased_expression(self, session: Session):
        """
        Test raising when an expression is used without an alias
        """
        query = parse(
            """
            SELECT
            min(event_time) as first_event_time,
            max(event_time) as last_event_time
            FROM (
              SELECT event_time
              FROM (
                SELECT event_id, event_time, message, (1*2)
                FROM customer_events ce
                LEFT JOIN purchases et
                ON ce.event_id = et.transaction_id
                WHERE ce.event_type = 'TRANSACTION'
              )
            )
            """,
            "hive",
        )

        with pytest.raises(InvalidSQLException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert "1 * 2 is an unnamed expression. Try adding an alias." in str(
            exc_info.value,
        )

    def test_deeply_nested_subqueries(self, session: Session):
        """
        Test extracting dependencies with deeply nested subqueries
        """
        query = parse(
            """
            SELECT
            min(event_time) as first_event_time,
            max(event_time) as last_event_time
            FROM (
              SELECT event_time
              FROM (
                SELECT event_id, event_time, message
                FROM customer_events ce
                LEFT JOIN purchases et
                ON ce.event_id = et.transaction_id
                WHERE ce.event_type = 'TRANSACTION'
              )
            )
            """,
            "hive",
        )

        query_dependencies = extract_dependencies_from_query(session, query)
        dependencies = query_dependencies.select

        assert len(list(dependencies.all_tables)) == 2
        assert dependencies.columns == ColumnDependencies(
            projection=[
                (
                    ASTColumn(
                        name=Name(name="event_time", quote_style=""),
                        namespace=None,
                    ),
                    Select(
                        from_=From(
                            tables=[
                                Select(
                                    from_=From(
                                        tables=[
                                            Alias(  # type: ignore
                                                name=Name(name="ce", quote_style=""),
                                                namespace=None,
                                                child=Table(
                                                    name=Name(
                                                        name="customer_events",
                                                        quote_style="",
                                                    ),
                                                    namespace=None,
                                                ),
                                            ),
                                        ],
                                        joins=[
                                            Join(
                                                kind=JoinKind.LeftOuter,
                                                table=Alias(  # type: ignore
                                                    name=Name(
                                                        name="et",
                                                        quote_style="",
                                                    ),
                                                    namespace=None,
                                                    child=Table(
                                                        name=Name(
                                                            name="purchases",
                                                            quote_style="",
                                                        ),
                                                        namespace=None,
                                                    ),
                                                ),
                                                on=BinaryOp(
                                                    op=BinaryOpKind.Eq,
                                                    left=ASTColumn(
                                                        name=Name(
                                                            name="event_id",
                                                            quote_style="",
                                                        ),
                                                        namespace=Namespace(
                                                            names=[
                                                                Name(
                                                                    name="ce",
                                                                    quote_style="",
                                                                ),
                                                            ],
                                                        ),
                                                    ),
                                                    right=ASTColumn(
                                                        name=Name(
                                                            name="transaction_id",
                                                            quote_style="",
                                                        ),
                                                        namespace=Namespace(
                                                            names=[
                                                                Name(
                                                                    name="et",
                                                                    quote_style="",
                                                                ),
                                                            ],
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ],
                                    ),
                                    group_by=[],
                                    having=None,
                                    projection=[
                                        ASTColumn(
                                            name=Name(name="event_id", quote_style=""),
                                            namespace=None,
                                        ),
                                        ASTColumn(
                                            name=Name(
                                                name="event_time",
                                                quote_style="",
                                            ),
                                            namespace=None,
                                        ),
                                        ASTColumn(
                                            name=Name(name="message", quote_style=""),
                                            namespace=None,
                                        ),
                                    ],
                                    where=BinaryOp(
                                        op=BinaryOpKind.Eq,
                                        left=ASTColumn(
                                            name=Name(
                                                name="event_type",
                                                quote_style="",
                                            ),
                                            namespace=Namespace(
                                                names=[Name(name="ce", quote_style="")],
                                            ),
                                        ),
                                        right=String(value="TRANSACTION"),
                                    ),
                                    limit=None,
                                    distinct=False,
                                ),
                            ],
                            joins=[],
                        ),
                        group_by=[],
                        having=None,
                        projection=[
                            ASTColumn(
                                name=Name(name="event_time", quote_style=""),
                                namespace=None,
                            ),
                        ],
                        where=None,
                        limit=None,
                        distinct=False,
                    ),
                ),
                (
                    ASTColumn(
                        name=Name(name="event_time", quote_style=""),
                        namespace=None,
                    ),
                    Select(
                        from_=From(
                            tables=[
                                Select(
                                    from_=From(
                                        tables=[
                                            Alias(  # type: ignore
                                                name=Name(name="ce", quote_style=""),
                                                namespace=None,
                                                child=Table(
                                                    name=Name(
                                                        name="customer_events",
                                                        quote_style="",
                                                    ),
                                                    namespace=None,
                                                ),
                                            ),
                                        ],
                                        joins=[
                                            Join(
                                                kind=JoinKind.LeftOuter,
                                                table=Alias(  # type: ignore
                                                    name=Name(
                                                        name="et",
                                                        quote_style="",
                                                    ),
                                                    namespace=None,
                                                    child=Table(
                                                        name=Name(
                                                            name="purchases",
                                                            quote_style="",
                                                        ),
                                                        namespace=None,
                                                    ),
                                                ),
                                                on=BinaryOp(
                                                    op=BinaryOpKind.Eq,
                                                    left=ASTColumn(
                                                        name=Name(
                                                            name="event_id",
                                                            quote_style="",
                                                        ),
                                                        namespace=Namespace(
                                                            names=[
                                                                Name(
                                                                    name="ce",
                                                                    quote_style="",
                                                                ),
                                                            ],
                                                        ),
                                                    ),
                                                    right=ASTColumn(
                                                        name=Name(
                                                            name="transaction_id",
                                                            quote_style="",
                                                        ),
                                                        namespace=Namespace(
                                                            names=[
                                                                Name(
                                                                    name="et",
                                                                    quote_style="",
                                                                ),
                                                            ],
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ],
                                    ),
                                    group_by=[],
                                    having=None,
                                    projection=[
                                        ASTColumn(
                                            name=Name(name="event_id", quote_style=""),
                                            namespace=None,
                                        ),
                                        ASTColumn(
                                            name=Name(
                                                name="event_time",
                                                quote_style="",
                                            ),
                                            namespace=None,
                                        ),
                                        ASTColumn(
                                            name=Name(name="message", quote_style=""),
                                            namespace=None,
                                        ),
                                    ],
                                    where=BinaryOp(
                                        op=BinaryOpKind.Eq,
                                        left=ASTColumn(
                                            name=Name(
                                                name="event_type",
                                                quote_style="",
                                            ),
                                            namespace=Namespace(
                                                names=[Name(name="ce", quote_style="")],
                                            ),
                                        ),
                                        right=String(value="TRANSACTION"),
                                    ),
                                    limit=None,
                                    distinct=False,
                                ),
                            ],
                            joins=[],
                        ),
                        group_by=[],
                        having=None,
                        projection=[
                            ASTColumn(
                                name=Name(name="event_time", quote_style=""),
                                namespace=None,
                            ),
                        ],
                        where=None,
                        limit=None,
                        distinct=False,
                    ),
                ),
            ],
            group_by=[],
            filters=[],
            ons=[],
        )

    def test_extract_dependencies_from_node(self, session: Session):
        """
        Test compound build exception when extracting dependencies
        """
        eligible_purchases = session.exec(
            select(Node).where(Node.name == "eligible_purchases"),
        ).one()
        node_dependencies, dangling_references = extract_dependencies(
            session=session,
            node=eligible_purchases,
        )

        assert len(node_dependencies) == 1
        assert len(dangling_references) == 0

        purchases = session.exec(select(Node).where(Node.name == "purchases")).one()
        assert purchases in node_dependencies

    def test_extract_dependencies_from_node_with_greater_distance(
        self,
        session: Session,
    ):
        """
        Test extracting dependencies further than immediately surrounding nodes
        """
        eligible_purchases_new_system = session.exec(
            select(Node).where(Node.name == "eligible_purchases_new_system"),
        ).one()
        node_dependencies, dangling_references = extract_dependencies(
            session=session,
            node=eligible_purchases_new_system,
        )

        assert len(node_dependencies) == 2
        assert len(dangling_references) == 0

        eligible_purchases = session.exec(
            select(Node).where(Node.name == "eligible_purchases"),
        ).one()
        purchases = session.exec(select(Node).where(Node.name == "purchases")).one()
        assert eligible_purchases in node_dependencies
        assert purchases in node_dependencies

    def test_extract_dependencies_from_node_with_exceptions(self, session: Session):
        """
        Test compound build exception when extracting dependencies
        """
        returned_transactions = session.exec(
            select(Node).where(Node.name == "returned_transactions"),
        ).one()
        with pytest.raises(CompoundBuildException) as exc_info:
            extract_dependencies(session=session, node=returned_transactions)

        assert "Found 2 issues:" in str(exc_info.value)
        assert (
            "InvalidSQLException: `transaction_id` appears in multiple "
            "references and so must be namespaced." in str(exc_info.value)
        )
        assert (
            "InvalidSQLException: `transaction_time` appears in multiple "
            "references and so must be namespaced." in str(exc_info.value)
        )

    def test_extract_dependencies_from_node_with_unraised_exceptions(
        self,
        session: Session,
    ):
        """
        Test compound build exception when extracting dependencies and not raising on exceptions
        """
        returned_transactions = session.exec(
            select(Node).where(Node.name == "returned_transactions"),
        ).one()
        node_dependencies, dangling_references = extract_dependencies(
            session=session,
            node=returned_transactions,
            raise_=False,
        )

        assert len(node_dependencies) == 2
        assert len(dangling_references) == 0

        purchases = session.exec(select(Node).where(Node.name == "purchases")).one()
        returns = session.exec(select(Node).where(Node.name == "returns")).one()
        assert purchases in node_dependencies
        assert returns in node_dependencies

    def test_extract_dependencies_from_node_with_no_query(self, session: Session):
        """
        Test compound build exception when extracting dependencies
        """

        with pytest.raises(Exception) as exc_info:
            extract_dependencies(
                session=session,
                node=Node(
                    name="queryless_node",
                    type=NodeType.TRANSFORM,
                ),
            )

        assert "Node has no query" in str(exc_info.value)

    def test_extract_dependencies_with_unknown_dep_raise_false(self, session: Session):
        """
        Test extracting dependencies with an unknown dependency with raising disabled
        """

        extract_dependencies(
            session=session,
            node=Node(
                name="foo",
                type=NodeType.TRANSFORM,
                query="select a from foobar",
            ),
            raise_=False,
        )

        assert "UnknownNodeException" in str(CompoundBuildException())

    def test_get_dj_node_raise_unknown_node_exception(self, session: Session):
        """
        Test raising an unknown node exception when calling get_dj_node
        """
        CompoundBuildException().reset()
        with pytest.raises(UnknownNodeException) as exc_info:
            get_dj_node(session, "foobar")

        assert "No  node `foobar` exists." in str(exc_info.value)

        with pytest.raises(UnknownNodeException) as exc_info:
            get_dj_node(session, "foobar", kinds={NodeType.METRIC, NodeType.DIMENSION})

        assert "NodeType.DIMENSION" in str(exc_info.value)
        assert "NodeType.METRIC" in str(exc_info.value)
        assert "NodeType.SOURCE" not in str(exc_info.value)
        assert "NodeType.TRANSFORM" not in str(exc_info.value)

        with pytest.raises(NodeTypeException) as exc_info:
            # test that the event_type raises because it's a dimension and not a transform
            get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

        assert (
            "Node `event_type` is of type `NODETYPE.DIMENSION`. "
            "Expected kind to be of NodeType.TRANSFORM. `event_type`"
        ) in str(exc_info.value)

    def test_raise_on_unknown_namespace_for_a_column(self, session: Session):
        """
        Test raising when the query is using a column
        """
        query = parse(
            "select baz.a from purchases",
            "hive",
        )
        with pytest.raises(MissingColumnException) as exc_info:
            extract_dependencies_from_query(session, query)

        assert "No namespace `baz` from which to reference column `a`." in str(
            exc_info.value,
        )

        CompoundBuildException().reset()
        CompoundBuildException().set_raise(False)
        extract_dependencies_from_query(session, query)
        CompoundBuildException().reset()
