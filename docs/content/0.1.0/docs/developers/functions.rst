---
weight: 55
---

.. _functions:

---------
Functions
---------

Currently, DJ supports only a small subset of SQL functions, limiting the definition of metrics. In order to add new functions to DJ we need to implement two things:

1. Type inference for the function. This is easier in some functions and harder in others. For example, the ``COUNT`` function always return an integer, so its return type is the same regardless of the input arguments. The ``MAX`` function, on the other hands, return a value with the same type as the input argument.

2. Translation to SQLAlchemy. In order to run queries, DJ parses the metric definitions (written in ANSI SQL), and converts them to a SQLAlchemy query object, so it can be translated to different dialects (Hive, Trino, Postgres, etc.). Some functions are easier to translate, especially if they are already defined in ``sqlalchemy.sql.functions``. Others, like ``DATE_TRUNC``, are more complex because they are dialect specific.

Supported functions
-------------------

``AVG``
-------

Return the average of a given column:

.. code-block:: sql

    > SELECT AVG(column);
    1.2

``COALESCE``
------------

Return the first non-null value:

.. code-block:: sql

    > SELECT column_a, column_b FROM some_table;
    1, NULL
    NULL, 10
    NULL, NULL
    > SELECT COALESCE(column_a, column_b, -1) FROM some_table;
    1
    10
    -1

``COUNT``
---------

The humble ``COUNT()``.

.. code-block:: sql

    > SELECT COUNT(*) FROM some_table;
    10
    > SELECT COUNT(1) FROM some_table;
    10
    > SELECT COUNT(column) FROM some_table;  -- ignores NULLs
    5

``DATE_TRUNC``
--------------

Truncate a ``TIMESTAMP`` column to a given resolution:

.. code-block:: sql

    > SELECT DATE_TRUNC('minute', CAST('2022-01-01T12:34:56Z' AS TIMESTAMP);
    2022-01-01T12:34:00Z

``MAX``
-------

Return the maximum value from a column:

.. code-block:: sql

    > SELECT MAX(column);

``MIN``
-------

Return the minimum value from a column:

.. code-block:: sql

    > SELECT MIN(column);
    1

``SUM``
-------

Return the sum of a given column:

.. code-block:: sql

    > SELECT SUM(sales)
    12345

Adding new functions
--------------------

Let's look at the ``COUNT`` function in DJ:

.. code-block:: python

    from sqlalchemy.sql import func
    from sqlalchemy.sql.schema import Column as SqlaColumn

    from dj.models.column import Column
    from dj.sql.parsing.types import ColumnType; import dj.sql.parsing.types as ct


    class Count(Function):
        """
        The ``COUNT`` function.
        """

        is_aggregation = True

        @staticmethod
        def infer_type(argument: Union[Column, "Wildcard", int]) -> ColumnType:
            return ColumnType.INT

        @staticmethod
        def get_sqla_function(
            argument: Union[SqlaColumn, str, int],
            *,
            dialect: Optional[str] = None,
        ) -> SqlaFunction:
            return func.count(argument)


The first method, ``infer_type``, is responsible for type inference. The function is usually called as ``COUNT(column)``, ``COUNT(1)`` or ``COUNT(*)``, so we define the input argument as either a column, a star, or a number. In retrospect we could have also added a default value, to make ``COUNT`` valid. We can see that the method always return an integer.

Compare that to the same method in the ``MAX`` function

.. code-block:: python

    class Max(Function):

        @staticmethod
        def infer_type(column: Column) -> ColumnType:
            return column.type

``MAX`` takes a column, and returns a value with the same type as the column.

Now let's look at the second method, ``get_sqla_function``, which is responsible for translating the function and its arguments to a SQLAlchemy function. For ``COUNT`` the method is very simple, because SQLAlchemy already has the `function defined <https://github.com/sqlalchemy/sqlalchemy/blob/13a8552053c21a9fa7ff6f992ed49ee92cca73e4/lib/sqlalchemy/sql/functions.py#L1278>`_.

But what should we do when the function is not defined in SQLAlchemy? The ``func`` object in SQLAlchemy is a special function generator, and it accepts **any** attribute. If the function exists, like ``func.count``, SQLAlchemy will know how to translate that function to different dialects, and also its return type. If the function doesn't exist, on the other hand, SQLAlchemy will just translate it as-is. For example, the code ``func.my_function(1)`` will be translated to ``my_function(1)``, and will probably fail when ran in a database.

Let's take a look at the ``DATE_TRUNC`` function to understand this better. Some databases (like Trino and Postgres) support ``DATE_TRUNC``, while others (like Druid and SQLite) don't. We can write our method like this, then:

.. code-block:: python

    class DateTrunc(Function):

        """
        Truncate a datetime column to a given resolution.

        Eg:

            > DATE_TRUNC('day', TIMESTAMP '2022-01-01T12:34:56Z')
            2022-01-01T00:00:00Z

        """

        @staticmethod
        def get_sqla_function(
            resolution: TextClause,
            column: SqlaColumn,
            *,
            dialect: Optional[str] = None,
        ) -> SqlaFunction:
            if dialect is None:
                raise Exception("A dialect is needed for `DATE_TRUNC`")

            if dialect in DATE_TRUNC_DIALECTS:
                return func.date_trunc(str(resolution), column, type_=DateTime)

            if dialect in SQLITE_DIALECTS:
                if str(resolution) == "minute":
                    return func.datetime(
                        func.strftime("%Y-%m-%dT%H:%M:00", column),
                        type_=DateTime,
                    )
                ...
            ...

The first thing to notice is that ``DATE_TRUNC`` **requires** a dialect, since it's not a standard function. If the dialect is in the set of dialects that support ``DATE_TRUNC`` natively we can simply translate the function to that using ``func.date_trunc``. Note that when using a custom function we should inform SQLAlchemy of the return type, using the ``type_`` argument.

If the dialect doesn't support ``DATE_TRUNC`` and is part of the SQLite family we can implement the function using other functions supported by the dialect. In the code above we're translating a call like this:

.. code-block:: sql

    DATE_TRUNC('minute', column)

To:

.. code-block:: sql

    TIMESTAMP(STRFTIME("%Y-%m-%dT%H:%M:00", column))


The code above converts the column to a string, replacing the seconds with zeros, and then converts it back to a datetime, reproducing the behavior of ``DATE_TRUNC('minute', column)``.