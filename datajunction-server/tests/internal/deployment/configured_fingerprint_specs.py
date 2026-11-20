"""Shared graph specs for configured fingerprint tests."""

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)


def configured_graph_specs(
    *,
    include_region=True,
    include_debug_column=True,
    opaque_fact=False,
    required_filters=None,
):
    fact_columns = [ColumnSpec(name="amount", type="double")]
    if include_debug_column:
        fact_columns.append(ColumnSpec(name="debug", type="bigint"))
    fact_columns.extend(
        ColumnSpec(name=name, type="bigint") for name in ("country_id", "region_id")
    )
    fact = SourceSpec(
        name="ns.fact",
        catalog="warehouse",
        schema_="sales",
        table="fact",
        columns=fact_columns,
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.fact.country_id = ns.country.id",
            ),
        ],
    )
    lookup = SourceSpec(
        name="ns.lookup",
        catalog="warehouse",
        schema_="sales",
        table="lookup",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="name", type="string"),
        ],
    )
    country = DimensionSpec(
        name="ns.country",
        query="SELECT id, name FROM ns.lookup",
        columns=lookup.columns,
        primary_key=["id"],
    )
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(amount) FROM ns.fact",
        **({} if required_filters is None else {"required_filters": required_filters}),
    )
    cube = CubeSpec(
        name="ns.by_country",
        metrics=["ns.total"],
        dimensions=["ns.country.name"],
    )
    items = [fact, lookup, country, metric, cube]
    if include_region:
        items.insert(3, country.model_copy(update={"name": "ns.region"}))
    nodes = {item.rendered_name: item for item in items}
    if opaque_fact:
        outputs = ["unknown_udf(amount) AS amount"] + [
            column.name for column in fact.columns if column.name != "amount"
        ]
        nodes["ns.fact_rows"] = fact.model_copy(
            update={"name": "ns.fact_rows", "dimension_links": []},
        )
        nodes["ns.fact"] = TransformSpec(
            name="ns.fact",
            query=f"SELECT {', '.join(outputs)} FROM ns.fact_rows",
            columns=fact.columns,
            dimension_links=fact.dimension_links,
        )
    return nodes
