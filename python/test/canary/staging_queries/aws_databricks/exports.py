from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=0,
)
