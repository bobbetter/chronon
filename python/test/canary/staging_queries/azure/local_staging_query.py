from ai.chronon.staging_query import StagingQuery, TableDependency

# Simple staging query reading from demo-v2.data.test_connection
simple = StagingQuery(
    query="""
SELECT
    id,
    {{ end_date }} as ds
FROM data.test_connection
""",
    output_namespace="data",
    dependencies=[
        TableDependency(table="data.test_connection", partition_column="ds", offset=0)
    ],
    version=0,
)
