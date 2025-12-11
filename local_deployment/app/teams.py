from gen_thrift.api.ttypes import Team
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.defaultCatalog": "bigquery_catalog",
            "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            # TODO: Please fill in the following values
            "spark.sql.catalog.bigquery_catalog.warehouse": "gs://zipline-warehouse-<customer_id>/data/tables/",
            "spark.sql.catalog.bigquery_catalog.gcp.bigquery.location": "<region>",
            "spark.sql.catalog.bigquery_catalog.gcp.bigquery.project-id": "<project-id>",
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            # TODO: Please fill in the following values
            "EMR_APPLICATION_ID": "00fvgrsoabsudb0d",
            "CLOUD_PROVIDER": "aws",
        },
    ),
)



quickstart = Team(
    outputNamespace="quickstart",
    env=EnvironmentVariables(
        common={},
    ),
)


mli = Team(
    outputNamespace="mli",
    env=EnvironmentVariables(
        common={},
    ),
)