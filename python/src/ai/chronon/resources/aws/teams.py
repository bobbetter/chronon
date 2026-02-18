from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables
from gen_thrift.api.ttypes import Team

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.defaultCatalog": "glue_catalog",
            "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            # TODO: Please fill in the following values
            "spark.sql.catalog.glue_catalog.warehouse": "s3://zipline-warehouse-<customer_id>/data/tables/",
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            # TODO: Please fill in the following values
            "CUSTOMER_ID": "<customer_id>",
            "AWS_REGION": "<region>",
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-<customer_id>",
            "CLOUD_PROVIDER": "aws",
            "HUB_URL": "<hub-url>",  # URL to the Zipline Hub control plane
            "FRONTEND_URL": "<frontend-url>",  # URL to the Zipline Frontend
        },
    ),
)


test = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={}, modeEnvironments={RunMode.BACKFILL: {}, RunMode.UPLOAD: {}}
    ),
)

team_conf = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={},
    ),
)
