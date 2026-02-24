"""Cloud resource cleanup helpers for integration tests."""

import logging

logger = logging.getLogger(__name__)


class GCPCleanup:
    """Delete BigQuery tables whose names contain a given suffix.

    Parameters
    ----------
    project : str
        GCP project id, e.g. ``"canary-443022"``.
    dataset : str
        BigQuery dataset, e.g. ``"data"``.
    """

    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset

    def cleanup_tables(self, suffix: str) -> list[str]:
        """Remove all tables in *dataset* whose name contains *suffix*.

        Returns a list of deleted table ids.
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project)
        dataset_ref = f"{self.project}.{self.dataset}"
        deleted: list[str] = []

        for table in client.list_tables(dataset_ref):
            if suffix in table.table_id:
                full_id = f"{dataset_ref}.{table.table_id}"
                logger.info("Deleting BQ table %s", full_id)
                client.delete_table(full_id, not_found_ok=True)
                deleted.append(table.table_id)

        return deleted


class AzureCleanup:
    """Stub for Azure table cleanup (Snowflake/Iceberg targets)."""

    def __init__(self, catalog: str):
        self.catalog = catalog

    def cleanup_tables(self, suffix: str) -> list[str]:
        # TODO: implement Snowflake-based cleanup for Azure canary tables
        logger.warning("Azure table cleanup not yet implemented for suffix=%s", suffix)
        return []


class AWSCleanup:
    """Delete AWS Glue tables whose names contain a given suffix.

    Parameters
    ----------
    database : str
        Glue catalog database name.
    """

    def __init__(self, database: str):
        self.database = database

    def cleanup_tables(self, suffix: str) -> list[str]:
        """Remove all Glue tables in *database* whose name contains *suffix*.

        Returns a list of deleted table names.
        """
        import boto3

        client = boto3.client("glue")
        deleted: list[str] = []

        paginator = client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=self.database):
            for table in page["TableList"]:
                if suffix in table["Name"]:
                    logger.info("Deleting Glue table %s.%s", self.database, table["Name"])
                    client.delete_table(DatabaseName=self.database, Name=table["Name"])
                    deleted.append(table["Name"])

        return deleted
