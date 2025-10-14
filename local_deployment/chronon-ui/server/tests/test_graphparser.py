from server.services.graphparser import GraphParser

compiled_data = {
  "aggregations": [
    {
      "argMap": {},
      "inputColumn": "session_id",
      "operation": 5,
      "windows": [
        {
          "length": 3,
          "timeUnit": 1
        },
        {
          "length": 14,
          "timeUnit": 1
        },
        {
          "length": 30,
          "timeUnit": 1
        }
      ]
    },
    {
      "argMap": {},
      "buckets": [
        "product_id"
      ],
      "inputColumn": "session_id",
      "operation": 6,
      "windows": [
        {
          "length": 3,
          "timeUnit": 1
        },
        {
          "length": 14,
          "timeUnit": 1
        },
        {
          "length": 30,
          "timeUnit": 1
        }
      ]
    },
    {
      "argMap": {
        "k": "5"
      },
      "inputColumn": "product_id",
      "operation": 13,
      "windows": [
        {
          "length": 3,
          "timeUnit": 1
        },
        {
          "length": 14,
          "timeUnit": 1
        },
        {
          "length": 30,
          "timeUnit": 1
        }
      ]
    }
  ],
  "backfillStartDate": "2025-10-01",
  "keyColumns": [
    "user_id"
  ],
  "metaData": {
    "columnHashes": {
      "product_id_last5_14d": "36168c4ed2d991b0ff7b54a69e031de8",
      "product_id_last5_30d": "36168c4ed2d991b0ff7b54a69e031de8",
      "product_id_last5_3d": "36168c4ed2d991b0ff7b54a69e031de8",
      "session_id_approx_unique_count_14d": "7a1ceae4106c35037bf31bd7fc911194",
      "session_id_approx_unique_count_30d": "7a1ceae4106c35037bf31bd7fc911194",
      "session_id_approx_unique_count_3d": "7a1ceae4106c35037bf31bd7fc911194",
      "session_id_count_14d_by_product_id": "7a1ceae4106c35037bf31bd7fc911194",
      "session_id_count_30d_by_product_id": "7a1ceae4106c35037bf31bd7fc911194",
      "session_id_count_3d_by_product_id": "7a1ceae4106c35037bf31bd7fc911194",
      "user_id": "01de5ae7e16db4373fd773f33e38e521"
    },
    "customJson": "{\"airflowDependencies\": [{\"name\": \"wf_data_page_views_with_offset_0\", \"spec\": \"data.page_views/<partition-column-name>={{ macros.ds_add(ds, 0) }}\"}]}",
    "executionInfo": {
      "conf": {
        "common": {
          "spark.chronon.coalesce.factor": "10",
          "spark.chronon.partition.column": "<partition-column-name>",
          "spark.chronon.partition.format": "<date-format>",
          "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
          "spark.chronon.table_write.format": "iceberg",
          "spark.default.parallelism": "10",
          "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",
          "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
          "spark.sql.catalog.bigquery_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
          "spark.sql.catalog.bigquery_catalog.gcp.bigquery.location": "<region>",
          "spark.sql.catalog.bigquery_catalog.gcp.bigquery.project-id": "<project-id>",
          "spark.sql.catalog.bigquery_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
          "spark.sql.catalog.bigquery_catalog.warehouse": "gs://zipline-warehouse-<customer_id>/data/tables/",
          "spark.sql.defaultCatalog": "bigquery_catalog",
          "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
          "spark.sql.shuffle.partitions": "10"
        }
      },
      "env": {
        "common": {
          "ARTIFACT_PREFIX": "<customer-artifact-bucket>",
          "CLOUD_PROVIDER": "aws",
          "CUSTOMER_ID": "<customer_id>",
          "GCP_BIGTABLE_INSTANCE_ID": "<bigtable-instance-id>",
          "GCP_DATAPROC_CLUSTER_NAME": "<dataproc-cluster-name>",
          "GCP_PROJECT_ID": "<project-id>",
          "GCP_REGION": "<region>"
        }
      },
      "historicalBackfill": 0,
      "scheduleCron": "@daily"
    },
    "name": "quickstart.page_views.v1__1",
    "online": 1,
    "outputNamespace": "quickstart",
    "sourceFile": "group_bys/quickstart/page_views.py",
    "team": "quickstart",
    "version": "1"
  },
  "sources": [
    {
      "events": {
        "query": {
          "selects": {
            "product_id": "product_id",
            "session_id": "session_id",
            "user_id": "user_id"
          },
          "startPartition": "2025-10-01",
          "timeColumn": "ts"
        },
        "table": "data.page_views"
      }
    }
  ]
}

expected_graph = {
  "nodes": [
    {
      "name": "quickstart.page_views.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"]
    },
    {
      "name": "data.page_views",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": True,
      "actions": ["show"]
    },
    {
      "name": "quickstart_page_views_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"]
    },
    {
      "name": "quickstart_page_views_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"]
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart_page_views_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": False
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart_page_views_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": False
    },
  ]
}

def test_graphparser_with_compiled_data():
    graph_parser = GraphParser(compiled_data)
    graph = graph_parser.parse()
    assert graph == expected_graph

def test_graphparser_with_directory_path():
    graph_parser = GraphParser("/Users/dhamerla/Apps/chronon-zipline-ui/server/tests/compiled/one_groupby")
    graph = graph_parser.parse()
    assert graph == expected_graph