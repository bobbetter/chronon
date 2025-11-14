<<<<<<< HEAD
The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
=======
This is a local deployment of the Chronon stack, including a Spark cluster for batch processing, a Flink cluster for streaming processing,
a DynamoDB local instance for the KV store, a LocalStack instance for the Kinesis stream.

There is also a simple Web UI that allows you to launch batch jobs, view data from offline and KV stores and inspect lineage of groupbys and joins.

The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
- Build the aws jar: `./mill cloud_aws.assembly`
>>>>>>> chrono_force_snapshot
- Build the online jar: `./mill online.assembly`
- Build the flink jar: `./mill flink.assembly`
- Build the flink connectors jar: `./mill flink_connectors.assembly`
- Install the Python package locally: `./mill python.installEditable`

<<<<<<< HEAD

Then you can run workflow as outlined in the Makefile:
- Run `make load-data` to load the data into the spark cluster
- Run `make compile` to compile the configs (locally)
- Run `make upload-meta` to upload the metadata to the KV store
- Run `make run-join` to run the join
- Run `make run-upload-returns` to run the returns upload
- Run `make run-upload-purchases` to run the purchases upload
- Run `make run-upload-to-kv-returns` to run the returns upload to the KV store
- Run `make run-upload-to-kv-purchases` to run the purchases upload to the KV store
- Run `make fetch-groupby` to fetch the groupby
- Run `make fetch-join` to fetch the join

To connect to the running Spark container:
- Run `make connect`

Additional GroupBy commands to backfill (outside of Join operations):
- Run `python3 run.py --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01`


To-do:
- clean up "metastore" folder, name it something like datastore and move into chronon-spark folder.
  - Also, there seems to be duplicated metastore_db folder in the chronon-spark folder. Also derby.log is duplicated.
- Spark scripts will currently fail if new team names are used because a schema is not present. `CREATE SCHEMA IF NOT EXISTS <team_name>;`
- Build a separate Scala service that handles:
-- the Fetcher and (DONE)
-- MetaData upload 
-- Also should handle table creation for _STREAMING tables (and _BATCH)


- Metadata cache (e.g. GroupBy Serving Info) is not refreshing at all it seems like.

=======
These jars are mounted into the containers via docker-compose.yml.
You can start the stack with `docker-compose up`

Not all commands are available (yet) in through the UI, so you have to run these commands manually:

- Run `make load-data` to load initial raw data (local_deployment/app/data) into tables available to the spark cluster
- Run `make compile` to compile the configs (locally)
- Run `make upload-meta` to upload join metadata to the KV store



To-do:
- Spark scripts will currently fail if new team names are used because a schema is not present. `CREATE SCHEMA IF NOT EXISTS <team_name>;`
- Fetcher Service metadata caching:
---- but Caches are not refreshing at all (e.g. GroupBy Serving Info) . Remove or check settings.
---- Add to UI button to upload metadata for a teamName (will upload all joins in that teamName's directory)
-- Also should handle table creation for _STREAMING tables (and _BATCH)


>>>>>>> chrono_force_snapshot
Strange behaviors:
- Streaming jobs will look up metadata in KV that has been previously uploaded through the batch upload.
-- And this uploaded metadata must be up-to-date with the latest version of the GroupBy config, meaning, if the GroupBy config has changed, the batch-upload must run again.

<<<<<<< HEAD

Useful commands:
docker exec dynamodb-local aws dynamodb scan --table-name QUICKSTART_RETURNS_V1__1_STREAMING --endpoint-url http://dynamodb-local:8000 --region us-west-2 --max-items 1 2>&1 | head -50
docker exec dynamodb-local aws dynamodb scan --table-name QUICKSTART_RETURNS_V1__1_BATCH --endpoint-url http://dynamodb-local:8000 --region us-west-2 --max-items 1 2>&1 | head -50


Force re-recrate chronon-ui-server container:
docker-compose up -d --force-recreate ui-server
=======
>>>>>>> chrono_force_snapshot
