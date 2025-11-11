This is a local deployment of the Chronon stack, including a Spark cluster for batch processing, a Flink cluster for streaming processing,
a DynamoDB local instance for the KV store, a LocalStack instance for the Kinesis stream.

There is also a simple Web UI that allows you to launch batch jobs, view data from offline and KV stores and inspect lineage of groupbys and joins.

The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
- Build the aws jar: `./mill cloud_aws.assembly`
- Build the online jar: `./mill online.assembly`
- Build the flink jar: `./mill flink.assembly`
- Build the flink connectors jar: `./mill flink_connectors.assembly`
- Install the Python package locally: `./mill python.installEditable`

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


Strange behaviors:
- Streaming jobs will look up metadata in KV that has been previously uploaded through the batch upload.
-- And this uploaded metadata must be up-to-date with the latest version of the GroupBy config, meaning, if the GroupBy config has changed, the batch-upload must run again.

