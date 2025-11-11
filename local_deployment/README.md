The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
- Build the aws jar: `./mill cloud_aws.assembly`
- Build the online jar: `./mill online.assembly`
- Build the flink jar: `./mill flink.assembly`
- Build the flink connectors jar: `./mill flink_connectors.assembly`

- Install the Python package locally: `./mill python.installEditable`


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
- Spark scripts will currently fail if new team names are used because a schema is not present. `CREATE SCHEMA IF NOT EXISTS <team_name>;`
- Build a separate Scala service that handles:
-- the Fetcher and (DONE)
-- MetaData upload (DONE)
---- but Caches are not refreshing at all (e.g. GroupBy Serving Info) . Remove or check settings.
---- Add to UI button to upload metadata for a teamName (will upload all joins in that teamName's directory)
-- Also should handle table creation for _STREAMING tables (and _BATCH)


Strange behaviors:
- Streaming jobs will look up metadata in KV that has been previously uploaded through the batch upload.
-- And this uploaded metadata must be up-to-date with the latest version of the GroupBy config, meaning, if the GroupBy config has changed, the batch-upload must run again.

