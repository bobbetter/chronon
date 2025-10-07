The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
- Build the online jar: `./mill online.assembly`
- Install the Python package locally: `./mill python.installEditable`

Initially you need to load the raw example dataset into spark and create tables:
- Run `spark-shell -i ./scripts/data-loader.scala --master local[*]`


Then you can run workflow as outlined in the Makefile:
- Run `make compile` to compile the configs (locally)
- Run `make upload-meta` to upload the metadata to the KV store
- Run `make run-join` to run the join
- Run `make run-upload-returns` to run the returns upload
- Run `make run-upload-purchases` to run the purchases upload
- Run `make fetch-groupby` to fetch the groupby
- Run `make fetch-join` to fetch the join

To connect to the running Spark container:
- Run `make connect`

Additional GroupBy commands to backfill (outside of Join operations):
- Run `python3 run.py --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01`




To-do:
- clean up "metastore" folder, name it something like datastore and move into chronon-spark folder.
- Spark scripts will currently fail if new team names are used because a schema is not present. `CREATE SCHEMA IF NOT EXISTS <team_name>;`
- Build a separate Scala service that handles the Fetcher and MetaData upload operations that are currently standalone Scala scripts (./fetcher/... and ./metauploader/...)