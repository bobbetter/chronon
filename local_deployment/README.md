The stack relies on certain .jars and the zipline Python package to be available:
- Build the spark jar: `./mill spark.assembly`
- Build the online jar: `./mill online.assembly`
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
