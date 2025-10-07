- Build the spark jar: `./mill spark.assembly`
- Build the online jar: `./mill online.assembly`
- Install the Python package locally: `./mill python.installEditable`

- Run `spark-shell -i ./scripts/data-loader.scala --master local[*]` to load data into spark and create tables.

- Run ` zipline compile` to compile the configs (locally)

Single GroupBy:
- Run `python3 run.py --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01`
- Run `python3 run.py --mode upload --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01`
- Run `python3 run.py --mode upload-to-kv --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01 --uploader spark`
- Fetch data back: `./fetcher/run.sh` (requires Scala 2.12.x locally, no Spark needed)

Join (multi-GroupBy):
- Run `python3 run.py --mode upload --conf=compiled/group_bys/quickstart/returns.v1__1 --ds 2023-12-01`
- Run `python3 run.py --mode upload-to-kv --conf=compiled/group_bys/quickstart/returns.v1__1 --ds 2023-12-01 --uploader spark`


To-do:
- clean up "metastore" folder, name it something like datastore and move into chronon-spark folder.
- Spark scripts will currently fail if new team names are used because a schema is not present. `CREATE SCHEMA IF NOT EXISTS <team_name>;`