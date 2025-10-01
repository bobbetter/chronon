- Build the spark assembly jar: `mill spark/assembly`
- Install the Python package locally: `mill python.installEditable`
- Run `spark-shell -i ./scripts/data-loader.scala --master local[*]` to load data into spark and create tables.


- ` zipline compile` to compile the configs


Next steps:
- Need to run some sort of mill script to build the wheel and install it in the Docker container.
  - this will enable the zipline cli to be used in the Docker container e.g. `zipline run --mode backfill --conf compiled/group_bys/quickstart/purchases.v1`

