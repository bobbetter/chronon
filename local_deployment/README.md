- Build the spark assembly jar: `mill spark/assembly`
- Install the Python package locally: `mill python.installEditable`

After installing Python package, run `rm -rf python/src/agent python/src/gen_thrift python/src/zipline_ai.egg-info` to delete the generated sources that otherwise prevent conflict errors.

Then: ` ./mill python.wheel` to build Python wheel


- Run `spark-shell -i ./scripts/data-loader.scala --master local[*]` to load data into spark and create tables.


- ` zipline compile` to compile the configs


Next steps:
- Need to run some sort of mill script to build the wheel and install it in the Docker container.
  - this will enable the zipline cli to be used in the Docker container e.g. `zipline run --mode backfill --conf compiled/group_bys/quickstart/purchases.v1`

To-do:
- clean up "metastore" folder, name it something like datastore and move into chronon-spark folder.