load-data:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && spark-shell -i ./scripts/data-loader.scala --master local[*]"

compile:
	cd app && zipline compile

upload-meta:
	cd metauploader && ./run.sh

run-join:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --conf=compiled/joins/quickstart/training_set.v1__1 --ds=2023-12-01"

run-purchases-backfill:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01"

run-upload-returns:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --mode upload --conf=compiled/group_bys/quickstart/returns.v1__1 --ds 2023-12-01"

run-upload-purchases:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --mode upload --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01"

run-upload-to-kv-returns:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --mode upload-to-kv --conf=compiled/group_bys/quickstart/returns.v1__1 --ds 2023-12-01"

run-upload-to-kv-purchases:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && python3 run.py --mode upload-to-kv --conf=compiled/group_bys/quickstart/purchases.v1__1 --ds 2023-12-01"

fetch-groupby:
	cd fetcher && ./run.sh --type=groupby

fetch-join:
	cd fetcher && ./run.sh --type=join

connect:
	docker exec -it local_deployment-chronon-spark-1 bash

delete-data:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && spark-shell --master local[*] --conf spark.chronon.database=data -i ./scripts/data-deletion.scala"

delete-quickstart:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && spark-shell --master local[*] --conf spark.chronon.database=quickstart -i ./scripts/data-deletion.scala"

connect-flink:
	docker exec -it local_deployment-flink-jobmanager-1 bash