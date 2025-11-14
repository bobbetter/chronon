load-data:
	docker exec -it local_deployment-chronon-spark-1 bash -c "cd app && spark-shell -i ./scripts/data-loader.scala --master local[*]"

compile:
	cd app && zipline compile
	make upload-meta

upload-meta:
	curl -X 'POST' \
	  'http://localhost:8083/api/v1/metadata/upload' \
	  -H 'accept: application/json' \
	  -H 'Content-Type: application/json' \
	  -d '{"teamName": "quickstart"}'

connect:
	docker exec -it local_deployment-chronon-spark-1 bash

connect-flink:
	docker exec -it local_deployment-flink-jobmanager-1 bash
