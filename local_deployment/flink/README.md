Local Flink sample: Kafka → Flink aggregation → Kafka

This sample reads JSON lines from a Kafka source topic, aggregates by user in a tumbling window, and writes the results to a sink topic.

Prereqs
- Flink Session cluster running via docker compose (services: `flink-jobmanager`, `flink-taskmanager`).
- Kafka running via docker compose (service: `kafka`, broker at `kafka:9092`).
- sbt installed locally (`brew install sbt`).

Build
```bash
cd /Users/dhamerla/Apps/chronon-zipline/local_deployment/flink
sbt clean assembly
```
The fat jar will be at `target/scala-2.12/simple-kafka-aggregation-assembly-0.1.0.jar`.

Create topics (if not already created)
```bash
docker compose -f /Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server kafka:9092 --create --topic source-events --partitions 1 --replication-factor 1 || true
docker compose -f /Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server kafka:9092 --create --topic sink-aggregates --partitions 1 --replication-factor 1 || true
```

Monitor via Kafka UI
- Open http://localhost:8080 in your browser
- View topics, messages, consumer groups
- Produce test messages directly from the UI (no command line needed!)
- Browse real-time message flow

Run the job
```bash
./run.sh
```
This copies the jar into the JobManager container and submits it with:
`--bootstrap kafka:9092 --source source-events --sink sink-aggregates --windowSec 10`.

Produce sample messages
```bash
docker compose -f /Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml exec -T kafka \
  bash -lc 'seq 1 20 | sed "s/.*/{\"user\":\"u\0\",\"val\":\0}/"' | \
  docker compose -f /Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:9092 --topic source-events
```

Consume sink results
```bash
docker compose -f /Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml exec -T kafka \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic sink-aggregates --from-beginning \
  --property print.key=true --property key.separator=,
```

Notes
- The assembly JAR bundles the Flink Kafka connector. No extra jars need to be mounted.
- Windowing is processing-time tumbling; tune with `--windowSec`.

