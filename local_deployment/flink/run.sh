#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/dhamerla/Apps/chronon-zipline/local_deployment/flink"
JAR_PATH="$ROOT_DIR/target/scala-2.12/simple-kafka-aggregation-assembly-0.1.0.jar"
COMPOSE_YML="/Users/dhamerla/Apps/chronon-zipline/local_deployment/docker-compose.yml"

if [ ! -f "$JAR_PATH" ]; then
  echo "Jar not found at $JAR_PATH. Building via sbt assembly..."
  (cd "$ROOT_DIR" && sbt -Dsbt.color=false clean assembly)
fi

echo "Copying jar into flink-jobmanager container..."
CONTAINER_ID=$(docker compose -f "$COMPOSE_YML" ps -q flink-jobmanager)
if [ -z "$CONTAINER_ID" ]; then
  echo "Flink JobManager container not running. Start it with docker compose up -d flink-jobmanager"
  exit 1
fi
docker compose -f "$COMPOSE_YML" exec flink-jobmanager mkdir -p /opt/flink/usrlib
docker cp "$JAR_PATH" "$CONTAINER_ID":/opt/flink/usrlib/

echo "Submitting job..."
docker compose -f "$COMPOSE_YML" exec flink-jobmanager \
  /opt/flink/bin/flink run -d \
  /opt/flink/usrlib/$(basename "$JAR_PATH") \
  --bootstrap kafka:9092 \
  --source source-events \
  --sink sink-aggregates \

echo "Submitted. Check Flink UI at http://localhost:8081"


