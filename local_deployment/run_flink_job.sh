#!/bin/bash
set -e

# Submit a Chronon Flink job to the local Flink cluster
# Usage: ./run_flink_job.sh <groupby-name>

# Configuration
GROUPBY_NAME="${1:-quickstart.logins.v1__1}"
STREAM_NAME="login-events"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
FLINK_JOBMANAGER="${FLINK_JOBMANAGER:-flink-jobmanager:8081}"
STREAMING_MANIFEST_PATH="${STREAMING_MANIFEST_PATH:-/tmp/flink/manifests}"
ENABLE_DEBUG="${ENABLE_DEBUG:-false}"

# REST API endpoints
FETCHER_SERVICE_URL="${FETCHER_SERVICE_URL:-http://localhost:8083}"
UI_BACKEND_URL="${UI_BACKEND_URL:-http://localhost:8005}"

# Convert groupby name to streaming table name (e.g., quickstart.logins.v1__1 -> QUICKSTART_LOGINS_V1__1_STREAMING)
STREAMING_TABLE_NAME=$(echo -n "$GROUPBY_NAME" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9_]/_/g')_STREAMING

echo "=========================================="
echo "Chronon Flink Job Submission"
echo "=========================================="
echo "GroupBy: $GROUPBY_NAME"
echo "Stream: $STREAM_NAME"
echo "Table: $STREAMING_TABLE_NAME"
echo "=========================================="

# Create DynamoDB table (fetcher-service)
echo "Creating DynamoDB table..."
curl -s -X POST "${FETCHER_SERVICE_URL}/api/v1/admin/dynamodb/create-table" \
  -H 'Content-Type: application/json' \
  -d "{\"tableName\": \"${STREAMING_TABLE_NAME}\", \"isTimeSorted\": true}"

# Create Kinesis stream (ui-backend)
echo "Creating Kinesis stream..."
curl -s -X POST "${UI_BACKEND_URL}/v1/kinesis/${STREAM_NAME}" \
  -H 'Content-Type: application/json' \
  -d "{\"shard_count\": ${SHARD_COUNT:-1}}"

# JAR paths (mounted inside containers)
FLINK_JAR="/srv/chronon/jars/chronon-flink-assembly.jar"
AWS_JAR="/srv/chronon/jars/chronon-aws-assembly.jar"
FLINK_CONNECTORS_JAR="/srv/chronon/jars/chronon-flink-connectors-assembly.jar"

# Build flink run command
FLINK_RUN_CMD="flink run \
  -Dclassloader.check-leaked-classloader=false \
  -Dclassloader.resolve-order=child-first \
  -m $FLINK_JOBMANAGER \
  -c ai.chronon.flink.FlinkJob \
  -C file://$AWS_JAR \
  -C file://$FLINK_CONNECTORS_JAR \
  --detached \
  $FLINK_JAR \
  --groupby-name $GROUPBY_NAME \
  --online-class ai.chronon.integrations.aws.AwsApiImpl \
  --kafka-bootstrap $KAFKA_BOOTSTRAP \
  --streaming-manifest-path $STREAMING_MANIFEST_PATH \
  -ZDYNAMO_ENDPOINT=http://dynamodb-local:8000 \
  -ZAWS_DEFAULT_REGION=us-west-2 \
  -ZAWS_ACCESS_KEY_ID=local \
  -ZAWS_SECRET_ACCESS_KEY=local \
  -ZKINESIS_ENDPOINT=http://localstack:4566"

[ "$ENABLE_DEBUG" = "true" ] && FLINK_RUN_CMD="$FLINK_RUN_CMD --enable-debug"

# Submit job
echo "Submitting job to Flink..."
docker exec flink-jobmanager bash -c "$FLINK_RUN_CMD"

echo ""
echo "✓ Job submitted successfully!"
echo "  Flink UI: http://localhost:8081"
echo "=========================================="
