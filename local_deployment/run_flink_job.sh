#!/bin/bash
set -e

# Script to submit a Chronon Flink job to the local Flink cluster
# Usage: ./run_flink_job.sh <groupby-name> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default values
GROUPBY_NAME="${1:-quickstart.logins.v1__1}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
FLINK_JOBMANAGER="${FLINK_JOBMANAGER:-flink-jobmanager:8081}"
STREAMING_MANIFEST_PATH="${STREAMING_MANIFEST_PATH:-/tmp/flink/manifests}"
ENABLE_DEBUG="${ENABLE_DEBUG:-false}"

# Compute streaming dataset (DynamoDB table) from GROUPBY_NAME
# Example: quickstart.returns.v1__1 -> QUICKSTART_RETURNS_V1__1_STREAMING
sanitize_to_streaming() {
  local name="$1"
  # Uppercase and replace any non-alphanumeric/underscore with underscore
  local upper
  upper=$(echo -n "$name" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9_]/_/g')
  echo -n "${upper}_STREAMING"
}

STREAMING_TABLE_NAME=$(sanitize_to_streaming "$GROUPBY_NAME")

# Ensure DynamoDB table exists via REST API
ensure_ddb_table() {
  local table_name="$1"
  local fetcher_service_url="${FETCHER_SERVICE_URL:-http://localhost:8080}"
  
  echo "Creating DynamoDB table: $table_name"
  
  curl -s -X POST \
    "${fetcher_service_url}/api/v1/admin/dynamodb/create-table" \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d "{\"tableName\": \"${table_name}\", \"isTimeSorted\": true}"
}

# ensure_ddb_table "$STREAMING_TABLE_NAME"

# JAR paths inside containers (mounted via docker-compose volumes)
FLINK_JAR="/srv/chronon/jars/chronon-flink-assembly.jar"
AWS_JAR="/srv/chronon/jars/chronon-aws-assembly.jar"
FLINK_CONNECTORS_JAR="/srv/chronon/jars/chronon-flink-connectors-assembly.jar"
ONLINE_CLASS="ai.chronon.integrations.aws.AwsApiImpl"

echo "Using mounted jars inside containers:"
echo "  FLINK_JAR=$FLINK_JAR"
echo "  AWS_JAR=$AWS_JAR"
echo "  FLINK_CONNECTORS_JAR=$FLINK_CONNECTORS_JAR"

echo "=========================================="
echo "Chronon Flink Job Submission"
echo "=========================================="
echo "GroupBy Name: $GROUPBY_NAME"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo "Flink JobManager: $FLINK_JOBMANAGER"
echo "Online Class: $ONLINE_CLASS"
echo "Flink JAR (container): $FLINK_JAR"
echo "AWS JAR (container): $AWS_JAR"
echo "Flink Connectors JAR (container): $FLINK_CONNECTORS_JAR"
echo "=========================================="

# Build the flink run command
# Note: We use -C to add AWS JAR and Flink Connectors JAR to classpath (not --jars which has parsing issues)
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
  --online-class $ONLINE_CLASS \
  --kafka-bootstrap $KAFKA_BOOTSTRAP \
  --streaming-manifest-path $STREAMING_MANIFEST_PATH \
  -ZDYNAMO_ENDPOINT=http://dynamodb-local:8000 \
  -ZAWS_DEFAULT_REGION=us-west-2 \
  -ZAWS_ACCESS_KEY_ID=local \
  -ZAWS_SECRET_ACCESS_KEY=local \
  -ZKINESIS_ENDPOINT=http://localstack:4566"

# Add debug flag if enabled
if [ "$ENABLE_DEBUG" = "true" ]; then
    FLINK_RUN_CMD="$FLINK_RUN_CMD --enable-debug"
fi

# Execute the command
echo ""
echo "Executing:"
echo "$FLINK_RUN_CMD"
echo ""

echo "Submitting job to Flink cluster..."
docker exec flink-jobmanager bash -c "$FLINK_RUN_CMD"

echo ""
echo "=========================================="
echo "Job submitted successfully!"
echo "Visit http://localhost:8081 to view the Flink UI"
echo "=========================================="

