#!/bin/bash
set -e

# Script to submit a Chronon Flink job to the local Flink cluster
# Usage: ./run_flink_job.sh <groupby-name> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
GROUPBY_NAME="${1:-quickstart.returns.v1__1}"
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

# Ensure DynamoDB table exists by compiling and running the Scala helper locally (uses DynamoDBKVStoreImpl)
ensure_ddb_table() {
  local table_name="$1"
  echo "Ensuring DynamoDB table exists: $table_name"
  
  local AWS_JAR="$REPO_ROOT/out/cloud_aws/assembly.dest/chronon-aws-assembly.jar"
  
  # Check for required jars
  if [ ! -f "$AWS_JAR" ]; then
      echo "Error: AWS assembly jar not found at: $AWS_JAR"
      echo "Build it with: ./mill cloud_aws.assembly"
      exit 1
  fi
  
  local SOURCE="$REPO_ROOT/local_deployment/app/scripts/create_ddb_table.scala"
  local OUTPUT_DIR="$REPO_ROOT/local_deployment/app/scripts/target"
  mkdir -p "$OUTPUT_DIR"
  
  # Use Scala 2.12.18 from coursier (compatible with project)
  local SCALAC="$HOME/Library/Application Support/Coursier/bin/scalac"
  
  if [ ! -f "$SCALAC" ]; then
      echo "Error: Scala 2.12.18 not found at expected location"
      echo "Install it with: cs install scala:2.12.18 scalac:2.12.18"
      exit 1
  fi
  
  # Compile the helper script
  "$SCALAC" -classpath "$AWS_JAR" -d "$OUTPUT_DIR" "$SOURCE" 2>/dev/null
  
  # Run it with environment pointing to local DynamoDB
  TABLE_NAME="$table_name" \
  DYNAMO_ENDPOINT="http://localhost:8000" \
  AWS_DEFAULT_REGION="us-west-2" \
  AWS_ACCESS_KEY_ID="local" \
  AWS_SECRET_ACCESS_KEY="local" \
  java -cp "$OUTPUT_DIR:$AWS_JAR" CreateDdbTable
}

ensure_ddb_table "$STREAMING_TABLE_NAME"

# JAR paths inside containers (mounted via docker-compose volumes)
FLINK_JAR="/srv/chronon/jars/chronon-flink-assembly.jar"
AWS_JAR="/srv/chronon/jars/chronon-aws-assembly.jar"
ONLINE_CLASS="ai.chronon.integrations.aws.AwsApiImpl"

echo "Using mounted jars inside containers:"
echo "  FLINK_JAR=$FLINK_JAR"
echo "  AWS_JAR=$AWS_JAR"

echo "=========================================="
echo "Chronon Flink Job Submission"
echo "=========================================="
echo "GroupBy Name: $GROUPBY_NAME"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo "Flink JobManager: $FLINK_JOBMANAGER"
echo "Online Class: $ONLINE_CLASS"
echo "Flink JAR (container): $FLINK_JAR"
echo "AWS JAR (container): $AWS_JAR"
echo "=========================================="

# Build the flink run command
# Note: We use -C to add AWS JAR to classpath (not --jars which has parsing issues)
FLINK_RUN_CMD="flink run \
  -Dclassloader.check-leaked-classloader=false \
  -Dclassloader.resolve-order=child-first \
  -m $FLINK_JOBMANAGER \
  -c ai.chronon.flink.FlinkJob \
  -C file://$AWS_JAR \
  --detached \
  $FLINK_JAR \
  --groupby-name $GROUPBY_NAME \
  --online-class $ONLINE_CLASS \
  --kafka-bootstrap $KAFKA_BOOTSTRAP \
  --streaming-manifest-path $STREAMING_MANIFEST_PATH \
  -ZDYNAMO_ENDPOINT=http://dynamodb-local:8000 \
  -ZAWS_DEFAULT_REGION=us-west-2 \
  -ZAWS_ACCESS_KEY_ID=local \
  -ZAWS_SECRET_ACCESS_KEY=local"

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

