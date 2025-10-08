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

# Paths to JARs (these should be built first)
FLINK_JAR="$REPO_ROOT/out/flink/assembly.dest/chronon-flink-assembly.jar"
AWS_JAR="$REPO_ROOT/out/cloud_aws/assembly.dest/chronon-aws-assembly.jar"
ONLINE_CLASS="ai.chronon.integrations.aws.AwsApiImpl"

# Check if JARs exist
if [ ! -f "$FLINK_JAR" ]; then
    echo "ERROR: Flink JAR not found at: $FLINK_JAR"
    echo "Please build it first with: ./mill flink.assembly"
    exit 1
fi

if [ ! -f "$AWS_JAR" ]; then
    echo "ERROR: AWS JAR not found at: $AWS_JAR"
    echo "Please build it first with: ./mill cloud_aws.assembly"
    exit 1
fi

echo "=========================================="
echo "Chronon Flink Job Submission"
echo "=========================================="
echo "GroupBy Name: $GROUPBY_NAME"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo "Flink JobManager: $FLINK_JOBMANAGER"
echo "Online Class: $ONLINE_CLASS"
echo "Flink JAR: $FLINK_JAR"
echo "AWS JAR: $AWS_JAR"
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

# Check if running inside docker or from host
if [ -f /.dockerenv ]; then
    # Inside docker container
    eval "$FLINK_RUN_CMD"
else
    # From host - need to use docker exec
    echo "Copying JARs to Flink JobManager and TaskManager containers..."
    docker cp "$FLINK_JAR" flink-jobmanager:/tmp/chronon-flink-assembly.jar || true
    docker cp "$AWS_JAR" flink-jobmanager:/tmp/chronon-aws-assembly.jar || true
    # Also copy to the TaskManager so TM classloader can resolve classes referenced via -C file:// paths
    if docker ps --format '{{.Names}}' | grep -q '^flink-taskmanager$'; then
      docker cp "$FLINK_JAR" flink-taskmanager:/tmp/chronon-flink-assembly.jar || true
      docker cp "$AWS_JAR" flink-taskmanager:/tmp/chronon-aws-assembly.jar || true
    fi
    
    # Update paths for container
    FLINK_RUN_CMD=$(echo "$FLINK_RUN_CMD" | sed "s|$FLINK_JAR|/tmp/chronon-flink-assembly.jar|g")
    FLINK_RUN_CMD=$(echo "$FLINK_RUN_CMD" | sed "s|$AWS_JAR|/tmp/chronon-aws-assembly.jar|g")
    
    echo "Submitting job to Flink cluster..."
    docker exec flink-jobmanager bash -c "$FLINK_RUN_CMD"
fi

echo ""
echo "=========================================="
echo "Job submitted successfully!"
echo "Visit http://localhost:8081 to view the Flink UI"
echo "=========================================="

