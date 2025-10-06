#!/bin/bash

# Standalone runner for FetcherDemo - NO SPARK REQUIRED
# Compiles with Scala 2.12.18 and runs locally

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."

# Set environment defaults
export DYNAMO_ENDPOINT="${DYNAMO_ENDPOINT:-http://localhost:8000}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-local}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-local}"

AWS_JAR="$ROOT_DIR/out/cloud_aws/assembly.dest/chronon-aws-assembly.jar"

# Check for required jars
if [ ! -f "$AWS_JAR" ]; then
    echo "Error: AWS assembly jar not found at: $AWS_JAR"
    echo "Build it with: ./mill cloud_aws.assembly"
    exit 1
fi

SOURCE="$SCRIPT_DIR/src/scala/ai/chronon/online/FetcherDemo.scala"
OUTPUT_DIR="$SCRIPT_DIR/target/classes"
mkdir -p "$OUTPUT_DIR"

# Use Scala 2.12.18 from coursier (compatible with project)
SCALAC="$HOME/Library/Application Support/Coursier/bin/scalac"

if [ ! -f "$SCALAC" ]; then
    echo "Error: Scala 2.12.18 not found at expected location"
    echo "Install it with: cs install scala:2.12.18 scalac:2.12.18"
    exit 1
fi

echo "Compiling FetcherDemo.scala with Scala 2.12.18..."
"$SCALAC" -classpath "$AWS_JAR" -d "$OUTPUT_DIR" "$SOURCE"

echo "Running FetcherDemo..."
echo ""
JAVA_OPTS="-Dlogback.configurationFile=$SCRIPT_DIR/logback.xml"
java $JAVA_OPTS -cp "$OUTPUT_DIR:$AWS_JAR" ai.chronon.online.FetcherDemo "$@"

