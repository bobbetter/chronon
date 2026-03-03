#!/usr/bin/env bash
# Build all required JARs for Chronon Zipline.
# Run from repo root: ./build_all_jars.sh
set -e

echo "Building chronon-spark-assembly.jar..."
./mill spark.assembly

echo "Building chronon-aws-assembly.jar..."
./mill cloud_aws.assembly

# flink_aws is an uber-JAR that bundles flink core, flink_connectors/kinesis,
# cloud_aws, and Spark deps into a single JAR used by both local and remote Flink.
echo "Building chronon-flink-aws-assembly.jar..."
./mill flink_aws.assembly

echo "All JARs built successfully."
