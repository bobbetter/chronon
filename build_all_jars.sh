#!/usr/bin/env bash
# Build all required JARs for Chronon Zipline.
# Run from repo root: ./build_all_jars.sh
set -e

echo "Building chronon-spark-assembly.jar..."
./mill spark.assembly

echo "Building chronon-aws-assembly.jar..."
./mill cloud_aws.assembly

echo "Building chronon-flink-assembly.jar..."
./mill flink.assembly

echo "Building flink-connectors out.jar..."
./mill flink_connectors.kinesis.assembly

echo "Building chronon-flink-aws uber-jar..."
./mill flink_aws.assembly

echo "All JARs built successfully."
