#!/bin/bash
set -e

# Validate required environment variables
required_vars=("CLASSPATH" "CLOUD_GCP_JAR" "DRIVER_JAR_PATH" "CHRONON_ONLINE_CLASS" "GCP_PROJECT_ID" "GCP_INSTANCE_ID")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

echo "Compiling configs"
if ! compile.py --conf=joins/quickstart/training_set.py; then
  echo "Error: Failed to compile config" >&2
  exit 1
fi

echo "Loading source table data"
if ! spark-shell -i scripts/data-loader.scala; then
  echo "Error: Failed to load source table data" >&2
  exit 1
fi

echo "Running GroupBy Uploads Batch Jobs"
for dataset in purchases returns; do
  if ! run.py --mode upload --conf production/group_bys/quickstart/$dataset.v1 --ds 2023-11-30; then
    echo "Error: Failed to run GroupBy upload batch job for $dataset" >&2
    exit 1
  fi
done
echo "GroupBy upload batch jobs completed successfully!"

echo "Uploading tables to KV Store"
for dataset in purchases returns; do
  if ! spark-submit --driver-class-path "$CLASSPATH:/opt/custom-jars/*" \
    --jars "/opt/custom-jars/spark-bigtable_2.12-0.2.1.jar,/opt/custom-jars/log4j-slf4j-impl-2.20.0.jar" \
    --class ai.chronon.integrations.cloud_gcp.Spark2BigTableLoader \
    --master local[*] $CLOUD_GCP_JAR --table-name default.quickstart_${dataset}_v1_upload --dataset quickstart.${dataset}.v1 \
    --end-ds 2023-11-30 --project-id $GCP_PROJECT_ID --instance-id $GCP_INSTANCE_ID; then
    echo "Error: Failed to upload table to KV Store" >&2
    exit 1
  fi
done
echo "Tables uploaded to KV Store successfully!"

echo "Loading metadata.."
if ! java -cp $DRIVER_JAR_PATH:$CLASSPATH ai.chronon.spark.Driver metadata-upload --conf-path=production/joins/quickstart/training_set.v2 --online-jar=$CLOUD_GCP_JAR --online-class=$CHRONON_ONLINE_CLASS; then
  echo "Error: Failed to load metadata into DynamoDB" >&2
  exit 1
fi
echo "Metadata load completed successfully!"
echo "Done computing and uploading all serving data to BigTable! 🥳"
