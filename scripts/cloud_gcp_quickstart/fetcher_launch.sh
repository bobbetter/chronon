#!/bin/bash
set -e

# Required environment variables
required_vars=("FETCHER_JAR" "CLOUD_GCP_JAR" "STATSD_HOST" "CHRONON_ONLINE_CLASS")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

echo "Starting Fetcher service"

if ! java -jar $FETCHER_JAR run ai.chronon.service.FetcherVerticle \
  -Dserver.port=9000 \
  -Donline.jar=$CLOUD_GCP_JAR \
  -Dai.chronon.metrics.host=$STATSD_HOST \
  -Donline.class=$CHRONON_ONLINE_CLASS; then
  echo "Error: Fetcher service failed to start"
  exit 1
fi
