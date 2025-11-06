+#!/bin/bash


# Validate environment variables
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set"
    exit 1
fi


if [ -z "$CHRONON_SPARK_JAR" ]; then
    echo "Error: CHRONON_SPARK_JAR is not set"
    exit 1
fi


java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
     --add-opens=java.base/sun.security.action=ALL-UNNAMED \
     --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
     -cp "$SPARK_HOME/jars/*:$CHRONON_SPARK_JAR" ai.chronon.spark.interactive.Evaluator