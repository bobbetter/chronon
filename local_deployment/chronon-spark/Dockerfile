FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

# Install full JDK (javac) and prefer it over the JRE bundled in the base image
USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64 \
    SPARK_HOME=/opt/spark \
    SCALA_HOME=/opt/scala \
    SPARK_CONF_DIR=/srv/chronon/conf \
    PATH=/usr/lib/jvm/java-11-openjdk-arm64/bin:/opt/spark/bin:$PATH

WORKDIR /srv/chronon

RUN mkdir -p \
    /srv/chronon/conf \
    /srv/chronon/data \
    /srv/chronon/metastore \
    /srv/chronon/jars \
    /srv/chronon/app


USER spark

CMD ["bash", "-lc", "sleep infinity"]