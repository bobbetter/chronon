FROM flink:1.17.0-scala_2.12-java11

# Install full JDK (javac) for FastSerdeCache runtime code generation
USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Update JAVA_HOME to point to the full JDK instead of JRE
# Architecture is arm64 (aarch64) for this base image
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=${JAVA_HOME}/bin:${PATH}

USER flink

