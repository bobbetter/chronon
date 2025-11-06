FROM flink:1.17.0-scala_2.12-java11

# Install full JDK (javac) for FastSerdeCache runtime code generation
USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Detect architecture and set JAVA_HOME dynamically
# This works for both arm64 and amd64
RUN ARCH=$(dpkg --print-architecture) && \
    JDK_PATH="/usr/lib/jvm/java-11-openjdk-${ARCH}" && \
    echo "JAVA_HOME=${JDK_PATH}" >> /etc/environment && \
    echo "PATH=${JDK_PATH}/bin:\${PATH}" >> /etc/environment

# Create a symlink for architecture-independent access
RUN ARCH=$(dpkg --print-architecture) && \
    ln -sf /usr/lib/jvm/java-11-openjdk-${ARCH} /usr/lib/jvm/java-11-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH=${JAVA_HOME}/bin:${PATH}

USER flink

