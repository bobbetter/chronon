FROM flink:1.17.0-scala_2.12-java11

# Install curl to download Temurin JDK tarballs
USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download and install Temurin JDK 11 (includes javac) for all architectures
ARG TEMURIN_BUILD=jdk-11.0.24+8
ARG TEMURIN_VERSION=11.0.24_8
ARG TARGETARCH

RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) temurin_arch='x64' ;; \
        arm64) temurin_arch='aarch64' ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/adoptium/temurin11-binaries/releases/download/${TEMURIN_BUILD}/OpenJDK11U-jdk_${temurin_arch}_linux_hotspot_${TEMURIN_VERSION}.tar.gz" -o /tmp/jdk.tar.gz; \
    mkdir -p /opt/java; \
    tar -xzf /tmp/jdk.tar.gz -C /opt/java --strip-components=1; \
    rm /tmp/jdk.tar.gz

ENV JAVA_HOME=/opt/java
ENV PATH=${JAVA_HOME}/bin:${PATH}

USER flink

