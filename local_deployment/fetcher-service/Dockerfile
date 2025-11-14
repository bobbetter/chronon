FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.0_2.12.18 AS build

WORKDIR /workspace

# Copy dependency files first for better Docker layer caching
COPY local_deployment/fetcher-service/project ./project
COPY local_deployment/fetcher-service/build.sbt ./
COPY out/cloud_aws/assembly.dest/chronon-aws-assembly.jar ./chronon-aws-assembly.jar

# Download dependencies in a separate layer (cached if dependencies don't change)
RUN sbt -batch update

# Copy source code
COPY local_deployment/fetcher-service/src ./src

# Build the application
ENV SBT_OPTS="${SBT_OPTS} -Xms1G -Xmx3G -XX:+UseG1GC -XX:+UseStringDeduplication"
RUN sbt -batch clean stage


FROM eclipse-temurin:11-jre AS runtime

WORKDIR /opt/app

# Copy the staged application (includes all dependencies in lib/)
COPY --from=build /workspace/target/universal/stage ./

# Create the configs directory that will be mounted
RUN mkdir -p /opt/chronon/configs

EXPOSE 8083

# Use the generated start script from stage
ENTRYPOINT ["./bin/fetcher-service"]

