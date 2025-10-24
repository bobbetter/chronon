FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.0_2.12.18 AS build

WORKDIR /workspace

# Copy dependency files first for better Docker layer caching
COPY project ./project
COPY build.sbt ./
COPY chronon-aws-assembly.jar ./

# Download dependencies in a separate layer (cached if dependencies don't change)
RUN sbt -batch update

# Copy source code
COPY src ./src

# Build the application
ENV SBT_OPTS="${SBT_OPTS} -Xms1G -Xmx3G -XX:+UseG1GC -XX:+UseStringDeduplication"
RUN sbt -batch clean stage


FROM eclipse-temurin:11-jre AS runtime

WORKDIR /opt/app

# Copy the staged application (includes all dependencies in lib/)
COPY --from=build /workspace/target/universal/stage ./
COPY chronon-aws-assembly.jar ./lib/

EXPOSE 8080

ENV FETCHER_SERVICE_HOST=0.0.0.0 \
    FETCHER_SERVICE_PORT=8080 \
    DYNAMO_ENDPOINT=http://localhost:8000 \
    AWS_DEFAULT_REGION=us-west-2 \
    AWS_ACCESS_KEY_ID=local \
    AWS_SECRET_ACCESS_KEY=local

# Use the generated start script from stage
ENTRYPOINT ["./bin/fetcher-service"]

