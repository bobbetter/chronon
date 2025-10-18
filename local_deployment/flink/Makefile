.PHONY: help clean-topics create-topics list-topics clean-kafka build test run package clean run-flink

help:
	@echo "Available targets:"
	@echo "  clean-topics    - Delete source-events and sink-aggregates topics"
	@echo "  create-topics   - Create source-events and sink-aggregates topics"
	@echo "  recreate-topics - Delete and recreate both topics (clean start)"
	@echo "  list-topics     - List all Kafka topics"
	@echo "  describe-topics - Show details for our topics"
	@echo "  build           - Compile Scala code"
	@echo "  test            - Run unit tests"
	@echo "  package         - Build assembly JAR"
	@echo "  clean           - Clean build artifacts"
	@echo "  run-flink       - Submit Chronon Flink job to local cluster"

# Kafka topic management
clean-topics:
	@echo "Deleting Kafka topics..."
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic source-events 2>/dev/null || echo "source-events doesn't exist or already deleted"
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic sink-aggregates 2>/dev/null || echo "sink-aggregates doesn't exist or already deleted"
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic events.returns 2>/dev/null || echo "events.returns doesn't exist or already deleted"
	@echo "Topics deleted!"


# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic source-events --partitions 1 --replication-factor 1 2>/dev/null || echo "source-events already exists"
# docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sink-aggregates --partitions 1 --replication-factor 1 2>/dev/null || echo "sink-aggregates already exists"
create-topics:
	@echo "Creating Kafka topics..."
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events.logins --partitions 1 --replication-factor 1 2>/dev/null || echo "events.returns already exists"
	@echo "Topics created!"

recreate-topics: clean-topics
	@sleep 1
	@$(MAKE) create-topics

list-topics:
	@echo "Available Kafka topics:"
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

describe-topics:
	@echo "=== source-events ==="
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic source-events 2>/dev/null || echo "Topic doesn't exist"
	@echo ""
	@echo "=== sink-aggregates ==="
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic sink-aggregates 2>/dev/null || echo "Topic doesn't exist"

# Clean all messages from topics without deleting them
clean-kafka:
	@echo "Cleaning all messages from topics..."
	@$(MAKE) clean-topics
	@$(MAKE) create-topics

# SBT build targets
build:
	sbt compile

test:
	sbt test

package:
	sbt assembly

clean:
	sbt clean

# Run Chronon Flink job
run-flink:
	@echo "Submitting Chronon Flink job to local cluster..."
	@bash run_flink_job.sh quickstart.returns.v1__1

