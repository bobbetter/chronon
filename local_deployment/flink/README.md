# Running Chronon Flink Jobs Locally

## Quick Start

```bash
# 1. Build JARs (one-time)
./mill flink.assembly cloud_aws.assembly

# 2. Start Docker services
cd local_deployment && docker-compose up -d

# 3. Upload metadata (one-time per GroupBy)
cd app
python3 run.py --mode metadata-upload --conf compiled/group_bys/quickstart/returns.v1__1

# 4. Create Kafka topic
cd ../flink && make create-topics

# 5. Submit Flink job
make run-flink

# Or manually:
./run_flink_job.sh quickstart.returns.v1__1
```

## What You Need to Know

**Important**: `--mode local-streaming` runs **Spark Streaming**, not Flink!
- To run Flink, you submit JARs to the Flink cluster (that's what the script does)
- Both engines read from Kafka → compute aggregations → write to DynamoDB
- Flink has lower latency and true event-time processing

## Your GroupBy Config

`compiled/group_bys/quickstart/returns.v1__1`:
- **Source**: Kafka topic `events.returns`
- **Key**: `user_id`
- **Aggregations**: SUM, AVG, COUNT on `refund_amt` over 3d/14d/30d windows
- **Output**: Pre-aggregated tiles in DynamoDB

## Monitoring

- **Flink UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8080  
- **DynamoDB Admin**: http://localhost:8001

```bash
# View logs
docker logs -f flink-taskmanager

# List running jobs
docker exec flink-jobmanager flink list -m localhost:8081

# Cancel a job
docker exec flink-jobmanager flink cancel -m localhost:8081 <JOB_ID>
```

## Troubleshooting

| Error | Fix |
|-------|-----|
| "Metadata not found" | Run `metadata-upload` first |
| "Topic does not exist" | Run `make create-topics` |
| JAR not found | Run `./mill flink.assembly cloud_aws.assembly` |

## Architecture

```
Kafka (events.returns) → Flink Job → DynamoDB
                          ↓
                    http://localhost:8081
```

The job:
1. Reads events from Kafka
2. Keys by `user_id`  
3. Computes rolling windows (3d/14d/30d)
4. Writes aggregated tiles to DynamoDB

See `FlinkJob.scala` for implementation details.
