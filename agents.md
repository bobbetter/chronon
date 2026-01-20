Notes for running individual tests in `chronon-zipline`

## Running Single Test Suites

Mill exposes `<module>.test.testOnly` for running individual test classes:

```bash
./mill -i <module>.test.testOnly <fully.qualified.TestClassName>
```

## Formatting

Check for formatting issues:
./mill __.checkFormat

Fixing formatting:
./mill __.reformat

### Module Examples

| Module | Command |
|--------|---------|
| spark | `./mill -i spark.test.testOnly ai.chronon.spark.upload.IonWriterTest` |
| flink_connectors | `./mill -i flink_connectors.test.testOnly ai.chronon.flink_connectors.kinesis.KinesisConfigSpec` |
| flink | `./mill -i flink.test.testOnly ai.chronon.flink.SomeFlinkTest` |
| api | `./mill -i api.test.testOnly ai.chronon.api.SomeApiTest` |
| online | `./mill -i online.test.testOnly ai.chronon.online.SomeOnlineTest` |
| cloud_aws | `./mill -i cloud_aws.test.testOnly ai.chronon.integrations.aws.DynamoDBKVStoreTest` |
| cloud_gcp | `./mill -i cloud_gcp.test.testOnly ai.chronon.integrations.cloud_gcp.SomeGcpTest` |

### Cross-compilation (Specific Scala Version)

If a specific Scala version is needed, run against the cross module:
```bash
./mill -i 'spark[2.13.17].test.testOnly ai.chronon.spark.upload.IonWriterTest'
```

### Running All Tests in a Module

```bash
./mill -i <module>.test
```

## Sandbox Restrictions

- In sandboxed environments, Spark tests may fail to bind local ports with `java.net.SocketException: Operation not permitted`. Rerun outside sandbox with `required_permissions: ['all']` if that appears.
- Tests requiring network access (e.g., downloading dependencies) need `required_permissions: ['network']`.
