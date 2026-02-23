package ai.chronon.flink_connectors.kinesis

import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec

/** Integration test for GlueSchemaSerDe against a real AWS Glue Schema Registry.
  *
  * This test is ignored by default since it requires:
  * 1. AWS credentials configured (via env vars, ~/.aws/config, or IAM role)
  * 2. Access to AWS Glue Schema Registry
  * 3. Environment variables: REGISTRY_NAME, SCHEMA_NAME
  *
  * To enable and run this test, set RUN_INTEGRATION_TESTS=true:
  *   RUN_INTEGRATION_TESTS=true REGISTRY_NAME=zipline-canary SCHEMA_NAME=user-activities ./mill flink_connectors.kinesis.test.testOnly "ai.chronon.flink_connectors.kinesis.GlueSchemaSerDeIntegrationSpec"
  *
  * With specific version:
  *   RUN_INTEGRATION_TESTS=true REGISTRY_NAME=zipline-canary SCHEMA_NAME=user-activities VERSION_NUMBER=1 ./mill flink_connectors.kinesis.test.testOnly "ai.chronon.flink_connectors.kinesis.GlueSchemaSerDeIntegrationSpec"
  *
  * With explicit region:
  *   RUN_INTEGRATION_TESTS=true AWS_REGION=us-west-2 REGISTRY_NAME=zipline-canary SCHEMA_NAME=user-activities ./mill flink_connectors.kinesis.test.testOnly "ai.chronon.flink_connectors.kinesis.GlueSchemaSerDeIntegrationSpec"
  */
class GlueSchemaSerDeIntegrationSpec extends AnyFlatSpec {

  val runIntegrationTests = sys.env.get("RUN_INTEGRATION_TESTS").contains("true")

  def integrationTest(testName: String)(testFun: => Any): Unit = {
    if (runIntegrationTests) {
      it should testName in testFun
    } else {
      ignore should testName in testFun
    }
  }

  integrationTest("retrieve schema from real AWS Glue Schema Registry") {
    val registryName = sys.env.get("REGISTRY_NAME")
    val schemaName = sys.env.get("SCHEMA_NAME")
    val versionNumber = sys.env.get("VERSION_NUMBER")
    val region = sys.env.get("AWS_REGION")

    require(registryName.isDefined, "REGISTRY_NAME environment variable is required")
    require(schemaName.isDefined, "SCHEMA_NAME environment variable is required")

    println(s"\n=== AWS Glue Schema Registry Integration Test ===")
    println(s"Configuration:")
    println(s"  Registry Name: ${registryName.get}")
    println(s"  Schema Name: ${schemaName.get}")
    versionNumber match {
      case Some(v) => println(s"  Version Number: $v")
      case None    => println("  Version: latest")
    }
    println(s"  Region: ${region.getOrElse("(using AWS default region provider chain)")}")
    println()

    val params = Map(
      GlueSchemaSerDe.RegistryNameKey -> registryName.get,
      GlueSchemaSerDe.SchemaNameKey -> schemaName.get
    ) ++ versionNumber.map(GlueSchemaSerDe.VersionNumberKey -> _).toMap ++
      region.map(GlueSchemaSerDe.RegionKey -> _).toMap

    val topicInfo = TopicInfo("test-stream", "kinesis", params)

    println("Creating GlueSchemaSerDe...")
    val serDe = new GlueSchemaSerDe(topicInfo)

    println("Retrieving schema from AWS Glue Schema Registry...")
    val schema = serDe.schema

    println(s"\n✓ SUCCESS! Schema retrieved successfully.\n")
    println("Schema Details:")
    println(s"  Type: ${schema.getClass.getSimpleName}")
    println(s"  Fields: ${schema.fields.length}")
    println("\nField List:")
    schema.fields.zipWithIndex.foreach { case (field, idx) =>
      println(s"  ${idx + 1}. ${field.name} (${field.fieldType.getClass.getSimpleName})")
    }

    println(s"\nFull Schema:")
    println(schema)

    // Assertions
    assert(schema != null, "Schema should not be null")
    assert(schema.fields.length > 0, "Schema should have at least one field")
  }
}
