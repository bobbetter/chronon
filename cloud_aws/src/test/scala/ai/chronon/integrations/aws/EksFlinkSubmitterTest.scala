package ai.chronon.integrations.aws

import ai.chronon.spark.submission.JobSubmitterConstants.MaxRetainedCheckpoints
import org.junit.Assert.{assertEquals, assertFalse}
import org.scalatest.flatspec.AnyFlatSpec

class EksFlinkSubmitterTest extends AnyFlatSpec {

  private val submitter = new EksFlinkSubmitter()

  private val checkpointUri = "s3://zipline-warehouse/flink-checkpoints"

  private def config(jobProperties: Map[String, String] = Map.empty): Map[String, String] =
    submitter.buildFlinkConfiguration(
      flinkCheckpointUri = checkpointUri,
      jobProperties      = jobProperties
    )

  "buildFlinkConfiguration" should "set checkpoint and backend defaults" in {
    val cfg = config()
    assertEquals(checkpointUri, cfg("state.savepoints.dir"))
    assertEquals(checkpointUri, cfg("state.checkpoints.dir"))
    assertEquals("rocksdb", cfg("state.backend.type"))
    assertEquals("true", cfg("state.backend.incremental"))
    assertEquals("filesystem", cfg("state.checkpoint-storage"))
    assertEquals("/tmp/flink-state", cfg("state.backend.rocksdb.localdir"))
    assertEquals(MaxRetainedCheckpoints, cfg("state.checkpoints.num-retained"))
  }

  it should "set IRSA credential provider and clear explicit keys" in {
    val cfg = config()
    assertEquals("", cfg("s3.access.key"))
    assertEquals("", cfg("s3.secret.key"))
    assertEquals("com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
      cfg("fs.s3a.aws.credentials.provider"))
  }

  it should "not set pipeline.jars — jars are on the classpath via FLINK_CLASSPATH env var" in {
    val cfg = config()
    assertFalse(cfg.contains("pipeline.jars"))
  }

  it should "use SmallTaskManager defaults when no TM memory is specified" in {
    val cfg = config()
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("4G", cfg("jobmanager.memory.process.size"))
    // TM memory intentionally absent — Flink uses its own defaults, pod resource falls back to 4G
    assertFalse(cfg.contains("taskmanager.memory.process.size"))
    assertEquals("4G", cfg.getOrElse("taskmanager.memory.process.size", "4G"))
    assertFalse(cfg.contains("taskmanager.memory.network.min"))
    assertFalse(cfg.contains("taskmanager.memory.managed.fraction"))
  }

  it should "use SmallTaskManager defaults for sub-32G TM memory (e.g. 8G)" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "8G"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    // jobProperties wins — 8G flows through untouched
    assertEquals("8G", cfg("taskmanager.memory.process.size"))
    assertFalse(cfg.contains("taskmanager.memory.network.min"))
  }

  it should "apply 32G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "32G"))
    assertEquals("32G", cfg("taskmanager.memory.process.size"))
    assertEquals("2", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("512m", cfg("taskmanager.memory.network.min"))
    assertEquals("1G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("512m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "apply 64G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "64G"))
    assertEquals("64G", cfg("taskmanager.memory.process.size"))
    assertEquals("4", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("1G", cfg("taskmanager.memory.network.min"))
    assertEquals("2G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("1G", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "allow jobProperties to override JM memory" in {
    val cfg = config(Map("jobmanager.memory.process.size" -> "2G"))
    assertEquals("2G", cfg("jobmanager.memory.process.size"))
  }

  it should "allow jobProperties to override any default config key" in {
    val cfg = config(Map("state.checkpoints.num-retained" -> "5"))
    assertEquals("5", cfg("state.checkpoints.num-retained"))
  }
}
