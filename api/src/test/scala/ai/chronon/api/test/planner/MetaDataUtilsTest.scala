package ai.chronon.api.test.planner

import ai.chronon.api.{
  Builders,
  ClusterConfigProperties,
  ConfigProperties,
  ExecutionInfo,
  PartitionSpec,
  TableDependency
}
import ai.chronon.api.planner.MetaDataUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.collection.JavaConverters._

class MetaDataUtilsTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  "mergeModeConfAndEnv" should "merge clusterConf common and mode-specific configurations" in {
    val commonClusterConf = Map("key1" -> "value1", "key2" -> "value2").asJava
    val modeClusterConf = Map("key2" -> "overridden", "key3" -> "value3").asJava
    val modeClusterConfigs = Map("test-mode" -> modeClusterConf).asJava

    val clusterConf = new ClusterConfigProperties()
    clusterConf.setCommon(commonClusterConf)
    clusterConf.setModeClusterConfigs(modeClusterConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(clusterConf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.clusterConf.common.asScala should contain("key2" -> "overridden")
    result.executionInfo.clusterConf.common.asScala should contain("key3" -> "value3")
    result.executionInfo.clusterConf.isSetModeClusterConfigs shouldBe false
  }

  it should "handle null clusterConf gracefully" in {
    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(null)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf shouldBe null
  }

  it should "handle clusterConf with null common configuration" in {
    val modeClusterConf = Map("key1" -> "value1").asJava
    val modeClusterConfigs = Map("test-mode" -> modeClusterConf).asJava

    val clusterConf = new ClusterConfigProperties()
    clusterConf.setCommon(null)
    clusterConf.setModeClusterConfigs(modeClusterConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(clusterConf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.clusterConf.isSetModeClusterConfigs shouldBe false
  }

  it should "handle clusterConf with null modeClusterConfigs" in {
    val commonClusterConf = Map("key1" -> "value1").asJava

    val clusterConf = new ClusterConfigProperties()
    clusterConf.setCommon(commonClusterConf)
    clusterConf.setModeClusterConfigs(null)

    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(clusterConf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.clusterConf.isSetModeClusterConfigs shouldBe false
  }

  it should "handle missing mode in modeClusterConfigs" in {
    val commonClusterConf = Map("key1" -> "value1").asJava
    val modeClusterConf = Map("key2" -> "value2").asJava
    val modeClusterConfigs = Map("other-mode" -> modeClusterConf).asJava

    val clusterConf = new ClusterConfigProperties()
    clusterConf.setCommon(commonClusterConf)
    clusterConf.setModeClusterConfigs(modeClusterConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(clusterConf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.clusterConf.common.asScala should not contain ("key2" -> "value2")
    result.executionInfo.clusterConf.isSetModeClusterConfigs shouldBe false
  }

  it should "handle empty clusterConf configurations" in {
    val clusterConf = new ClusterConfigProperties()
    clusterConf.setCommon(new util.HashMap[String, String]())
    clusterConf.setModeClusterConfigs(new util.HashMap[String, util.Map[String, String]]())

    val executionInfo = new ExecutionInfo()
    executionInfo.setClusterConf(clusterConf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.clusterConf.common shouldBe empty
    result.executionInfo.clusterConf.isSetModeClusterConfigs shouldBe false
  }

  it should "merge conf common and mode-specific configurations" in {
    val commonConf = Map("key1" -> "value1", "key2" -> "value2").asJava
    val modeConf = Map("key2" -> "overridden", "key3" -> "value3").asJava
    val modeConfigs = Map("test-mode" -> modeConf).asJava

    val conf = new ConfigProperties()
    conf.setCommon(commonConf)
    conf.setModeConfigs(modeConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(conf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.conf.common.asScala should contain("key2" -> "overridden")
    result.executionInfo.conf.common.asScala should contain("key3" -> "value3")
    result.executionInfo.conf.isSetModeConfigs shouldBe false
  }

  it should "handle null conf gracefully" in {
    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(null)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf shouldBe null
  }

  it should "handle conf with null common configuration" in {
    val modeConf = Map("key1" -> "value1").asJava
    val modeConfigs = Map("test-mode" -> modeConf).asJava

    val conf = new ConfigProperties()
    conf.setCommon(null)
    conf.setModeConfigs(modeConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(conf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.conf.isSetModeConfigs shouldBe false
  }

  it should "handle conf with null modeConfigs" in {
    val commonConf = Map("key1" -> "value1").asJava

    val conf = new ConfigProperties()
    conf.setCommon(commonConf)
    conf.setModeConfigs(null)

    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(conf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.conf.isSetModeConfigs shouldBe false
  }

  it should "handle missing mode in modeConfigs" in {
    val commonConf = Map("key1" -> "value1").asJava
    val modeConf = Map("key2" -> "value2").asJava
    val modeConfigs = Map("other-mode" -> modeConf).asJava

    val conf = new ConfigProperties()
    conf.setCommon(commonConf)
    conf.setModeConfigs(modeConfigs)

    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(conf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf.common.asScala should contain("key1" -> "value1")
    result.executionInfo.conf.common.asScala should not contain ("key2" -> "value2")
    result.executionInfo.conf.isSetModeConfigs shouldBe false
  }

  it should "handle empty conf configurations" in {
    val conf = new ConfigProperties()
    conf.setCommon(new util.HashMap[String, String]())
    conf.setModeConfigs(new util.HashMap[String, util.Map[String, String]]())

    val executionInfo = new ExecutionInfo()
    executionInfo.setConf(conf)

    val baseMetaData = Builders.MetaData(namespace = "test", name = "test_node")
    baseMetaData.setExecutionInfo(executionInfo)

    val result = MetaDataUtils.layer(
      baseMetaData,
      "test-mode",
      "test-node",
      Seq.empty[TableDependency]
    )

    result.executionInfo.conf.common shouldBe empty
    result.executionInfo.conf.isSetModeConfigs shouldBe false
  }
}
