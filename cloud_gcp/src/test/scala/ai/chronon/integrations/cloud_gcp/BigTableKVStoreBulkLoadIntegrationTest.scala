package ai.chronon.integrations.cloud_gcp

import org.junit.Ignore

// Integration test to sanity check end to end bulkLoad behavior against a real BigTable instance.
// We have this turned off by default since it requires a real BigTable instance to run.
// To test - set the environment variables GCP_PROJECT_ID and GCP_INSTANCE_ID to your BigTable project and instance and run
class BigTableKVStoreBulkLoadIntegrationTest {

  @Ignore
  def testBigTableBulkPut(): Unit = {
    val srcOfflineTable = "data.test_gbu"
    val destinationTable = "quickstart.purchases.v1"
    val partitions = "2023-11-30"

    val kvStore = new GcpApiImpl(Map.empty).genKvStore
    kvStore.bulkPut(srcOfflineTable, destinationTable, partitions)
    println(s"Successful bulk put to BigTable - $destinationTable!")
  }
}
