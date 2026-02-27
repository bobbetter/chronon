package ai.chronon.integrations.aws

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{FlinkJob, SparkJob}
import io.fabric8.kubernetes.client.Config
import org.junit.Assert.assertEquals
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse}

class EmrSubmitterTest extends AnyFlatSpec with MockitoSugar {
  "EmrSubmitterClient" should "return job id when a job is submitted and assert EMR request args" in {
    val stepId = "mock-step-id"
    val clusterId = "j-MOCKCLUSTERID123"

    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]
    val mockEksSubmitter = mock[EksFlinkSubmitter]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[AddJobFlowStepsRequest])

    when(
      mockEmrClient.addJobFlowSteps(
        requestCaptor.capture()
      )).thenReturn(AddJobFlowStepsResponse.builder().stepIds(stepId).build())

    val expectedCustomerId = "canary"
    val expectedApplicationArgs = Seq("group-by-backfill", "arg1", "arg2")
    val expectedFiles = List("s3://random-conf", "s3://random-data")
    val expectedMainClass = "some-main-class"
    val expectedJarURI = "s3://-random-jar-uri"

    val expectedJobProperties = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "2")

    val submitter = new EmrSubmitter(expectedCustomerId, mockEmrClient, mockEc2Client, Some(mockEksSubmitter))
    val submittedStepId = submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> expectedMainClass,
        JarURI -> expectedJarURI,
        ClusterId -> clusterId
      ),
      jobProperties = expectedJobProperties,
      files = expectedFiles,
      labels = Map.empty,
      expectedApplicationArgs: _*
    )
    assertEquals(submittedStepId, stepId)

    val actualRequest = requestCaptor.getValue

    // Verify the cluster ID is correct
    assertEquals(actualRequest.jobFlowId(), clusterId)

    // Verify step configuration
    assertEquals(actualRequest.steps().size(), 1)

    val stepConfig = actualRequest.steps().get(0)
    assertEquals(stepConfig.actionOnFailure().name(), "CONTINUE")
    assertEquals(stepConfig.name(), "Run Zipline Job")
    assertEquals(stepConfig.hadoopJarStep().jar(), "command-runner.jar")

    val actualArgs = stepConfig.hadoopJarStep().args().toScala.mkString(" ")
    // Verify file copy commands are present
    assert(actualArgs.contains("aws s3 cp s3://random-conf /mnt/zipline/"))
    assert(actualArgs.contains("aws s3 cp s3://random-data /mnt/zipline/"))
    // Verify spark-submit with --conf args for job properties
    assert(actualArgs.contains("--conf 'spark.executor.memory=4g'"))
    assert(actualArgs.contains("--conf 'spark.executor.cores=2'"))
    // Verify spark-submit class, jar, and application args
    assert(actualArgs.contains(s"--class $expectedMainClass"))
    assert(actualArgs.contains(expectedJarURI))
    assert(actualArgs.contains(expectedApplicationArgs.mkString(" ")))
  }

  it should "test createSubmissionPropsMap for Spark job on EMR on EC2" in {
    val clusterId = "j-SPARKCLUSTER123"
    val mainClass = "ai.chronon.spark.Driver"
    val jarURI = "s3://zipline-jars/cloud-aws.jar"

    val actual = EmrSubmitter.createSubmissionPropsMap(
      jobType = SparkJob,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass"
      ),
      clusterId = clusterId
    )

    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(ClusterId), clusterId)
    assertEquals(actual(ClusterInstanceType), "m5.xlarge") // default value
    assert(actual.contains(ClusterInstanceCount))
    assert(actual.contains(ClusterIdleTimeout))
  }

  it should "test createSubmissionPropsMap for Flink job" in {
    val jobId = "test-job-id-123"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "s3://zipline-jars/cloud-aws.jar"
    val flinkMainJarURI = "s3://zipline-jars/flink-assembly.jar"
    val flinkCheckpointUri = "s3://zipline-warehouse/flink-checkpoints"
    val kinesisConnectorJarURI = "s3://zipline-jars/kinesis-connector.jar"
    val flinkJarsUri = "s3://zipline-artifacts/flink-jars/"
    val customSavepoint = "s3://zipline-warehouse/flink-checkpoints/chk-100"
    val eksServiceAccount = "zipline-flink-sa"
    val eksNamespace = "zipline-flink"

    val actual = EmrSubmitter.createSubmissionPropsMap(
      jobType = FlinkJob,
      args = Array(
        s"$JobIdArgKeyword=$jobId",
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$FlinkKinesisJarUriArgKeyword=$kinesisConnectorJarURI",
        s"$FlinkJarsUriArgKeyword=$flinkJarsUri",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingCustomSavepointArgKeyword=$customSavepoint",
        s"$EksServiceAccountArgKeyword=$eksServiceAccount",
        s"$EksNamespaceArgKeyword=$eksNamespace"
      ),
      clusterId = ""
    )

    assertEquals(actual(JobId), jobId)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkKinesisConnectorJarURI), kinesisConnectorJarURI)
    assertEquals(actual(FlinkJarsUri), flinkJarsUri)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)
    assertEquals(actual(SavepointUri), customSavepoint)
    assertEquals(actual(EksServiceAccount), eksServiceAccount)
    assertEquals(actual(EksNamespace), eksNamespace)
    assert(!actual.contains(ClusterId))
  }

  it should "test Kinesis Flink job end-to-end on EMR on EKS. Do NOT enable in CI / CD" ignore {
    // This test submits an actual Flink job to EMR on EKS to verify the FlinkDeployment submission works
    // To run:
    // CUSTOMER_ID=canary ./mill cloud_aws.test.testOnly "ai.chronon.integrations.aws.EmrSubmitterTest" -- -z "test Kinesis Flink job"

    // Skip if CUSTOMER_ID is not set (CI/CD or local env without setup)
    assume(sys.env.contains("CUSTOMER_ID"), "CUSTOMER_ID environment variable must be set to run E2E tests")

    val region = "us-west-2"
    val streamName = "user-activities"
    val registryName = "zipline-canary"
    val schemaName = "user-activities"
    val jobId = java.util.UUID.randomUUID.toString.take(8)

    // JARs - update these paths to match your deployment
    val flinkMainJarUri = "s3://zipline-artifacts-canary/jars/flink_deploy.jar"
    val cloudAwsJarUri = "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar"
    val kinesisConnectorJarUri = "s3://zipline-artifacts-canary/jars/connectors_kinesis_deploy.jar"
    val checkpointPath = "s3://zipline-warehouse-canary/flink-checkpoints/kinesis-user-activities-test"

    // Explicitly target the canary EKS cluster rather than relying on the ambient kubeconfig context
    val k8sConfig = Config.autoConfigure("arn:aws:eks:us-west-2:345594603419:cluster/canary-eks")
    val eksFlinkSubmitter = new EksFlinkSubmitter(Some(k8sConfig))

    val deploymentName = eksFlinkSubmitter.submit(
      jobId = jobId,
      mainClass = "ai.chronon.flink_connectors.kinesis.KinesisUserActivitiesJob",
      mainJarUri = flinkMainJarUri,
      jarUris = Array(cloudAwsJarUri, kinesisConnectorJarUri),
      flinkCheckpointUri = checkpointPath,
      maybeSavepointUri = None,
      maybeFlinkJarsUri = None,
      jobProperties = Map.empty,
      args = Seq(
        s"--stream-name=$streamName",
        s"--registry-name=$registryName",
        s"--schema-name=$schemaName",
        s"--region=$region",
        s"--checkpoint-dir=$checkpointPath"
      ),
      serviceAccount = "zipline-flink-sa",
      namespace = "zipline-flink"
    )

    println(s"✅ Submitted Flink job")
    println(s"   Deployment Name: $deploymentName")
    println(s"   Monitor with: kubectl get flinkdeployment $deploymentName -n zipline-flink")
    println(s"   View logs: kubectl logs -n zipline-flink -l app=$deploymentName")

    assert(deploymentName.nonEmpty, "Deployment name should not be empty")
  }

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {
    val emrSubmitter = new EmrSubmitter(
      "canary",
      EmrClient.builder().build(),
      Ec2Client.builder().build(),
      Some(new EksFlinkSubmitter())
    )
    val jobId = emrSubmitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
        ClusterId -> "j-13BASWFP15TLR"
      ),
      jobProperties = Map.empty,
      files = List("s3://zipline-warehouse-canary/purchases.v1"),
      Map.empty,
      "group-by-backfill",
      "--conf-path",
      "/mnt/zipline/purchases.v1",
      "--end-date",
      "2025-02-26",
      "--conf-type",
      "group_bys",
    )
    println("EMR job id: " + jobId)
    0
  }

}
