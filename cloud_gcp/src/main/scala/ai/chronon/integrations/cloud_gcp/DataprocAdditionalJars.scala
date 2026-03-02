package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.submission.JobSubmitterConstants

// Tracks additional Jars that need to be specified while submitting Dataproc Flink jobs as we build
// thin jars with mill and these additional deps need to be augmented during job submission
object DataprocAdditionalJars {

  private val defaultFlinkJarsBasePath = "gs://zipline-spark-libs/spark-3.5.3/libs/"

  // Need a lot of the Spark jars for Flink as Flink leverages Spark's catalyst / sql components for Spark expr eval.
  // This list can be trimmed down, but a lot of these jars are needed for a class or two that if absent, results in a CNF exception.
  // The base path can be configured via FLINK_JARS_URI environment variable, which should point to a directory containing these jars.
  def additionalFlinkJobJars(flinkJarsBasePath: Option[String] = None): Array[String] = {
    JobSubmitterConstants.additionalFlinkJars(flinkJarsBasePath.getOrElse(defaultFlinkJarsBasePath))
  }
}
