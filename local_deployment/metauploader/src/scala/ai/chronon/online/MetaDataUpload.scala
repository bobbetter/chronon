package ai.chronon.online

import ai.chronon.api.Constants
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}

import scala.concurrent.Await
import scala.concurrent.duration._

object MetaDataUpload {

  final case class Args(confPath: String, confType: Option[String])

  private def parseArgs(argv: Array[String]): Args = {
    var path: String = null
    var confType: Option[String] = None

    argv.foreach { arg =>
      if (arg.startsWith("--conf-path=")) path = arg.substring("--conf-path=".length)
      else if (arg.startsWith("--conf-type=")) confType = Some(arg.substring("--conf-type=".length))
      else if (!arg.startsWith("--") && path == null) path = arg
    }

    // Defaults when not provided
    if (path == null || path.isEmpty) path = "../app/compiled/joins/quickstart/"
    if (confType.isEmpty) confType = Some("joins")

    Args(path, confType)
  }

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)

    println("Chronon MetaDataUpload - Writing to KV Store (DynamoDB)")
    println("=" * 80)
    val endpoint = sys.env.getOrElse("DYNAMO_ENDPOINT", "http://localhost:8000")
    val region = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")
    println(s"DynamoDB Endpoint: $endpoint")
    println(s"AWS Region: $region")
    println()

    // Build AWS Api and helpers
    val api = new ai.chronon.integrations.aws.AwsApiImpl(Map.empty[String, String])
    val fetchContext = FetchContext(api.genKvStore, Constants.MetadataDataset)
    val metadataStore = new MetadataStore(fetchContext)

    val acceptedEndPoints = List(MetadataEndPoint.ConfByKeyEndPointName)

    // Walker extracts KV pairs (endpoint -> (key -> List(value)))
    val walker = new MetadataDirWalker(args.confPath, acceptedEndPoints, maybeConfType = args.confType)
    val kvMap = walker.run

    // Ensure tables exist
    acceptedEndPoints.foreach(e => metadataStore.create(e))

    // Upload
    val putResults = kvMap.toSeq.flatMap { case (endPoint, keyValues) =>
      Await.result(metadataStore.put(keyValues, endPoint), 30.minutes)
    }

    val success = putResults.count(identity)
    val failure = putResults.length - success
    println(s"Uploaded Chronon Configs to the KV store, success count = $success, failure count = $failure")
  }
}


