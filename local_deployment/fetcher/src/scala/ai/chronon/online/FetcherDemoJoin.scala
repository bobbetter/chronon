package ai.chronon.online

import ai.chronon.integrations.aws.AwsApiImpl
import ai.chronon.online.fetcher.Fetcher

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Standalone script to fetch Join data using fetchJoin (no Spark required).
 *
 * Run examples:
 *   - Using defaults (join quickstart.training_set.v1__1, user_id=85):
 *       java -cp "chronon-aws-assembly.jar:chronon-online.jar" ai.chronon.online.FetcherDemoJoin
 *   - Explicit join and key:
 *       java -cp "..." ai.chronon.online.FetcherDemoJoin quickstart.training_set.v1__1 85
 */
object FetcherDemoJoin {

  def main(args: Array[String]): Unit = {
    println("Chronon Fetcher Demo (Join) - Reading from DynamoDB")
    println("=" * 80)

    val joinName = if (args.length > 0) args(0) else "quickstart.training_set.v1__1"
    val userId = if (args.length > 1) args(1) else "user_5"

    println(s"Fetching Join: $joinName")
    println(s"Key: user_id = $userId")
    println("=" * 80)

    val endpoint = sys.env.getOrElse("DYNAMO_ENDPOINT", "http://localhost:8000")
    val region = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")

    println(s"DynamoDB Endpoint: $endpoint")
    println(s"AWS Region: $region")
    println()

    val api = new AwsApiImpl(Map.empty[String, String])
    val fetcher = api.buildFetcher(debug = true)

    val request = Fetcher.Request(
      name = joinName,
      keys = Map[String, AnyRef]("user_id" -> userId),
      atMillis = Some(System.currentTimeMillis())
    )

    try {
      val responseFuture = fetcher.fetchJoin(Seq(request))
      val responses = Await.result(responseFuture, 30.seconds)

      responses.foreach { response =>
        println(s"\nResults for ${response.request.keys}:")

        response.values match {
          case Success(valueMap) =>
            if (valueMap.isEmpty) {
              println("  (no data found)")
            } else {
              println(s"  Retrieved ${valueMap.size} features:")
              valueMap.toSeq.sortBy(_._1).foreach { case (name, value) =>
                println(f"    $name%-40s = $value")
              }
            }

          case Failure(exception) =>
            println(s"  ERROR: ${exception.getMessage}")
            if (sys.env.get("DEBUG").contains("true")) {
              exception.printStackTrace()
            }
        }
      }

      println("\n" + "=" * 80)
      println("Join fetch completed successfully!")

    } catch {
      case e: Exception =>
        println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      try api.ddbClient.close() catch { case _: Throwable => () }
    }
    System.exit(0)
  }
}




