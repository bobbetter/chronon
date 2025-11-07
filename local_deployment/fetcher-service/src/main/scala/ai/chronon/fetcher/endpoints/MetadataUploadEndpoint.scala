package ai.chronon.fetcher.endpoints

import ai.chronon.api.Constants
import ai.chronon.fetcher.services.ChrononFetcherClient
import ai.chronon.online.{MetadataDirWalker, MetadataEndPoint}
import io.circe.generic.auto._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object MetadataUploadEndpoint {

  final case class ErrorResponse(error: String)

  final case class MetadataUploadRequest(
    teamName: String,
  )

  final case class MetadataUploadResponse(
    message: String,
    successCount: Int,
    failureCount: Int,
    details: Map[String, Int]
  )

  val defaultPath = sys.env.getOrElse("CHRONON_CONFIGS_PATH", "/opt/chronon/configs")
  val metadataTableName = MetadataEndPoint.ConfByKeyEndPointName
  val confType: String = "joins" // Currently only joins metadata is supported

  val join: PublicEndpoint[MetadataUploadRequest, ErrorResponse, MetadataUploadResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "metadata" / "upload")
      .in(jsonBody[MetadataUploadRequest])
      .out(jsonBody[MetadataUploadResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Upload Chronon metadata configurations from the local filesystem to KV store")
      .summary("Upload Metadata Configs")

  def uploadJoinConfLogic(request: MetadataUploadRequest)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, MetadataUploadResponse]] = {
    Future {
      Try {        
        val confPath = s"$defaultPath/${confType}/${request.teamName}"
        // Ensure the metadata table exists
        ChrononFetcherClient.createDataset(metadataTableName)

        // Walk the directory to collect metadata
        val walker = new MetadataDirWalker(confPath, List(metadataTableName), maybeConfType = Some(confType))
        val kvMap: Map[String, Map[String, List[String]]] = walker.run

        // Upload metadata to KV store
        val results = kvMap.toSeq.flatMap { case (endPoint, keyValues) =>
          val future = ChrononFetcherClient.putMetadata(keyValues, endPoint)
          Await.result(future, 5.seconds)
        }

        val successCount = results.count(identity)
        val failureCount = results.length - successCount
        val details = kvMap.map { case (endpoint, kvs) => endpoint -> kvs.size }

        MetadataUploadResponse(
          message = s"Successfully uploaded metadata from $confPath",
          successCount = successCount,
          failureCount = failureCount,
          details = details
        )
      } match {
        case Success(response) => Right(response)
        case Failure(exception) =>
          Left(ErrorResponse(s"Failed to upload metadata: ${exception.getMessage}"))
      }
    }
  }
}

