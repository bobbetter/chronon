package ai.chronon.fetcher.endpoints

import ai.chronon.fetcher.services.ChrononFetcherClient
import io.circe.parser._
import io.circe.Json
import io.circe.generic.auto._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object MetadataEndpoint {

  final case class ErrorResponse(error: String)

  final case class GroupByServingInfoResponse(
    groupByName: String,
    servingInfo: Json,
    message: String
  )

  val metadataEndpoint: PublicEndpoint[String, ErrorResponse, GroupByServingInfoResponse, Any] =
    endpoint.get
      .in("api" / "v1" / "metadata" / "groupby" / path[String]("groupByName"))
      .out(jsonBody[GroupByServingInfoResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Retrieves GroupBy serving info metadata from the KV store")
      .summary("Get GroupBy Serving Info")

  def metadataLogic(groupByName: String)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, GroupByServingInfoResponse]] = {
    Future {
      ChrononFetcherClient.getGroupByServingInfo(groupByName) match {
        case Success(jsonString) =>
          parse(jsonString) match {
            case Right(json) =>
              Right(GroupByServingInfoResponse(
                groupByName = groupByName,
                servingInfo = json,
                message = s"Successfully retrieved serving info for GroupBy: $groupByName"
              ))
            case Left(parseError) =>
              Left(ErrorResponse(s"Failed to parse serving info JSON: ${parseError.getMessage}"))
          }
        case Failure(exception) =>
          Left(ErrorResponse(s"Failed to fetch serving info for GroupBy '$groupByName': ${exception.getMessage}"))
      }
    }
  }
}

