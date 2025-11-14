package ai.chronon.fetcher.endpoints

<<<<<<< HEAD
import ai.chronon.fetcher.services.ChrononFetcherClient
=======
import ai.chronon.fetcher.services.{ChrononFetcherClient, MetadataNotFoundException}
>>>>>>> chrono_force_snapshot
import io.circe.parser._
import io.circe.Json
import io.circe.generic.auto._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
<<<<<<< HEAD
=======
import sttp.model.StatusCode
>>>>>>> chrono_force_snapshot

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object MetadataEndpoint {

  final case class ErrorResponse(error: String)

  final case class GroupByServingInfoResponse(
    groupByName: String,
    servingInfo: Json,
    message: String
  )

<<<<<<< HEAD
  val metadataEndpoint: PublicEndpoint[String, ErrorResponse, GroupByServingInfoResponse, Any] =
=======
  final case class JoinConfResponse(
    joinName: String,
    joinConf: Json,
    message: String
  )

  val groupBy: PublicEndpoint[String, ErrorResponse, GroupByServingInfoResponse, Any] =
>>>>>>> chrono_force_snapshot
    endpoint.get
      .in("api" / "v1" / "metadata" / "groupby" / path[String]("groupByName"))
      .out(jsonBody[GroupByServingInfoResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Retrieves GroupBy serving info metadata from the KV store")
      .summary("Get GroupBy Serving Info")

<<<<<<< HEAD
  def metadataLogic(groupByName: String)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, GroupByServingInfoResponse]] = {
=======
  val join: PublicEndpoint[String, (StatusCode, ErrorResponse), JoinConfResponse, Any] =
    endpoint.get
      .in("api" / "v1" / "metadata" / "join" / path[String]("joinName"))
      .out(jsonBody[JoinConfResponse])
      .errorOut(statusCode.and(jsonBody[ErrorResponse]))
      .description("Retrieves Join configuration metadata from the KV store")
      .summary("Get Join Configuration")

  def getGroupByServingInfoLogic(groupByName: String)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, GroupByServingInfoResponse]] = {
>>>>>>> chrono_force_snapshot
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
<<<<<<< HEAD
=======

  def getJoinConfLogic(joinName: String)(implicit ec: ExecutionContext): Future[Either[(StatusCode, ErrorResponse), JoinConfResponse]] = {
    Future {
      ChrononFetcherClient.getJoinConf(joinName) match {
        case Success(jsonString) =>
          parse(jsonString) match {
            case Right(json) =>
              Right(
                JoinConfResponse(
                  joinName = joinName,
                  joinConf = json,
                  message = s"Successfully retrieved configuration for Join: $joinName"
                )
              )
            case Left(parseError) =>
              Left(
                (
                  StatusCode.InternalServerError, 
                  ErrorResponse(s"Failed to parse join conf JSON: ${parseError.getMessage}")
                )
              )
          }
        case Failure(exception: MetadataNotFoundException) =>
          // Handle "not found" errors with 
          Left((StatusCode.NotFound, ErrorResponse(exception.getMessage)))
        case Failure(exception) =>
          // All other errors return 500
          Left((StatusCode.InternalServerError, ErrorResponse(exception.getMessage)))
      }
    }
  }
>>>>>>> chrono_force_snapshot
}

