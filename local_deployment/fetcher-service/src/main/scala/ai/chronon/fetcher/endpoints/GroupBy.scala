package ai.chronon.fetcher.endpoints

import ai.chronon.fetcher.services.ChrononFetcherClient
import ai.chronon.fetcher.utils.JsonConversions
import ai.chronon.online.fetcher.Fetcher
import io.circe.Json
import io.circe.generic.auto._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object GroupByEndpoint {

  final case class GroupByRequest(
    name: String,
    keys: Map[String, Json],
    atMillis: Option[Long] = None
  )

  final case class GroupByResponse(
    name: String,
    keys: Map[String, Json],
    atMillis: Option[Long],
    values: Option[Map[String, Json]],
    message: String
  )

  final case class ErrorResponse(error: String)

  val groupByEndpoint: PublicEndpoint[GroupByRequest, ErrorResponse, GroupByResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "groupby")
      .in(jsonBody[GroupByRequest])
      .out(jsonBody[GroupByResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Fetches DynamoDB GroupBy results via Chronon online fetcher")
      .summary("Fetch GroupBy features")

  def groupByLogic(request: GroupByRequest)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, GroupByResponse]] = {
    val fetcherRequest = Fetcher.Request(
      name = request.name,
      keys = request.keys.map { case (key, value) => key -> JsonConversions.jsonToAnyRef(value) },
      atMillis = request.atMillis
    )

    ChrononFetcherClient
      .fetchGroupBys(Seq(fetcherRequest))
      .map { responses =>
        responses.headOption match {
          case Some(response) =>
            handleResponse(response)
          case None =>
            Left(ErrorResponse("Fetcher returned an empty response"))
        }
      }
      .recover {
        case ex: Throwable =>
          Left(ErrorResponse(s"Failed to fetch GroupBy data: ${ex.getMessage}"))
      }
  }

  private def handleResponse(response: Fetcher.Response): Either[ErrorResponse, GroupByResponse] = {
    val requestKeys = response.request.keys.map { case (k, v) => k -> JsonConversions.anyToJson(v) }

    response.values match {
      case Success(valueMap) =>
        val valuesOpt = Option(valueMap).map(_.toMap).filter(_.nonEmpty).map { map =>
          map.map { case (feature, value) => feature -> JsonConversions.anyToJson(value) }
        }

        val message = valuesOpt match {
          case Some(values) => s"Retrieved ${values.size} features"
          case None => "No data found"
        }

        Right(
          GroupByResponse(
            name = response.request.name,
            keys = requestKeys,
            atMillis = response.request.atMillis,
            values = valuesOpt,
            message = message
          )
        )

      case Failure(exception) =>
        Left(ErrorResponse(s"Fetcher error: ${exception.getMessage}"))
    }
  }

}

