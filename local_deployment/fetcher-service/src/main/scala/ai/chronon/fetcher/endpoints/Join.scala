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

object JoinEndpoint {

  final case class JoinRequest(
    name: String,
    keys: Map[String, Json],
    atMillis: Option[Long] = None
  )

  final case class JoinResponse(
    name: String,
    keys: Map[String, Json],
    atMillis: Option[Long],
    values: Option[Map[String, Json]],
    message: String
  )

  final case class ErrorResponse(error: String)

  val joinEndpoint: PublicEndpoint[JoinRequest, ErrorResponse, JoinResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "join")
      .in(jsonBody[JoinRequest])
      .out(jsonBody[JoinResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Fetches Join results via Chronon online fetcher")
      .summary("Fetch Join features")

  def joinLogic(request: JoinRequest)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, JoinResponse]] = {
    val fetcherRequest = Fetcher.Request(
      name = request.name,
      keys = request.keys.map { case (key, value) => key -> JsonConversions.jsonToAnyRef(value) },
      atMillis = request.atMillis
    )

    ChrononFetcherClient
      .fetchJoin(Seq(fetcherRequest))
      .map { responses =>
        responses.headOption match {
          case Some(response) => handleResponse(response)
          case None => Left(ErrorResponse("Fetcher returned an empty response"))
        }
      }
      .recover {
        case ex: Throwable =>
          Left(ErrorResponse(s"Failed to fetch Join data: ${ex.getMessage}"))
      }
  }

  private def handleResponse(response: Fetcher.Response): Either[ErrorResponse, JoinResponse] = {
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
          JoinResponse(
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
