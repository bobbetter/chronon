package ai.chronon.fetcher.endpoints

import ai.chronon.fetcher.services.ChrononFetcherClient
import ai.chronon.fetcher.services.ChrononFetcherClient.{TableCreated, TableAlreadyExists}
import io.circe.generic.auto._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.model.StatusCode

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DynamoDBAdminEndpoint {

  final case class CreateTableRequest(
    tableName: String,
    isTimeSorted: Boolean = true
  )

  final case class CreateTableResponse(
    tableName: String,
    status: String,
    message: String
  )

  final case class ErrorResponse(error: String)

  val createTableEndpoint: PublicEndpoint[CreateTableRequest, ErrorResponse, CreateTableResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "admin" / "dynamodb" / "create-table")
      .in(jsonBody[CreateTableRequest])
      .out(jsonBody[CreateTableResponse])
      .errorOut(jsonBody[ErrorResponse])
      .description("Creates a DynamoDB table in the configured KV store")
      .summary("Create DynamoDB Table")

  def createTableLogic(request: CreateTableRequest)(implicit ec: ExecutionContext): Future[Either[ErrorResponse, CreateTableResponse]] = {
    Future {
      ChrononFetcherClient.createTable(request.tableName, request.isTimeSorted) match {
        case Success(TableCreated) =>
          Right(
            CreateTableResponse(
              tableName = request.tableName,
              status = "success",
              message = s"DynamoDB table '${request.tableName}' created successfully (is-time-sorted=${request.isTimeSorted})"
            )
          )
        case Success(TableAlreadyExists) =>
          Left(
            (
              ErrorResponse(s"DynamoDB table '${request.tableName}' already exists")
            )
          )
        case Failure(ex) =>
          Left(
            (
              ErrorResponse(s"Failed to create table: ${ex.getMessage}")
            )
          )
      }
    }
  }
}

