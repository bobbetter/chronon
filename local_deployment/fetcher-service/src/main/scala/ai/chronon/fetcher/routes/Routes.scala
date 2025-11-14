package ai.chronon.fetcher.routes

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import sttp.tapir.server.akkahttp.AkkaHttpServerInterpreter
import ai.chronon.fetcher.endpoints.{GroupByEndpoint, JoinEndpoint, MetadataEndpoint, MetadataUploadEndpoint, DynamoDBAdminEndpoint}
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.apispec.openapi.circe.yaml._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import ch.megard.akka.http.cors.scaladsl.model.HttpOriginMatcher

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Routes(implicit ec: ExecutionContext) {
  
  // List of all endpoints
  private val endpoints = List(
    GroupByEndpoint.groupByEndpoint, 
    JoinEndpoint.joinEndpoint,
    MetadataEndpoint.groupBy,
    MetadataEndpoint.join,
    MetadataUploadEndpoint.join,
    DynamoDBAdminEndpoint.createTableEndpoint
  )
  

  private val groupByRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      GroupByEndpoint.groupByEndpoint.serverLogic(request => GroupByEndpoint.groupByLogic(request))
    )

  private val joinRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      JoinEndpoint.joinEndpoint.serverLogic(request => JoinEndpoint.joinLogic(request))
    )

  private val groupByMetaDataRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      MetadataEndpoint.groupBy.serverLogic(groupByName => MetadataEndpoint.getGroupByServingInfoLogic(groupByName))
    )

  private val joinMetadataRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      MetadataEndpoint.join.serverLogic(joinName => MetadataEndpoint.getJoinConfLogic(joinName))
    )

  private val metadataUploadRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      MetadataUploadEndpoint.join.serverLogic(request => MetadataUploadEndpoint.uploadJoinConfLogic(request))
    )

  private val dynamoDBCreateTableRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      DynamoDBAdminEndpoint.createTableEndpoint.serverLogic(request => DynamoDBAdminEndpoint.createTableLogic(request))
    )
  
  // Generate OpenAPI documentation
  private val openApiDocs = {
    val docs = OpenAPIDocsInterpreter().toOpenAPI(endpoints, "Chronon Fetcher Service", "1.0.0")
    docs.copy(info = docs.info.description("REST API for Chronon Fetcher Service"))
  }
  
  // OpenAPI YAML endpoint
  private val openApiRoute: Route = path("api" / "openapi.yaml") {
    get {
      complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, openApiDocs.toYaml))
    }
  }
  
  // Try to serve Swagger UI assets, fallback if not present
  private val swaggerRoute: Route = Try {
    val swaggerEndpoints = SwaggerInterpreter()
      .fromEndpoints[Future](endpoints, "Chronon Fetcher Service", "1.0.0")
    AkkaHttpServerInterpreter().toRoute(swaggerEndpoints)
  } match {
    case Success(route) => route
    case Failure(_) =>
      path("docs") {
        get {
          complete(
            HttpEntity(
              ContentTypes.`text/plain(UTF-8)`,
              "Swagger UI is unavailable. Access the OpenAPI spec at /api/openapi.yaml."
            )
          )
        }
      }
  }
  
  private val corsSettings: CorsSettings = CorsSettings.defaultSettings
    .withAllowGenericHttpRequests(true)
    .withAllowCredentials(false)
    .withAllowedOrigins(HttpOriginMatcher.*)

  // Combine all routes with CORS support
  val allRoutes: Route = handleRejections(corsRejectionHandler) {
    cors(corsSettings) {
      concat(
        groupByRoute,
        joinRoute,
        groupByMetaDataRoute,
        joinMetadataRoute,
        metadataUploadRoute,
        dynamoDBCreateTableRoute,
        openApiRoute,
        swaggerRoute
      )
    }
  }
}

