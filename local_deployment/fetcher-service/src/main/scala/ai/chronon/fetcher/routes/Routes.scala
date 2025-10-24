package ai.chronon.fetcher.routes

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import sttp.tapir.server.akkahttp.AkkaHttpServerInterpreter
import ai.chronon.fetcher.endpoints.{GroupByEndpoint, JoinEndpoint}
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.apispec.openapi.circe.yaml._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Routes(implicit ec: ExecutionContext) {
  
  // List of all endpoints
  private val endpoints = List(GroupByEndpoint.groupByEndpoint, JoinEndpoint.joinEndpoint)
  

  private val groupByRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      GroupByEndpoint.groupByEndpoint.serverLogic(request => GroupByEndpoint.groupByLogic(request))
    )

  private val joinRoute: Route =
    AkkaHttpServerInterpreter().toRoute(
      JoinEndpoint.joinEndpoint.serverLogic(request => JoinEndpoint.joinLogic(request))
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
  
  // Combine all routes
  val allRoutes: Route = concat(
    groupByRoute,
    joinRoute,
    openApiRoute,
    swaggerRoute
  )
}

