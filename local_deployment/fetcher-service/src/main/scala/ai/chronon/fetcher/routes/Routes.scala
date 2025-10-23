package ai.chronon.fetcher.routes

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import sttp.tapir.server.akkahttp.AkkaHttpServerInterpreter
import ai.chronon.fetcher.endpoints.HelloEndpoint
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.apispec.openapi.circe.yaml._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class Routes(implicit ec: ExecutionContext) {
  
  // List of all endpoints
  private val endpoints = List(HelloEndpoint.helloEndpoint)
  
  // Convert Tapir endpoints to Akka HTTP routes
  private val helloRoute: Route = 
    AkkaHttpServerInterpreter().toRoute(
      HelloEndpoint.helloEndpoint.serverLogicSuccess(_ => Future.successful(
        HelloEndpoint.helloLogic(()).getOrElse(
          HelloEndpoint.HelloResponse("Error", System.currentTimeMillis())
        )
      ))
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
  
  // Try to use Swagger UI, fallback gracefully if it fails
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
    helloRoute,
    openApiRoute,
    swaggerRoute
  )
}

