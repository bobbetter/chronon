package ai.chronon.fetcher

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import ai.chronon.fetcher.routes.Routes
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object FetcherServiceApp {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    // Create the actor system
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "fetcher-service")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    
    // Configuration
    val host = sys.env.getOrElse("HOST", "0.0.0.0")
    val port = sys.env.getOrElse("PORT", "8080").toInt
    
    // Create routes
    val routes = new Routes()
    val allRoutes: Route = routes.allRoutes
    
    // Start the HTTP server
    val bindingFuture: Future[Http.ServerBinding] = 
      Http()
        .newServerAt(host, port)
        .bind(allRoutes)
    
    bindingFuture.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        val displayHost = resolveDisplayHost(host, address)
        val baseUrl = s"http://$displayHost:${address.getPort}"
        logger.info(s"Chronon Fetcher Service started at ${baseUrl}/")
        logger.info(s"Swagger UI available at ${baseUrl}/docs")
        logger.info(s"Hello endpoint available at ${baseUrl}/api/v1/hello")
        
      case Failure(ex) =>
        logger.error(s"Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
    
    // Create a promise that will be completed on shutdown
    val shutdownPromise = Promise[Unit]()
    
    // Add shutdown hook for graceful shutdown
    sys.addShutdownHook {
      logger.info("Shutting down Chronon Fetcher Service...")
      
      // Perform graceful shutdown with timeout
      val gracefulShutdown = for {
        binding <- bindingFuture
        _ <- binding.unbind()
        _ = logger.info("HTTP server unbound successfully")
      } yield {
        system.terminate()
      }
      
      try {
        Await.result(gracefulShutdown, 10.seconds)
        logger.info("Chronon Fetcher Service shut down successfully")
      } catch {
        case ex: Exception =>
          logger.warn(s"Error during graceful shutdown: ${ex.getMessage}")
      } finally {
        shutdownPromise.success(())
      }
    }

    // Keep the application running until shutdown
    try {
      Await.result(shutdownPromise.future, Duration.Inf)
    } catch {
      case _: InterruptedException =>
        logger.info("Main thread interrupted, shutting down...")
    }
  }

  private def resolveDisplayHost(configuredHost: String, address: InetSocketAddress): String = {
    def isUnspecified(host: String): Boolean = host match {
      case "0.0.0.0" | "::" | "0:0:0:0:0:0:0:0" | "[::]" => true
      case _ => false
    }

    val candidates = List(
      Option(configuredHost),
      Option(address.getAddress).map(_.getHostAddress),
      Option(address.getHostString)
    ).flatten

    candidates.find(host => host.nonEmpty && !isUnspecified(host)).getOrElse("localhost")
  }
}

