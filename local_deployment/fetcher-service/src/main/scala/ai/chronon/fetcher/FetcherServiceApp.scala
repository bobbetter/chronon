package ai.chronon.fetcher

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import ai.chronon.fetcher.routes.Routes
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object FetcherServiceApp extends App {

  implicit val system: ActorSystem = ActorSystem("fetcher-service-system")
  implicit val ec: ExecutionContext = system.dispatcher

  private val config = ConfigFactory.load()

  private val host: String = if (config.hasPath("fetcher-service.http.host")) {
    config.getString("fetcher-service.http.host")
  } else {
    "0.0.0.0"
  }

  private val port: Int = if (config.hasPath("fetcher-service.http.port")) {
    config.getInt("fetcher-service.http.port")
  } else {
    8080
  }

  private val routes = new Routes()

  private val bindingFuture: Future[Http.ServerBinding] =
    Http().newServerAt(host, port).bind(routes.allRoutes)

  bindingFuture.onComplete {
    case Success(binding) =>
      val address = binding.localAddress
      system.log.info(s"Fetcher Service online at http://${address.getHostString}:${address.getPort}/")
    case Failure(exception) =>
      system.log.error("Failed to bind HTTP endpoint, terminating system", exception)
      system.terminate()
  }

  sys.addShutdownHook {
    bindingFuture
      .flatMap(_.unbind())
      .onComplete { _ =>
        system.log.info("Fetcher Service stopping, actor system terminating")
        system.terminate()
      }
  }

  Await.result(system.whenTerminated, Duration.Inf)
}


