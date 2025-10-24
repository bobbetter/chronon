package ai.chronon.fetcher.services

import ai.chronon.integrations.aws.AwsApiImpl
import ai.chronon.online.fetcher.Fetcher

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final case class FetcherClient(fetcher: Fetcher, close: () => Unit)

object ChrononFetcherClient {

  private lazy val client: FetcherClient = build()

  def fetchGroupBys(requests: Seq[Fetcher.Request])(implicit ec: ExecutionContext): Future[Seq[Fetcher.Response]] =
    client.fetcher.fetchGroupBys(requests)

  def fetchJoin(requests: Seq[Fetcher.Request])(implicit ec: ExecutionContext): Future[Seq[Fetcher.Response]] =
    client.fetcher.fetchJoin(requests)

  private def build(): FetcherClient = {
    val awsApi = new AwsApiImpl(Map.empty[String, String])
    val fetcher = awsApi.buildFetcher(debug = true)
    @volatile var closed = false

    val closeFn = () => this.synchronized {
      if (!closed) {
        closed = true
        Try(awsApi.ddbClient.close()).getOrElse(())
      }
    }

    sys.addShutdownHook(closeFn)

    FetcherClient(fetcher, closeFn)
  }

  def close(): Unit = client.close()
}


