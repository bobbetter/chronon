package ai.chronon.fetcher.services

import ai.chronon.integrations.aws.AwsApiImpl
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.KVStore
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.integrations.aws.{DynamoDBKVStoreConstants, TableAlreadyExistsException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final case class FetcherClient(fetcher: Fetcher, awsApi: AwsApiImpl, close: () => Unit)

// Custom exception types for better error handling
sealed trait MetadataException extends Exception
case class MetadataNotFoundException(message: String) extends Exception(message) with MetadataException
case class MetadataFetchException(message: String, cause: Throwable) extends Exception(message, cause) with MetadataException

sealed trait CreateTableResult
case object TableCreated extends CreateTableResult
case object TableAlreadyExists extends CreateTableResult

object ChrononFetcherClient {
  private lazy val client: FetcherClient = build()

  def fetchGroupBys(requests: Seq[Fetcher.Request])(implicit ec: ExecutionContext): Future[Seq[Fetcher.Response]] =
    client.fetcher.fetchGroupBys(requests)

  def fetchJoin(requests: Seq[Fetcher.Request])(implicit ec: ExecutionContext): Future[Seq[Fetcher.Response]] =
    client.fetcher.fetchJoin(requests)

  def getGroupByServingInfo(groupByName: String): Try[String] =
    client.fetcher.metadataStore.getGroupByServingInfo(groupByName).map { servingInfoParsed =>
      ThriftJsonCodec.toJsonStr(servingInfoParsed.groupByServingInfo)
    }

  def getJoinConf(joinName: String): Try[String] = {
    // Wrap the call in Try because TTLCache throws exceptions instead of returning Failure
    Try(client.fetcher.metadataStore.getJoinConf(joinName)) match {
      case Success(tryJoinOps) =>
        // Now we have Try[JoinOps], flatten it and convert to JSON
        tryJoinOps.flatMap { joinOps =>
          Try(ThriftJsonCodec.toJsonStr(joinOps.join))
        }
      case Failure(exception) =>
        // Check if this is a "not found" error by examining the exception message
        val isNotFound = exception.getMessage.contains("Couldn't fetch") || 
                        exception.getMessage.contains("Empty response") ||
                        Option(exception.getCause).exists(_.getMessage.contains("Empty response"))
        
        if (isNotFound) {
          Failure(MetadataNotFoundException(s"Join '$joinName' not found. Please ensure metadata has been uploaded."))
        } else {
          Failure(MetadataFetchException(s"Failed to fetch configuration for Join '$joinName'", exception))
        }
    }
  }

  def putMetadata(kvPairs: Map[String, Seq[String]], datasetName: String)(implicit ec: ExecutionContext): Future[Seq[Boolean]] =
    client.fetcher.metadataStore.put(kvPairs, datasetName)

  def createDataset(datasetName: String): Unit =
    client.fetcher.metadataStore.create(datasetName)

  def createTable(tableName: String, isTimeSorted: Boolean = true): Try[CreateTableResult] = {
    Try {
      val kvStore = client.awsApi.genKvStore
      val props = Map(DynamoDBKVStoreConstants.isTimedSorted -> isTimeSorted.toString)
      
      try {
        kvStore.create(tableName, props)
        TableCreated
      } catch {
        case _: TableAlreadyExistsException =>
          TableAlreadyExists
      }
    }
  }

  def getKvStore: KVStore =
    client.awsApi.genKvStore

  def getAwsApi: AwsApiImpl =
    client.awsApi

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

    sys.addShutdownHook { closeFn() }

    FetcherClient(fetcher, awsApi, closeFn)
  }

  def close(): Unit = client.close()
}


