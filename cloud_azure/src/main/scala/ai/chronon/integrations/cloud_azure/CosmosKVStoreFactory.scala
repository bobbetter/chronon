package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_azure.CosmosKVStoreConstants._
import com.azure.cosmos.models.CosmosContainerIdentity
import com.azure.cosmos.{
  ConsistencyLevel,
  CosmosAsyncClient,
  CosmosClientBuilder,
  CosmosContainerProactiveInitConfigBuilder,
  DirectConnectionConfig
}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

object CosmosKVStoreFactory {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  // Singleton client cache (CosmosAsyncClient is expensive to create and thread-safe)
  private val clientCache = new AtomicReference[CosmosAsyncClient]()
  private val clientLock = new Object()

  def create(conf: Map[String, String]): CosmosKVStoreImpl = {
    val endpoint = getOrElseThrow(PropCosmosEndpoint, EnvCosmosEndpoint, conf)
    val key = getOrElseThrow(PropCosmosKey, EnvCosmosKey, conf)
    val databaseName = conf.getOrElse(
      PropCosmosDatabase,
      sys.env.getOrElse(EnvCosmosDatabase, DefaultDatabaseName)
    )

    val client = Option(clientCache.get()) match {
      case Some(existingClient) => existingClient
      case None =>
        clientLock.synchronized {
          Option(clientCache.get()) match {
            case Some(existingClient) => existingClient
            case None =>
              logger.info(s"Creating Cosmos client for endpoint: $endpoint")
              val newClient = createCosmosClient(endpoint, databaseName, key, conf)
              clientCache.set(newClient)
              newClient
          }
        }
    }

    val database = client.getDatabase(databaseName)
    new CosmosKVStoreImpl(database, conf)
  }

  private def createCosmosClient(
      endpoint: String,
      databaseName: String,
      key: String,
      conf: Map[String, String]
  ): CosmosAsyncClient = {

    val clientBuilder = new CosmosClientBuilder()
      .endpoint(endpoint)
      .key(key)
      .consistencyLevel(ConsistencyLevel.SESSION)

    // Use Gateway mode for emulator (required), Direct mode for production
    val isEmulator = conf.get(PropEmulatorMode).exists(_.toBoolean) || CosmosKVStoreConstants.isEmulator(endpoint)
    if (isEmulator) {
      logger.info("Detected emulator endpoint, using Gateway mode")
      clientBuilder.gatewayMode()
    } else {
      val directMode = new DirectConnectionConfig()
        .setMaxConnectionsPerEndpoint(130) // Per region endpoint
        .setConnectTimeout(Duration.ofSeconds(10))
      clientBuilder.directMode(directMode)

      val preferredRegions = parsePreferredRegions(conf)
      // Proactive container init requires preferredRegions to be set
      if (!preferredRegions.isEmpty) {
        clientBuilder.preferredRegions(preferredRegions)

        // Set up proactive connections for the shared containers
        val containerIdentityList = List(
          new CosmosContainerIdentity(databaseName, CosmosKVStoreConstants.GroupByBatchContainer),
          new CosmosContainerIdentity(databaseName, CosmosKVStoreConstants.GroupByStreamingContainer)
        ).asJava
        val proactiveInitCfg =
          new CosmosContainerProactiveInitConfigBuilder(containerIdentityList)
            .setAggressiveWarmupDuration(Duration.ofSeconds(10))
            .build()
        clientBuilder.openConnectionsAndInitCaches(proactiveInitCfg)
      } else {
        logger.info("Preferred regions not configured - skipping proactive container initialization")
      }
    }

    clientBuilder.buildAsyncClient()
  }

  private def parsePreferredRegions(conf: Map[String, String]): java.util.List[String] = {
    conf
      .get(PropCosmosPreferredRegions)
      .orElse(sys.env.get(EnvCosmosPreferredRegions))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList.asJava)
      .getOrElse(java.util.Collections.emptyList[String]())
  }

  private def getOrElseThrow(
      propKey: String,
      envKey: String,
      conf: Map[String, String]
  ): String = {
    conf.getOrElse(
      propKey,
      sys.env.getOrElse(
        envKey,
        throw new IllegalArgumentException(s"$propKey or $envKey required but not found")
      )
    )
  }

  // Cleanup on shutdown
  sys.addShutdownHook {
    Option(clientCache.get()).foreach { client =>
      logger.info("Closing Cosmos client")
      client.close()
    }
  }
}
