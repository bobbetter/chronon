package ai.chronon.integrations.cloud_gcp

import com.google.auth.oauth2.GoogleCredentials
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.{HttpResponse, WebClient}

import scala.jdk.CollectionConverters._

sealed trait HttpMethod
case object GetMethod extends HttpMethod
case object PostMethod extends HttpMethod
case object PutMethod extends HttpMethod
case object DeleteMethod extends HttpMethod

class VertexHttpClient(client: WebClient) extends Serializable {

  // Init google creds - we need this to set the Auth header
  private lazy val googleCredentials: GoogleCredentials =
    GoogleCredentials
      .getApplicationDefault()
      .createScoped(List("https://www.googleapis.com/auth/aiplatform").asJava)

  private def getAccessToken: String = {
    googleCredentials.refreshIfExpired()
    googleCredentials.getAccessToken.getTokenValue
  }

  def makeHttpRequest[T](url: String, method: HttpMethod, requestBody: Option[JsonObject] = None)(
      handler: HttpResponse[Buffer] => T): Unit = {
    val request = method match {
      case GetMethod    => client.getAbs(url)
      case PostMethod   => client.postAbs(url)
      case PutMethod    => client.putAbs(url)
      case DeleteMethod => client.deleteAbs(url)
      case _            => throw new IllegalArgumentException(s"Currently unsupported HTTP method: $method")
    }

    // Add common headers
    val requestWithHeaders = request
      .putHeader("Authorization", s"Bearer $getAccessToken")
    val finalRequest = if (requestBody.isDefined) {
      requestWithHeaders
        .putHeader("Content-Type", "application/json")
        .putHeader("Accept", "application/json")
    } else {
      requestWithHeaders
    }

    // Send the request
    val responseFuture = requestBody match {
      case Some(body) => finalRequest.sendJsonObject(body)
      case None       => finalRequest.send()
    }

    responseFuture.onComplete { asyncResult =>
      if (asyncResult.succeeded()) {
        handler(asyncResult.result())
      } else {
        handler(null)
      }
    }
  }
}

object VertexHttpUtils {

  def convertToVertxJson(obj: Any): Any = {
    obj match {
      case map: Map[_, _] =>
        val jsonObject = new JsonObject()
        map.foreach { case (key, value) =>
          jsonObject.put(key.toString, convertToVertxJson(value))
        }
        jsonObject
      case seq: Seq[_] =>
        val jsonArray = new JsonArray()
        seq.foreach { item =>
          jsonArray.add(convertToVertxJson(item))
        }
        jsonArray
      case other => other
    }
  }

  /** Build the request body for Vertex AI prediction requests.
    * Format is the same for both publisher and custom models:
    * { "instances": [ { ..req 1..}, { ... } ], "parameters": { ... } }
    *
    * Publisher: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings
    * Custom: https://docs.cloud.google.com/vertex-ai/docs/predictions/get-online-predictions
    */
  def createPredictionRequestBody(inputRequests: Seq[Map[String, AnyRef]],
                                  modelParams: Map[String, String]): JsonObject = {
    val instancesArray = new JsonArray()

    inputRequests.foreach { inputRequest =>
      val instance = inputRequest("instance")
      val jsonInstance = convertToVertxJson(instance)
      instancesArray.add(jsonInstance)
    }

    val requestBody = new JsonObject()
    requestBody.put("instances", instancesArray)

    // Add parameters if present (exclude model_name and model_type)
    val additionalParams = modelParams.filterKeys(k => k != "model_name" && k != "model_type")
    if (additionalParams.nonEmpty) {
      val parametersObj = new JsonObject()
      additionalParams.foreach { case (key, value) =>
        parametersObj.put(key, value)
      }
      requestBody.put("parameters", parametersObj)
    }

    requestBody
  }

  /** Extract prediction results from Vertex AI prediction response.
    * Response is a JsonObject with "predictions": [ {...}, {...} ]
    */
  def extractPredictionResults(responseBody: JsonObject): Seq[Map[String, AnyRef]] = {
    val predictions = responseBody.getJsonArray("predictions")

    if (predictions == null) {
      throw new RuntimeException("No 'predictions' array found in response")
    }

    (0 until predictions.size()).map { index =>
      val predictionJsonObject = predictions.getJsonObject(index)
      predictionJsonObject.getMap.asScala.toMap
    }
  }
}
