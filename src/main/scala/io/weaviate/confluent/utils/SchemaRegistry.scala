package io.weaviate.confluent.utils
import scala.io.Source
import play.api.libs.json.{JsDefined, Json, JsString}
import org.apache.spark.sql.{functions => fn, DataFrame}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.avro.functions.from_avro
import java.util.Base64
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.URI
import java.net.http.HttpResponse

case class SchemaRegistryConfig(
    apiKey: String,
    apiSecret: String,
    url: String
)

object SchemaRegistry {

  def getSchemaById(
      id: Int,
      config: SchemaRegistryConfig
  ): String = {
    val schemaRegistryUrl = config.url
    val schemaRegistryApiKey = config.apiKey
    val schemaRegistryApiSecret = config.apiSecret

    val url = new java.net.URL(s"$schemaRegistryUrl/schemas/ids/$id")
    val connection = url.openConnection.asInstanceOf[java.net.HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty(
      "Authorization",
      s"Basic ${java.util.Base64.getEncoder
          .encodeToString(s"$schemaRegistryApiKey:$schemaRegistryApiSecret".getBytes("UTF-8"))}"
    )

    val response = Source.fromInputStream(connection.getInputStream).mkString

    Json.parse(response) \ "schema" match {
      case JsDefined(JsString(text)) => text
      case _ =>
        throw new IllegalArgumentException("Invalid or missing 'schema' key.")
    }
  }

  def getSubjectsAssociatedToSchemaId(
      id: Int,
      config: SchemaRegistryConfig
  ): List[String] = {

    val schemaRegistryUrl = config.url

    val authToken = buildAuthToken(config)

    val client = HttpClient.newHttpClient()

    val request = HttpRequest
      .newBuilder()
      .uri(URI.create(s"$schemaRegistryUrl/schemas/ids/$id/subjects"))
      .header("Authorization", s"Basic $authToken")
      .GET()
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    Json.parse(response.body).as[List[String]]

  }

  private def buildAuthToken(config: SchemaRegistryConfig): String = {
    val apiKey = config.apiKey
    val apiSecret = config.apiSecret

    val authString = s"$apiKey:$apiSecret"
    val encodedAuthString =
      Base64.getEncoder.encodeToString(authString.getBytes("UTF-8"))
    encodedAuthString
  }
}
