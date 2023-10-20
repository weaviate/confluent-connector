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
import play.api.libs.json.JsValue

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

  /** Wraps the Confluent Schema Registry API endpoint for searching schema
    * records by name.
    *
    * @param query
    *   The query string to search for.
    * @param config
    *   The configuration object for the Schema Registry.
    * @return
    *   A JSON object representing the search results.
    * @see
    *   <a
    *   href="https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html#search-schema-record-by-name">Confluent
    *   Schema Registry API documentation</a>
    */
  def searchSchemaRecordByName(
      query: String,
      config: SchemaRegistryConfig
  ): JsValue = {
    val schemaRegistryUrl = config.url
    val authToken = buildAuthToken(config)

    val client = HttpClient.newHttpClient()

    val request = HttpRequest
      .newBuilder()
      .uri(
        URI.create(
          s"$schemaRegistryUrl/catalog/v1/search/basic?type=sr_record&query=$query"
        )
      )
      .header("Authorization", s"Basic $authToken")
      .GET()
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    Json.parse(response.body)
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
