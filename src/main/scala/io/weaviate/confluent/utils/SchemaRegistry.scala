package io.weaviate.confluent.utils

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.Base64

import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsDefined, Json, JsString, JsValue}

import scala.io.Source

case class SchemaRegistryConfig(
    apiKey: String,
    apiSecret: String,
    url: String
)

object SchemaRegistry {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Wraps the Confluent Schema Registry API's endpoint to get a schema string
    * by ID.
    *
    * @param id
    *   The ID of the schema to retrieve.
    * @param config
    *   The configuration for the Schema Registry.
    * @return
    *   The schema string.
    * @throws IllegalArgumentException
    *   If the 'schema' key is invalid or missing in the response.
    * @see
    *   [[https://docs.confluent.io/cloud/current/sr/sr-rest-apis.html#get-schema-string-by-id Get schema string by ID]]
    */
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

  /** Wraps the Confluent Schema Registry API's endpoint to get a list of
    * subjects associated to a schema id
    *
    * @param id
    *   The ID of the schema to retrieve.
    * @param config
    *   The configuration for the Schema Registry.
    * @return
    *   A list of subjects associated to the schema id
    * @see
    *   [[https://docs.confluent.io/cloud/current/sr/sr-rest-apis.html#get-subjects-associated-with-the-schema List subjects associated to a schema ID]]
    */
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
    *   [[https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html#search-schema-record-by-name Search schema record by name]]
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

  /** Searches for a schema record by name and returns the fully qualified name
    * of that schema's latest version.
    *
    * @param schemaName
    *   The name of the schema to search for.
    * @param config
    *   The schema registry configuration to use for the search.
    * @return
    *   The fully qualified name of the latest schema matching the given name.
    */
  def getSchemaFullyQualifiedName(
      schemaName: String,
      config: SchemaRegistryConfig
  ): String = {
    val result = SchemaRegistry.searchSchemaRecordByName(schemaName, config)
    val schemaFQNs = result("entities")
      .as[JsArray]
      .value
      .map(entity => entity("attributes")("qualifiedName").as[String])

    if (schemaFQNs.length > 1) {
      logger.warn(
        s"Found ${schemaFQNs.length} schema FQNs for schema $schemaName. Schema evolution is not supported yet. Using the latest schema."
      )
    }

    schemaFQNs.last
  }

  /** Retrieves the top-level tags for a schema from the Confluent Schema
    * Registry.
    *
    * A top-level tag is a tag that is linked to a schema's name, instead of its
    * field(s).
    *
    * @param schemaFQN
    *   The fully-qualified name of the schema to retrieve.
    * @param config
    *   The configuration for the Schema Registry.
    * @return
    *   A list of top-level tags for the schema.
    * @throws NoSuchElementException
    *   If no schema record is found with the given name.
    */
  def getSchemaTopLevelTags(
      schemaFQN: String,
      config: SchemaRegistryConfig
  ): List[String] = {
    val schemaName = schemaFQN.split("\\.").last
    val result = SchemaRegistry.searchSchemaRecordByName(schemaName, config)
    val entities = result("entities").as[JsArray].value
    val matchingSchema = entities
      .filter(e => e("attributes")("qualifiedName").as[String] == schemaFQN)
      .head
    matchingSchema("classificationNames").as[JsArray].as[List[String]]
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
