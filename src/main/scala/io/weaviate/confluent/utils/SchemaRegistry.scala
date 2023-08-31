package io.weaviate.confluent.utils
import scala.io.Source
import play.api.libs.json.{Json, JsDefined, JsString}

object SchemaRegistry {

  def getSchemaById(
      id: Int,
      registryApiKey: String,
      registryApiSecret: String,
      schemaRegistryUrl: String
  ): String = {
    val url = new java.net.URL(s"$schemaRegistryUrl/schemas/ids/$id")
    val connection = url.openConnection.asInstanceOf[java.net.HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty(
      "Authorization",
      s"Basic ${java.util.Base64.getEncoder
          .encodeToString(s"$registryApiKey:$registryApiSecret".getBytes("UTF-8"))}"
    )

    val response = Source.fromInputStream(connection.getInputStream).mkString

    Json.parse(response) \ "schema" match {
      case JsDefined(JsString(text)) => text
      case _ =>
        throw new IllegalArgumentException("Invalid or missing 'schema' key.")
    }
  }
}
