package io.weaviate.confluent.utils
import scala.io.Source
import play.api.libs.json.{Json, JsDefined, JsString}
import org.apache.spark.sql.{DataFrame, functions => fn}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.avro.functions.from_avro

object SchemaRegistry {

  private val schemaIdColumn = "schema_id"

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
