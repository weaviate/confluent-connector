package io.weaviate.confluent.utils
import scala.io.Source
import play.api.libs.json.{Json, JsDefined, JsString}
import org.apache.spark.sql.{DataFrame, functions => fn}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.avro.functions.from_avro

object SchemaRegistry {

  private def extractSchemaId(df: DataFrame): DataFrame = {
    val binaryToString = fn.udf((x: Array[Byte]) => BigInt(x).toString)
    df.withColumn("key", fn.col("key").cast("string"))
      .withColumn("_schema_id", fn.expr("substring(value, 2, 4)"))
      .withColumn(
        "_schema_id",
        binaryToString(fn.col("_schema_id").cast(BinaryType))
      )
  }

  private def skipFirstFiveBytes(
      df: DataFrame,
      column: String = "value"
  ): DataFrame = {
    df.withColumn(column, fn.expr(s"substring($column, 6, length($column)-5)"))
  }

  private def deserializeValue(
      df: DataFrame,
      schemaRegistryUrl: String,
      schemaRegistryApiKey: String,
      schemaRegistryApiSecret: String
  ): DataFrame = {
    val distinctSchemaIds =
      df.select("_schema_id").distinct().collect().map(_.getString(0))
    val deserializedDFs = distinctSchemaIds.map(id => {
      val schema = getSchemaById(
        id.toInt,
        schemaRegistryApiKey,
        schemaRegistryApiSecret,
        schemaRegistryUrl
      )
      df.filter(fn.col("_schema_id") === id)
        .withColumn("value", from_avro(fn.col("value"), schema))
    })
    deserializedDFs.reduce(_ union _)
  }

  def deserializeDataFrame(
      df: DataFrame,
      schemaRegistryUrl: String,
      schemaRegistryApiKey: String,
      schemaRegistryApiSecret: String
  ): DataFrame = {

    val schemaIdExtractedDF = extractSchemaId(df)
    val skippedBytesDF = skipFirstFiveBytes(schemaIdExtractedDF, "value")
    
    deserializeValue(
      skippedBytesDF,
      schemaRegistryUrl,
      schemaRegistryApiKey,
      schemaRegistryApiSecret
    )
  }

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
