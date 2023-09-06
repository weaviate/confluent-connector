package io.weaviate.confluent

import io.weaviate.confluent.utils.WeaviateOptions
import io.weaviate.spark.Utils
import io.weaviate.spark.WeaviateClassNotFoundError
import io.weaviate.spark.WeaviateResultError
import io.weaviate.spark.{Weaviate => SparkWeaviate}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

class Weaviate extends SparkWeaviate {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val weaviateOptions = new WeaviateOptions(
      new CaseInsensitiveStringMap(properties)
    )
    WeaviateCluster(weaviateOptions, schema)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // this is the schema of ANY kafka message
    StructType(
      Seq(
        StructField("key", BinaryType, true),
        StructField("value", BinaryType, true),
        StructField("topic", StringType, true),
        StructField("partition", IntegerType, true),
        StructField("offset", LongType, true),
        StructField("timestamp", TimestampType, true),
        StructField("timestampType", IntegerType, true)
      )
    )
  }
}
