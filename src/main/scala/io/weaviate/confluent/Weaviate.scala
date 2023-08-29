package io.weaviate.confluent

import io.weaviate.spark.{Weaviate => SparkWeaviate}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog.Table
import io.weaviate.confluent.utils.WeaviateOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

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
}
