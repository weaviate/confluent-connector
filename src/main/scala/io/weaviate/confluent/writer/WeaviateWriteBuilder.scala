package io.weaviate.confluent.writer

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
import io.weaviate.confluent.utils.WeaviateOptions

// in scala, you can't extend a case class so copy-paste the code here
case class WeaviateWriteBuilder(
    weaviateOptions: WeaviateOptions,
    schema: StructType
) extends WriteBuilder
    with Serializable {
  override def build(): Write = WeaviateWrite(weaviateOptions, schema)
}
