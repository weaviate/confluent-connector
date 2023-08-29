package io.weaviate.confluent

import io.weaviate.confluent.utils.WeaviateOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.catalog.TableCapability

import java.util
import scala.jdk.CollectionConverters._
import io.weaviate.confluent.writer.WeaviateWriteBuilder

// in scala, you can't extend a case class so copy-paste the code here
case class WeaviateCluster(weaviateOptions: WeaviateOptions, schema: StructType)
    extends SupportsWrite {
  override def name(): String = weaviateOptions.className

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    WeaviateWriteBuilder(weaviateOptions, info.schema())
  }
  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE
  ).asJava
}
