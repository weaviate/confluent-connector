package io.weaviate.confluent.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType
import io.weaviate.confluent.utils.WeaviateOptions

// in scala, you can't extend a case class so copy-paste the code here
case class WeaviateDataWriterFactory(
    weaviateOptions: WeaviateOptions,
    schema: StructType
) extends DataWriterFactory
    with StreamingDataWriterFactory
    with Serializable {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    WeaviateDataWriter(weaviateOptions, schema)
  }

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long
  ): DataWriter[InternalRow] = {
    WeaviateDataWriter(weaviateOptions, schema)
  }
}
