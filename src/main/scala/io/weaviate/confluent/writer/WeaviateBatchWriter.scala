package io.weaviate.confluent.writer

import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType
import io.weaviate.confluent.utils.WeaviateOptions

// in scala, you can't extend a case class so copy-paste the code here
case class WeaviateBatchWriter(
    weaviateOptions: WeaviateOptions,
    schema: StructType
) extends BatchWrite {
  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    WeaviateDataWriterFactory(weaviateOptions, schema)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}
