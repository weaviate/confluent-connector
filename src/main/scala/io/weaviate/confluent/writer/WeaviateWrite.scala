package io.weaviate.confluent.writer

import io.weaviate.confluent.utils.WeaviateOptions
import io.weaviate.spark.WeaviateStreamingWriter
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

// in scala, you can't extend a case class so copy-paste the code here
case class WeaviateWrite(weaviateOptions: WeaviateOptions, schema: StructType)
    extends Write
    with Serializable {
  override def toBatch: BatchWrite = {
    WeaviateBatchWriter(weaviateOptions, schema)
  }
  override def toStreaming: StreamingWrite =
    WeaviateStreamingWriter(weaviateOptions, schema)
}
