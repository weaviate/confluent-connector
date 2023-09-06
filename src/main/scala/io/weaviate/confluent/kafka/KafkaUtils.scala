package io.weaviate.confluent.kafka

import scala.collection.JavaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import io.weaviate.confluent.utils.SchemaRegistry

object KafkaUtils {

  /** Deserializes the given byte array into a row and a struct type using the
    * given schema.
    *
    * Note that the `data` parameter should not include the magic and schema ID
    * bytes.
    *
    * @param schema
    *   the schema to use for deserialization
    * @param data
    *   the serialized data to deserialize
    * @return
    *   a tuple containing the deserialized row and struct type
    */
  def deserializeData(schema: Schema, data: Array[Byte]): (Row, StructType) = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(data, null)
    val record = reader.read(null, decoder)

    val fields = record.getSchema.getFields.asScala.map { field =>
      val name = field.name()
      val dataType = getDataType(field.schema().getType)
      StructField(name, dataType)
    }

    val structType = StructType(fields)
    val values = fields.map(field => record.get(field.name)).toArray
    val row = Row.fromSeq(values)

    (row, structType)

  }

  /** Parses a Kafka message from a Spark row.
    *
    * @param row
    *   the Spark row containing the Kafka message data
    * @return
    *   a KafkaMessage object representing the parsed message
    */
  def parseKafkaMessage(
      row: InternalRow
  ): KafkaMessage = {
    val key = new String(row.getBinary(0))
    val value = row.getBinary(1)
    val topic = row.getString(2)
    val partition = row.getInt(3)
    val schemaId = java.nio.ByteBuffer.wrap(value.slice(1, 5)).getInt()
    val data = value.slice(5, value.length)
    val offset = row.getLong(4)
    val timestamp = new java.sql.Timestamp(row.getLong(5))
    val timestampType = row.getInt(6)

    KafkaMessage(
      schemaId,
      key,
      data,
      topic,
      partition,
      offset,
      timestamp,
      timestampType
    )
  }

  /** Processes an array of Spark SQL rows containing Kafka messages.
    *
    * Parses the Kafka messages, deserializes the data, and enriches the
    * deserialized data with additional metadata from the Kafka messages.
    *
    * @param rows
    *   the array of Spark SQL rows containing Kafka messages
    * @param schemaRegistry
    *   a map of schema IDs to Avro schema strings
    * @return
    *   a sequence of tuples containing the enriched deserialized data and its
    *   schema
    */
  def processRows(
      rows: Array[InternalRow]
  ): Seq[(Row, StructType)] = {
    val kafkaMessages = rows.map(parseKafkaMessage)
    val schemaIds = kafkaMessages.map(_.schemaId.toString).distinct

    val deserializedRowsWithSchema = schemaIds.flatMap { schemaId =>
      val schemaString = Some(
        SchemaRegistry.getSchemaById(
          schemaId.toInt,
          sys.env("CONFLUENT_REGISTRY_API_KEY"),
          sys.env("CONFLUENT_REGISTRY_SECRET"),
          sys.env("CONFLUENT_SCHEMA_REGISTRY_URL")
        )
      )

      schemaString.map { schemaString =>
        val schema = new Schema.Parser().parse(schemaString)
        val matchingMessages =
          kafkaMessages.filter(_.schemaId.toString == schemaId)

        processKafkaMessages(schema, matchingMessages)
      }

    }.flatten

    deserializedRowsWithSchema
  }

  /** Processes an array of Kafka messages by deserializing the data field and
    * enriching it with metadata from the messages.
    *
    * @param schema
    *   the Avro schema to use for deserialization
    * @param kafkaMessages
    *   the array of Kafka messages to process
    * @return
    *   a sequence of tuples containing the enriched deserialized data and its
    *   schema
    */
  def processKafkaMessages(
      schema: Schema,
      kafkaMessages: Array[KafkaMessage]
  ): Seq[(Row, StructType)] = {
    kafkaMessages.map { kafkaMessage =>
      val (row, struct) = deserializeData(schema, kafkaMessage.data)
      val (enrichedRow, enrichedStruct) =
        addMessageFields(row, struct, kafkaMessage)

      (enrichedRow, enrichedStruct)
    }
  }

  /** Enriches a Spark SQL row and struct type with metadata from a Kafka
    * message.
    *
    * Adds fields to the struct type for the Kafka message key, topic,
    * partition, offset, timestamp, and timestamp type, and appends the
    * corresponding values to the row.
    *
    * @param row
    *   the Spark SQL row to enrich
    * @param struct
    *   the Spark SQL struct type to enrich
    * @param kafkaMessage
    *   the Kafka message to extract metadata from
    * @return
    *   a tuple containing the enriched row and struct type
    */
  def addMessageFields(
      row: Row,
      struct: StructType,
      kafkaMessage: KafkaMessage
  ): (Row, StructType) = {
    val kafkaFields = Seq(
      ("_kafka_key", kafkaMessage.key),
      ("_kafka_topic", kafkaMessage.topic),
      ("_kafka_partition", kafkaMessage.partition),
      ("_kafka_offset", kafkaMessage.offset),
      ("_kafka_timestamp", kafkaMessage.timestamp),
      ("_kafka_timestampType", kafkaMessage.timestampType),
      ("_kafka_schemaId", kafkaMessage.schemaId)
    )

    val updatedFields = kafkaFields.foldLeft(struct.fields) {
      case (fields, (name, value)) =>
        val dataType = getDataType(value)
        val field = StructField(name, dataType)
        fields :+ field
    }

    val updatedStruct = StructType(updatedFields)
    val updatedValues = row.toSeq ++ kafkaFields.map(_._2)
    val updatedRow = Row.fromSeq(updatedValues)

    (updatedRow, updatedStruct)
  }

  def avroToSparkDataType(avroType: Schema.Type): DataType = avroType match {
    case Schema.Type.INT     => IntegerType
    case Schema.Type.LONG    => LongType
    case Schema.Type.FLOAT   => FloatType
    case Schema.Type.DOUBLE  => DoubleType
    case Schema.Type.BOOLEAN => BooleanType
    case Schema.Type.STRING  => StringType
    case Schema.Type.BYTES   => BinaryType
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported Avro data type: $avroType"
      )
  }

  def scalaToSparkDataType(value: Any): DataType = value match {
    case _: Integer     => IntegerType
    case _: Long        => LongType
    case _: Float       => FloatType
    case _: Double      => DoubleType
    case _: Boolean     => BooleanType
    case _: String      => StringType
    case _: Array[Byte] => BinaryType
    case _: Timestamp   => TimestampType
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported Scala data type: ${value.getClass}"
      )
  }

  /** Infers the Spark SQL data type for the given value.
    *
    * If the value is an Avro schema type, the corresponding Spark SQL data type
    * is returned. Otherwise, the value is assumed to be a Scala primitive type
    * and the corresponding Spark SQL data type is returned.
    *
    * @param value
    *   the value to infer the data type for
    * @return
    *   the inferred Spark SQL data type
    */
  def getDataType(value: Any): DataType = value match {
    case avroType: Schema.Type => avroToSparkDataType(avroType)
    case _                     => scalaToSparkDataType(value)
  }
}
