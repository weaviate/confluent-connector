package io.weaviate.confluent.kafka

/** Represents a Kafka message
  *
  * @param schemaId
  *   the ID of the schema used to serialize the message data
  * @param key
  *   the message key
  * @param data
  *   the serialized message data without the magic byte and schema ID
  * @param topic
  *   the Kafka topic that the message was published to
  * @param partition
  *   the Kafka partition that the message was published to
  * @param offset
  *   the offset of the message within the Kafka partition
  * @param timestamp
  *   the timestamp of the message
  * @param timestampType
  *   the type of the message timestamp
  */
case class KafkaMessage(
    schemaId: Integer,
    key: String,
    data: Array[Byte],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: java.sql.Timestamp,
    timestampType: Int
)
