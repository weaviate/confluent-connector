package io.weaviate.confluent.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers



class SchemaRegistryFlatSpec extends AnyFlatSpec with DataFrameSuiteBase with BeforeAndAfterAll with Matchers{
    val schemaRegistryUrl = sys.env("CONFLUENT_SCHEMA_REGISTRY_URL")
    val schemaRegistryApiKey = sys.env("CONFLUENT_REGISTRY_API_KEY")
    val schemaRegistryApiSecret = sys.env("CONFLUENT_REGISTRY_SECRET")
    var testDF: org.apache.spark.sql.DataFrame = _

    override def beforeAll() {
        super.beforeAll()
        
        val jarPackages = List("org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", 
                        "org.apache.spark:spark-avro_2.12:3.4.0")
        
        val spark = SparkSession.builder()
            .appName("scalaWorksheet")
            .master("local[*]") 
            .config("spark.jars.packages", jarPackages.mkString(","))
            .getOrCreate()
        
        testDF = spark.read
            .format("avro")
            .load("src/it/resources/clickstream_data.avro")
    }

    "SchemaRegistry" should "deserialize the 'value' column" in {
        // a message from kafka has a column named "value" that contains the actual data
        // but in a binary format. The schema is stored in the schema registry
        
        val deserializedDF = SchemaRegistry.deserializeDataFrame(
            testDF,
            schemaRegistryUrl,
            schemaRegistryApiKey,
            schemaRegistryApiSecret
        )

        val actualDeserializedColumns = deserializedDF.select("value.*").columns
        val expectedDeserializedColumns = Array("user_id", "username", "registered_at", "first_name", "last_name", "city", "level")

        actualDeserializedColumns should be (expectedDeserializedColumns)

    }
    
}