package io.weaviate.confluent.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers

class SchemaRegistryFlatSpec
    extends AnyFlatSpec
    with DataFrameSuiteBase
    with BeforeAndAfterAll
    with Matchers {
  val schemaRegistryUrl = sys.env("CONFLUENT_SCHEMA_REGISTRY_URL")
  val schemaRegistryApiKey = sys.env("CONFLUENT_REGISTRY_API_KEY")
  val schemaRegistryApiSecret = sys.env("CONFLUENT_REGISTRY_SECRET")
  val schemaName = "integrationTestRecord"
  val schemaFQN = "lsrc-j81now:.:100004:io.weaviate.integrationTestRecord"

  val config = SchemaRegistryConfig(
    schemaRegistryApiKey,
    schemaRegistryApiSecret,
    schemaRegistryUrl
  )

  var testDF: org.apache.spark.sql.DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    val jarPackages = List(
      "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
      "org.apache.spark:spark-avro_2.12:3.4.0"
    )

    val spark = SparkSession
      .builder()
      .appName("scalaWorksheet")
      .master("local[*]")
      .config("spark.jars.packages", jarPackages.mkString(","))
      .getOrCreate()

    testDF = spark.read
      .format("avro")
      .load("src/it/resources/clickstream_data.avro")
  }

  "getSchemaById" should "return a schema string if the schema id is valid" in {
    val schemaId = 100002
    val schema = SchemaRegistry.getSchemaById(schemaId, config)

    schema should not be empty
    schema shouldBe a[String]
    schema should fullyMatch regex """^\{.*\}$"""

  }

  it should "throw a file not found exception if the schema id is invalid" in {
    val schemaId = 999999
    val exception = intercept[java.io.FileNotFoundException] {
      SchemaRegistry.getSchemaById(schemaId, config)
    }

    assert(exception.getMessage == s"${config.url}/schemas/ids/$schemaId")

  }

  "schema id 100004" should "be associated with the subject 'confluent-connector-integrationt-test' only" in {
    val schemaId = 100004
    val subjects =
      SchemaRegistry.getSubjectsAssociatedToSchemaId(schemaId, config)

    subjects should have size 1
    subjects should contain("confluent-connector-integrationt-test")

  }

  "getSchemaFullyQualifiedName" should "return the fully qualified name of the latest schema" in {
    val result = SchemaRegistry.getSchemaFullyQualifiedName(schemaName, config)

    result shouldEqual "lsrc-j81now:.:100004:io.weaviate.integrationTestRecord"
  }

  "getSchemaTopLevelTags" should "return the top-level tags for a schema" in {

    val result = SchemaRegistry.getSchemaTopLevelTags(schemaFQN, config)

    result should have size 1
    result should contain("vectorizer_txt2vec_cohere")

  }

  it should "throw an exception if no schema record is found with the given name" in {
    val nonExistingSchemaFQN = schemaFQN + "non-existing"

    assertThrows[RuntimeException] {
      SchemaRegistry.getSchemaTopLevelTags(nonExistingSchemaFQN, config)
    }
  }

}
