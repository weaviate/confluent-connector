package io.weaviate.confluent

import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.ExposedService
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SharedSparkContext
import io.weaviate.client.v1.schema.model.Property
import io.weaviate.client.v1.schema.model.WeaviateClass
import io.weaviate.spark.WeaviateOptions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec._
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.jdk.CollectionConverters._
import io.weaviate.client.WeaviateClient
import org.scalatest.BeforeAndAfter
import java.util.logging.Logger

case class Book(title: String, author: String, summary: String)

class ConfluentConnectorFlatSpec
    extends AnyFlatSpec
    with TestContainerForEach
    with DataFrameSuiteBase
    with BeforeAndAfter {

  import spark.implicits._

  val confluentBootstrapServers = sys.env("CONFLUENT_BOOTSTRAP_SERVERS")
  val confluentTopicName = sys.env("CONFLUENT_TOPIC_NAME")
  val confluentApiKey = sys.env("CONFLUENT_API_KEY")
  val confluentSecret = sys.env("CONFLUENT_SECRET")

  val confluentSchemaRegistryUrl = sys.env("CONFLUENT_SCHEMA_REGISTRY_URL")
  val confluentSchemaRegistryApiKey = sys.env("CONFLUENT_REGISTRY_API_KEY")
  val confluentSchemaRegistryApiSecret = sys.env("CONFLUENT_REGISTRY_SECRET")

  val logger = Logger.getLogger(getClass.getName)

  override val containerDef =
    DockerComposeContainer.Def(
      new File("src/it/resources/docker-compose.yml"),
      tailChildContainers = true,
      exposedServices = Seq(
        ExposedService(
          "weaviate",
          8080,
          Wait.forHttp("/v1/.well-known/ready").forStatusCode(200)
        )
      )
    )
  var client: WeaviateClient = _
  var testDF: DataFrame = _
  var readStream: DataFrame = _

  private def sendHttpPostRequest(
      urlString: String,
      json: String
  ): (Int, String) = {
    val url = new java.net.URL(urlString)
    val connection = url.openConnection.asInstanceOf[java.net.HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setDoOutput(true)

    val outputStream =
      new java.io.OutputStreamWriter(connection.getOutputStream)
    outputStream.write(json)
    outputStream.flush()

    val responseCode = connection.getResponseCode
    val responseMessage = connection.getResponseMessage

    (responseCode, responseMessage)
  }
  override def beforeAll() {
    super.beforeAll()
    // define the Weaviate client
    val options = new CaseInsensitiveStringMap(
      Map("scheme" -> "http", "host" -> "localhost:8080").asJava
    )
    val weaviateOptions = new WeaviateOptions(options)
    client = weaviateOptions.getClient()

    // define the test data frame
    testDF = spark.read
      .format("avro")
      .load("src/it/resources/clickstream_data.avro")

    // define the spark read stream
    val jarPackages = Array(
      "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
      "org.apache.spark:spark-avro_2.12:3.1.2"
    )

    val sparkSession = SparkSession
      .builder()
      .appName("confluent-weaviate-connector-it")
      .master("local[*]")
      .config("spark.jars.packages", jarPackages.mkString(","))
      .getOrCreate()

    readStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", confluentBootstrapServers)
      .option("subscribe", confluentTopicName)
      .option("startingOffsets", "latest")
      .option("kafka.security.protocol", "SASL_SSL")
      .option(
        "kafka.sasl.jaas.config",
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username='$confluentApiKey' password='$confluentSecret';"""
      )
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("failOnDataLoss", "false")
      .load()

  }

  "Weaviate container" should "have 5 objects" in {

    withContainers { composedContainers =>
      val className = "Clickstream"
      testDF.write
        .format("io.weaviate.confluent.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", className)
        .option("schemaRegistryUrl", confluentSchemaRegistryUrl)
        .option("schemaRegistryApiKey", confluentSchemaRegistryApiKey)
        .option("schemaRegistryApiSecret", confluentSchemaRegistryApiSecret)
        .mode("append")
        .save()

      val results =
        client
          .data()
          .objectsGetter()
          .withClassName(className)
          .run()
          .getResult()

      assert(results.size() == 5)

    }

  }

  "Weaviate container" should "have some Clickstream objects" in {

    withContainers { composedContainers =>
      val className = "Clickstream"

      // crate a schema in Weaviate
      val url = "http://localhost:8080/v1/schema"
      val json =
        scala.io.Source.fromFile("src/it/resources/schema.json").mkString
      val response = sendHttpPostRequest(url, json)

      response match {
        case (200, _) =>
          logger.info("Schema created successfully")
        case (code, message) =>
          println(s"Error creating schema in Weaviate: $code $message")

      }

      // // function to run on each micro-batch
      def f(batchDF: DataFrame, batchId: Long): Unit = {
        batchDF.write
          .format("io.weaviate.confluent.Weaviate")
          .option("scheme", "http")
          .option("host", "localhost:8080")
          .option("className", className)
          .option("schemaRegistryUrl", confluentSchemaRegistryUrl)
          .option("schemaRegistryApiKey", confluentSchemaRegistryApiKey)
          .option("schemaRegistryApiSecret", confluentSchemaRegistryApiSecret)
          .mode("append")
          .save()
      }

      // write the stream to Weaviate
      val query = readStream.writeStream
        // .option("checkpointLocation", checkpointPath)
        .foreachBatch(f _)
        .queryName(s"write-$confluentTopicName-to-weaviate")
        .start()

      // stop after 30 seconds
      query.awaitTermination(30000)
      query.stop()

      // // check that the object was written to Weaviate
      val results =
        client.data().objectsGetter().withClassName(className).run().getResult()
      assert(results.size() > 0)
    }
  }

}
