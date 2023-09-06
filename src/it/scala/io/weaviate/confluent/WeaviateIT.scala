package io.weaviate.confluent

import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.ExposedService
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SharedSparkContext
import io.weaviate.client.v1.schema.model.Property
import io.weaviate.client.v1.schema.model.WeaviateClass
import io.weaviate.spark.WeaviateOptions
import org.apache.spark.sql.{SparkSession, DataFrame}
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

  "Weaviate container" should "have 1 object" in {

    withContainers { composedContainers =>
      
      // create a schema to hold a book
      val properties = Seq(
        Property
          .builder()
          .dataType(List[String]("text").asJava)
          .name("title")
          .build(),
        Property
          .builder()
          .dataType(List[String]("text").asJava)
          .name("author")
          .build(),
        Property
          .builder()
          .dataType(List[String]("text").asJava)
          .name("summary")
          .build()
      )
      val schema = WeaviateClass.builder
        .className("Book")
        .properties(properties.asJava)
        .build
      client.schema().classCreator().withClass(schema).run

      // create a dataframe containing one book
      val book = Book(
        "The Catcher in the Rye",
        "J.D. Salinger",
        "A novel about a teenage boy named Holden Caulfield who is expelled from his school."
      )
      val df = Seq(book).toDF()

      // write the dataframe to Weaviate
      df.write
        .format("io.weaviate.confluent.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Book")
        .mode("append")
        .save()

      // check that the object was written to Weaviate
      val results =
        client.data().objectsGetter().withClassName("Book").run().getResult()
      assert(results.size() == 1)
    }
  }
}
