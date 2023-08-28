package io.weaviate.confluent

import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.ExposedService
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SharedSparkContext
import io.weaviate.client.v1.schema.model.Property
import io.weaviate.client.v1.schema.model.WeaviateClass
import io.weaviate.spark.WeaviateOptions
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec._
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.jdk.CollectionConverters._
import io.weaviate.client.WeaviateClient
import org.scalatest.BeforeAndAfter

case class Book(title: String, author: String, summary: String)

class ConfluentConnectorFlatSpec
    extends AnyFlatSpec
    with TestContainerForEach
    with DataFrameSuiteBase
    with BeforeAndAfter {

  import spark.implicits._

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

  before {
    val options = new CaseInsensitiveStringMap(
      Map("scheme" -> "http", "host" -> "localhost:8080").asJava
    )
    val weaviateOptions = new WeaviateOptions(options)
    client = weaviateOptions.getClient()
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
