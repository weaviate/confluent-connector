package io.weaviate.confluent.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

class WeaviateOptionsSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfter {
  var config: CaseInsensitiveStringMap = _

  before {
    // create a config
  }
  "WeaviateOptions" should "have a static property called WEAVIATE_HOST_CONF with value host" in {
    WeaviateOptions should have('WEAVIATE_HOST_CONF("host"))
  }

  it should "have a static property called CONFLUENT_SCHEMA_REGISTRY_URL with value schemaRegistryUrl" in {
    WeaviateOptions should have(
      'CONFLUENT_SCHEMA_REGISTRY_URL("schemaRegistryUrl")
    )
  }

  it should "have a static property called CONFLUENT_SCHEMA_REGISTRY_API_KEY with value schemaRegistryApiKey" in {
    WeaviateOptions should have(
      'CONFLUENT_SCHEMA_REGISTRY_API_KEY("schemaRegistryApiKey")
    )
  }

  it should "have a static property called CONFLUENT_SCHEMA_REGISTRY_API_SECRET with value schemaRegistryApiSecret" in {
    WeaviateOptions should have(
      'CONFLUENT_SCHEMA_REGISTRY_API_SECRET("schemaRegistryApiSecret")
    )
  }

  it should "read the confluent schema registry parameters from the supplied config" in {
    val params = Map(
      "schemaRegistryUrl" -> "http://localhost:8081",
      "schemaRegistryApiKey" -> "API_KEY_42",
      "schemaRegistryApiSecret" -> "SuperSecretPassword"
    )
    val config = new CaseInsensitiveStringMap(params.asJava)
    val weaviateOptions = new WeaviateOptions(config)

    weaviateOptions.schemaRegistryUrl should be("http://localhost:8081")
    weaviateOptions.schemaRegistryApiKey should be("API_KEY_42")
    weaviateOptions.schemaRegistryApiSecret should be("SuperSecretPassword")
  }

}
