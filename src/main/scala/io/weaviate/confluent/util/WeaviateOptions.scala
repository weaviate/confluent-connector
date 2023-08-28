package io.weaviate.confluent.util

import  io.weaviate.spark.{WeaviateOptions => SparkWeaviateOptions}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WeaviateOptions(config: CaseInsensitiveStringMap) extends SparkWeaviateOptions(config) {

}

object WeaviateOptions  {
    // in scala, an object cannot inherit from another object, so 
    // we have to copy the val from the 'parent' object
    val WEAVIATE_BATCH_SIZE_CONF: String = "batchSize"
    val WEAVIATE_HOST_CONF: String       = "host"
    val WEAVIATE_SCHEME_CONF: String     = "scheme"
    val WEAVIATE_CLASSNAME_CONF: String  = "className"
    val WEAVIATE_VECTOR_COLUMN_CONF: String  = "vector"
    val WEAVIATE_ID_COLUMN_CONF: String  = "id"
    val WEAVIATE_RETRIES_CONF: String = "retries"
    val WEAVIATE_RETRIES_BACKOFF_CONF: String = "retriesBackoff"
    val WEAVIATE_TIMEOUT: String = "timeout"
    val WEAVIATE_OIDC_USERNAME: String = "oidc:username"
    val WEAVIATE_OIDC_PASSWORD: String = "oidc:password"
    val WEAVIATE_OIDC_CLIENT_SECRET: String = "oidc:clientSecret"
    val WEAVIATE_OIDC_ACCESS_TOKEN: String = "oidc:accessToken"
    val WEAVIATE_OIDC_ACCESS_TOKEN_LIFETIME: String = "oidc:accessTokenLifetime"
    val WEAVIATE_OIDC_REFRESH_TOKEN: String = "oidc:refreshToken"
    val WEAVIATE_API_KEY: String = "apiKey"
    val WEAVIATE_HEADER_PREFIX: String = "header:"
    val CONFLUENT_SCHEMA_REGISTRY_URL: String = "schemaRegistryUrl"
    val CONFLUENT_SCHEMA_REGISTRY_API_KEY: String = "schemaRegistryApiKey"
    val CONFLUENT_SCHEMA_REGISTRY_API_SECRET: String = "schemaRegistryApiSecret"
     
}
