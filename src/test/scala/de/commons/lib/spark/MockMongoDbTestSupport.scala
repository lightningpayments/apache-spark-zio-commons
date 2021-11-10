package de.commons.lib.spark

import com.mongodb.client.MongoClient
import org.bson.Document
import org.scalatest.Assertion

import java.util.Properties
import scala.jdk.CollectionConverters._

trait MockMongoDbTestSupport {
  protected val properties: Properties = new Properties()

  def mockDb(
    preparedDocs: List[Document])(
    dbName: String,
    collection: String,
    client: MongoClient)(
    exec: => Assertion
  ): Unit = {
    client.getDatabase(dbName).getCollection(collection).insertMany(preparedDocs.asJava)
    exec
  }
}
