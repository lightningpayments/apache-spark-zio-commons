package de.commons.lib.spark

import org.scalatest.Assertion

import java.sql.DriverManager
import java.util.Properties
import scala.jdk.CollectionConverters._

trait MockDbTestSupport {
  protected val properties: Properties = new Properties()

  def mockDb(url: String, dbConfig: Map[String, String])(query: String*)(exec: => Assertion): Unit = {
    lazy val conn = DriverManager.getConnection(url, properties)

    Class.forName("org.h2.Driver")
    properties.putAll(dbConfig.mapValues(identity).asJava)

    query.foreach(conn.prepareStatement(_).executeUpdate())

    conn.commit()
    exec
    conn.close()
  }
}
