package de.commons.lib.spark

import org.scalatest.Assertion

import java.sql.DriverManager
import java.util.Properties
import scala.jdk.CollectionConverters._

trait MockDbTestSupport {
  protected val properties: Properties = new Properties()

  def mockDB(url: String, dbConfig: Map[String, String])(query: String*)(exec: => Assertion): Unit = {
    Class.forName("org.h2.Driver")
    properties.putAll(dbConfig.mapValues(_.toString).asJava)
    lazy val conn = DriverManager.getConnection(url, properties)
    query.foreach(q => conn.prepareStatement(q).executeUpdate())
    conn.commit()
    exec
    conn.close()
  }
}
