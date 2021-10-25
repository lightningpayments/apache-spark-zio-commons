package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.environments.io.SparkDbDataFrameReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Order
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkTestSupport, TestSpec}
import zio.ZIO

import java.time.LocalDate

class OrderSpec extends TestSpec with SparkTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val order =
    Order(200100, 1000.00F, 600.00F, LocalDate.parse("2008-08-01"), "C00013", "A001", "SOD")

  "Order#select" must {
    "return one order dataset" in withSparkSession { spark => logger =>
      mockDb(url, dbConf)(createTableOrdersQuery, order.insert) {
        val program = (for {
          env    <- ZIO.environment[SparkDbDataFrameReader]
          reader  = env.reader(spark)(url, properties)(_)
          ds      = Order.select(reader)
        } yield ds).provide(SparkDbDataFrameReader)

        whenReady(program)(_.map(_.collect().toList) mustBe Right(order :: Nil))
      }
    }
  }

}
