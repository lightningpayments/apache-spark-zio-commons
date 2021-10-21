package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.environments.io.SparkDBDataFrameReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Customer
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkTestSupport, TestSpec}
import zio.ZIO

class CustomerSpec extends TestSpec with SparkTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val customer =
    Customer("C00001", "Micheal", "New York", "New York", "USA", 2, 3000, 5000, 2000, 6000, "CCCC", "A008")

  "Customer#select" must {
    "return one customer dataset" in withSparkSession { spark => logger =>
      mockDB(url, dbConf)(createTableCustomerQuery, customer.insert) {
        val program = (for {
          env    <- ZIO.environment[SparkDBDataFrameReader]
          reader  = env.reader(spark)(url, properties)(_)
          ds      = Customer.select(reader)
        } yield ds).provide(SparkDBDataFrameReader)

        whenReady(program)(_.map(_.collect().toList) mustBe Right(customer :: Nil))
      }
    }
  }

}
