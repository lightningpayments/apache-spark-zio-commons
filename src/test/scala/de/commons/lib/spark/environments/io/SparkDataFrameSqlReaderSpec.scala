package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql.{Encoder, Encoders}
import zio.ZIO

class SparkDataFrameSqlReaderSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)
  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]

  "SparkDataFrameReader#apply" must {
    "return 1" in withSparkSession { spark => _ =>
      val url = "jdbc:h2:mem:testdb;MODE=MYSQL"
      val createTable = "create table if not exists sparkDataFrameReader (id int)"
      val insert = "INSERT INTO `sparkDataFrameReader` values (1)"

      val program = ZIO.environment[SparkDataFrameReader].map {
        _
          .sqlReader(spark)(url, properties)(SqlQuery("(SELECT * FROM sparkDataFrameReader) as q1"))
          .as[Dummy]
          .count()
      }

      mockDb(url, dbConf)(createTable, insert) {
        whenReady(program.provide(SparkDataFrameReader))(_ mustBe Right(1))
      }
    }
  }

}