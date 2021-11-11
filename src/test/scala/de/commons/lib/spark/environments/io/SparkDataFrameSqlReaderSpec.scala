package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql.{Encoder, Encoders, Row}
import zio.ZIO

class SparkDataFrameSqlReaderSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)
  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]

  "SparkDataFrameSqlReader#DatabaseReader" must {
    "return 1" in withSparkSession { implicit spark => _ =>
      val url = "jdbc:h2:mem:testdb;MODE=MYSQL"
      val createTable = "create table if not exists sparkDataFrameReader (id int)"
      val insert = "INSERT INTO `sparkDataFrameReader` values (1)"

      val reader =
        SparkDataFrameReader.DatabaseReader(url, properties)(SqlQuery("(SELECT * FROM sparkDataFrameReader) as q1"))

      mockDb(url, dbConf)(createTable, insert) {
        whenReady(ZIO(reader.run.collect().headOption))(_ mustBe Right(Some(Row(1))))
      }
    }
  }

}
