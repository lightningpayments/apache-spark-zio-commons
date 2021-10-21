package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql.{Encoder, Encoders}
import zio.ZIO

class SparkDBDataFrameReaderSpec extends TestSpec with SparkTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)

  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]

  "SparkIODbDataFrameReader#apply" must {
    "return 1" in withSparkSession { spark => logger =>
      val url = "jdbc:h2:mem:testdb;MODE=MYSQL"
      val createTable = "create table if not exists sparkIODbDataFrameReader (id int)"
      val insert = "INSERT INTO `sparkIODbDataFrameReader` values (1)"

      val program = for {
        env <- ZIO.environment[SparkDBDataFrameReader]
        ds   = env.reader(spark)(url, properties)(
          SqlQuery("(SELECT * FROM sparkIODbDataFrameReader) as q1")
        ).as[Dummy]
      } yield ds.count()

      mockDB(url, dbConf)(createTable, insert) {
        whenReady(program.provide(SparkDBDataFrameReader))(_ mustBe Right(1))
      }
    }
  }

}
