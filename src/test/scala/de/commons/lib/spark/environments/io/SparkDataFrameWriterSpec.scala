package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.TableName
import org.apache.spark.sql.{Encoder, Encoders}
import zio.{Task, ZIO}

class SparkDataFrameWriterSpec extends TestSpec with SparkTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)

  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]
  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val query: String = "CREATE TABLE IF NOT EXISTS sparkDbDataFrameWriter (id int);"

  "SparkDbDataFrameWriter#writeAppend" must {
    "return unit" in withSparkSession { spark => logger =>
      val program = ZIO.environment[SparkDataFrameWriter].flatMap { writer =>
        import spark.implicits._
        Task((Dummy(1) :: Dummy(2) :: Nil).toDF())
        for {
          df <- Task((Dummy(1) :: Dummy(2) :: Nil).toDF())
          _  <- Task(writer.insert(url, properties)(df, TableName("sparkDbDataFrameWriter")))
        } yield ()
      }

      mockDb(url = url, dbConfig = dbConf)(query = query) {
        whenReady(program.provide(SparkDataFrameWriter))(_ mustBe Right(()))
      }
    }
  }

}
