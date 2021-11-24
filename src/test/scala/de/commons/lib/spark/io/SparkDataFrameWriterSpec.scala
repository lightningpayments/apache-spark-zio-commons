package de.commons.lib.spark.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.{SqlQuery, TableName}
import org.apache.spark.sql.{Encoder, Encoders}
import zio.Task

class SparkDataFrameWriterSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)
  private implicit val ordering: Ordering[Dummy] = (x: Dummy, y: Dummy) => x.id compare y.id

  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]
  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val query: String = "CREATE TABLE IF NOT EXISTS sparkDbDataFrameWriter (id int);"

  "SparkDbDataFrameWriter#insert" must {
    "return unit" in withSparkSession { spark => _ =>
      import spark.implicits._
      val program = for {
        df     <- Task((Dummy(1) :: Dummy(2) :: Nil).toDF())
        writer <- Task(SparkDataFrameWriter.DatabaseInsert(url, properties)(TableName("sparkDbDataFrameWriter")))
        _      <- Task(writer.run(df))
      } yield ()

      mockDb(url = url, dbConfig = dbConf)(query = query) {
        whenReady(program)(_ mustBe Right(()))
      }
    }
  }

  "SparkDbDataFrameWriter#update" must {
    "return unit" in withSparkSession { implicit spark => _ =>
      import spark.implicits._
      val program = for {
        df     <- Task((Dummy(1) :: Dummy(2) :: Nil).toDF())
        insert <- Task(SparkDataFrameWriter.DatabaseInsert(url, properties)(TableName("sparkDbDataFrameWriter")))
        update <- Task(SparkDataFrameWriter.DatabaseUpdate(url, properties)(TableName("sparkDbDataFrameWriter")))
        _      <- Task(insert.run(df)) *> Task(update.run(df))
        reader  = SparkDataFrameReader.DatabaseReader(url, properties) {
          SqlQuery("(SELECT * FROM sparkDbDataFrameWriter) as q1")
        }
      } yield reader.run.as[Dummy]

      mockDb(url = url, dbConfig = dbConf)(query = query) {
        whenReady(program) {
          case Left(_)   => fail()
          case Right(ds) => ds.collect().toList.sorted mustBe List(Dummy(1), Dummy(2)).sorted
        }
      }
    }
  }

}
