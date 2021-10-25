package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.{SqlQuery, TableName}
import org.apache.spark.sql.{Encoder, Encoders}
import zio.{Task, ZIO}

class SparkDataFrameWriterSpec extends TestSpec with SparkTestSupport with MockDbTestSupport {

  private case class Dummy(id: Int)

  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]
  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val query: String = "CREATE TABLE IF NOT EXISTS sparkDbDataFrameWriter (id int);"

  "SparkDbDataFrameWriter#insert" must {
    "return unit" in withSparkSession { spark => _ =>
      val program = ZIO.environment[SparkDataFrameWriter].flatMap { writer =>
        import spark.implicits._

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

  "SparkDbDataFrameWriter#update" must {
    "return unit" in withSparkSession { spark => _ =>
      val program = ZIO.environment[SparkDataFrameWriter with SparkDbDataFrameReader].flatMap { env =>
        import spark.implicits._

        for {
          df <- Task((Dummy(1) :: Dummy(2) :: Nil).toDF())

          _  <- Task(env.insert(url, properties)(df, TableName("sparkDbDataFrameWriter")))
          _  <- Task(env.update(url, properties)(df, TableName("sparkDbDataFrameWriter")))

          q   = SqlQuery("(SELECT * FROM sparkDbDataFrameWriter) as q1")
          ds  = env.reader(spark)(url, properties)(q).as[Dummy]
        } yield ds
      }

      mockDb(url = url, dbConfig = dbConf)(query = query) {
        whenReady(program.provide(new SparkDataFrameWriter with SparkDbDataFrameReader)) {
          case Left(_)   => fail()
          case Right(ds) => ds.collect().toList mustEqual List(Dummy(1), Dummy(2))
        }
      }
    }
  }

}
