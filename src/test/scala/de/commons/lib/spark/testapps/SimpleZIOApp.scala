package de.commons.lib.spark.testapps

import cats.Monoid
import cats.implicits._
import de.commons.lib.spark.environments.SparkR._
import de.commons.lib.spark.runnable.SparkIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import zio.{ExitCode, Task, URIO, ZEnv, ZIO}

private[testapps] object SimpleZIOApp extends zio.App with AppConfig {

  trait RandomNumberEnv {
    val randomMathGen: Task[Double] = Task(math.random())
  }

  private final case class ArticleId(value: Long) extends AnyVal
  private object ArticleId {
    implicit val encoders: Encoder[ArticleId] = Encoders.product[ArticleId]
    implicit def monoid(implicit sparkSession: SparkSession): Monoid[Dataset[ArticleId]] = {
      new Monoid[Dataset[ArticleId]] {
        override def empty: Dataset[ArticleId] = sparkSession.createDataset(Nil)
        override def combine(x: Dataset[ArticleId], y: Dataset[ArticleId]): Dataset[ArticleId] = {
          import sparkSession.implicits._
          (x.map(_.value) union y.map(_.value)).map(ArticleId(_))
        }
      }
    }
    def from(rdd: RDD[Int])(implicit spark: SparkSession): Dataset[ArticleId] = {
      import spark.implicits._
      rdd.toDS.as[ArticleId]
    }
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO
      .environment[SparkEnvironment with RandomNumberEnv]
      .>>>(SparkIO[SparkEnvironment, RandomNumberEnv, Unit](io = program.map(_.show)).run)
      .provide(new SparkEnvironment(configuration, logger) with RandomNumberEnv)
      .exitCode

  private val program: ZIO[SparkEnvironment with RandomNumberEnv, Throwable, Dataset[Long]] =
    ZIO.accessM[SparkEnvironment with RandomNumberEnv] { env =>
      for {
        ds  <- env.sparkM.flatMap { implicit spark =>
          import ArticleId._
          import spark.implicits._

          (for {
            a <- pi.map(ArticleId.from)
            b <- pi.map(ArticleId.from)
            c <- pi.map(ArticleId.from)
            d <- Task.succeed(ArticleId(42L)).map(n => spark.createDataset(n :: Nil))
          } yield a |+| b |+| c |+| d)
            .map(_.agg(sum(columnName = "value")).first().getLong(0) :: Nil)
            .map(spark.createDataset(_))
        }
      } yield ds
    }

  private val pi: ZIO[SparkEnvironment with RandomNumberEnv, Throwable, RDD[Int]] =
    ZIO.accessM[SparkEnvironment with RandomNumberEnv] { env =>
      for {
        x <- env.randomMathGen
        y <- env.randomMathGen
        r <- env.sparkM.map(_.sparkContext.parallelize(1 to 10).filter { _ => x * x + y * y < 1 })
      } yield r
    }

}
