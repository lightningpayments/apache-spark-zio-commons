package de.commons.lib.spark.testapps

import de.commons.lib.spark
import de.commons.lib.spark.environments.SparkR._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import zio.{ExitCode, Task, URIO, ZEnv, ZIO}

private[testapps] object SimpleZIOApp extends zio.App with AppConfig {

  type R = SparkEnvironment with RandomNumberEnv

  trait RandomNumberEnv {
    val randomMathGen: Task[Double] = Task(math.random())
  }

  private final case class ArticleId(value: Long) extends AnyVal
  private object ArticleId {
    implicit val encoders: Encoder[ArticleId] = Encoders.product[ArticleId]
    def from(rdd: RDD[Int])(implicit spark: SparkSession): Dataset[ArticleId] = {
      import spark.implicits._
      rdd.toDS.as[ArticleId]
    }
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO
      .environment[R]
      .>>>(spark.SparkIO[SparkEnvironment, RandomNumberEnv, Unit](io = program.map(_.show)).run)
      .provide(new SparkEnvironment(configuration, logger) with RandomNumberEnv)
      .exitCode

  private val program: ZIO[R, Throwable, Dataset[Long]] =
    ZIO.accessM[R] { env =>
      for {
        ds  <- env.sparkM.flatMap { implicit spark =>
          import ArticleId._
          import spark.implicits._

          (for {
            a <- pi.map(ArticleId.from)
            b <- pi.map(ArticleId.from)
            c <- pi.map(ArticleId.from)
            d <- Task.succeed(ArticleId(42L)).map(n => spark.createDataset(n :: Nil))
          } yield a union b union c union d)
            .map(_.agg(sum(columnName = "value")).first().getLong(0) :: Nil)
            .map(spark.createDataset(_))
        }
      } yield ds
    }

  private val pi: ZIO[R, Throwable, RDD[Int]] =
    ZIO.accessM[R] { env =>
      for {
        x <- env.randomMathGen
        y <- env.randomMathGen
        r <- env.sparkM.map(_.sparkContext.parallelize(1 to 10).filter { _ => x * x + y * y < 1 })
      } yield r
    }

}
