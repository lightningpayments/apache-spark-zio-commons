package de.commons.lib.spark.testapps

import de.commons.lib.spark.environments.Spark
import org.apache.spark.sql.SparkSession
import zio.{ExitCode, Has, Task, URIO, ZEnv, ZIO, ZLayer}

private[testapps] object SimpleZIOApp extends zio.App with AppConfig {

  private type HasRandom = Has[RandomNumberEnv]

  private trait RandomNumberEnv {
    val randomMathGen: Task[Double] = Task(math.random())
  }

  private val randomLayer = ZLayer.succeed(new RandomNumberEnv {
    override val randomMathGen: Task[Double] = super.randomMathGen
  })

  private final case class Pi(value: Double) extends AnyVal

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val io = for {
      sparkRT <- Spark.apply.provideLayer(Spark.live)
      random   = ZIO.accessM[Has[RandomNumberEnv]](_.get.randomMathGen).provideLayer(randomLayer)
      _       <- sparkRT.sparkM.flatMap(program(_, random))
    } yield ()

    io.exitCode
  }

  // scalastyle:off
  private def program(implicit spark: SparkSession, random: Task[Double]): Task[Unit] =
    ZIO.tupled(pi, pi, pi).map {
      case (pi1, pi2, pi3) => println(s"$pi1 $pi2 $pi3")
    }
  // scalastyle:on

  private def pi(implicit spark: SparkSession, random: Task[Double]): Task[Pi] = {
    val predicates = spark.sparkContext.parallelize(1 to 100).toLocalIterator.toList.map { _ =>
      ZIO.tupled(random, random).map {
        case (x, y) =>
          x * x + y * y < 1
      }
    }
    ZIO.collectAll(predicates).map(cons => Pi(4.0 * cons.count(identity) / 100.0))
  }

}
