package de.commons.lib.spark.testapps

import de.commons.lib.spark.SparkRunnable.SparkRZIO
import de.commons.lib.spark.environments.SparkR._
import zio.{ExitCode, Task, URIO, ZEnv, ZIO}

private[testapps] object SimpleZIOApp extends zio.App with AppConfig {

  private type R = SparkEnvironment with RandomNumberEnv

  private val env = new SparkEnvironment(configuration, logger) with RandomNumberEnv

  trait RandomNumberEnv {
    val randomMathGen: Task[Double] = Task(math.random())
  }

  private final case class Pi(value: Double) extends AnyVal

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    SparkRZIO[R, Unit](io = program).run.provide(env).exitCode

  // scalastyle:off
  private val program: ZIO[R, Throwable, Unit] =
    ZIO.accessM[R](_ => ZIO.tupled(pi, pi, pi)).map {
      case (pi1, pi2, pi3) => println(s"$pi1 $pi2 $pi3")
    }
  // scalastyle:on

  private val pi: ZIO[R, Throwable, Pi] =
    for {
      spark      <- env.sparkM
      predicates  = spark.sparkContext.parallelize(1 to 100).toLocalIterator.toList.map { _ =>
        ZIO.tupled(env.randomMathGen, env.randomMathGen).map {
          case (x, y) =>
            x * x + y * y < 1
        }
      }
      sequenced  <- ZIO.collectAll(predicates)
      pi          = Pi(4.0 * sequenced.count(identity) / 100.0)
    } yield pi

}
