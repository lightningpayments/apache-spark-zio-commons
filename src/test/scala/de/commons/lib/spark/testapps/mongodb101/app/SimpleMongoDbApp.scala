package de.commons.lib.spark.testapps.mongodb101.app

import de.commons.lib.spark.SparkRunnable.RunnableSparkRT
import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import org.apache.spark.sql.Dataset
import zio.{ExitCode, URIO, ZIO}

private[testapps] object SimpleMongoDbApp extends zio.App with AppConfig {

  private val program: ZIO[SparkR, Throwable, Dataset[Agent]] =
    ZIO.environment[SparkR].flatMap { env =>
      val reader: DataFrameMongoDbReader = SparkDataFrameReader.MongoDbReader(properties)
      env.sparkM.map(implicit spark => Agent.getAll(reader))
    }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    RunnableSparkRT[SparkR, Unit](program.map(_.show))
      .run
      .provide(new SparkR(configuration, logger))
      .exitCode

}
