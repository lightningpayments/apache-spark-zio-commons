package de.commons.lib.spark.testapps.sql101.app

import de.commons.lib.spark.SparkRunnable.SparkRZIO
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.{SparkDataFrameWriter, SparkDbDataFrameReader}
import de.commons.lib.spark.testapps.sql101.app.logic.services.DbService
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import zio.{ExitCode, Task, URIO, ZIO}

import java.util.UUID
import scala.language.postfixOps

private[testapps] object SimpleDbApp extends zio.App with AppConfig {

  private type R = SparkDbDataFrameReader with SparkDataFrameWriter
  private val env = new SparkEnvironment(configuration, logger) with SparkDbDataFrameReader with SparkDataFrameWriter

  private val dbService = new DbService(url, properties)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    SparkRZIO[SparkEnvironment, R, Unit](program1 >>= program2).run.provide(env).exitCode

  private val program1: ZIO[SparkEnvironment with R, Throwable, DataFrame] =
    for {
      agents <- dbService.getAgents
      _      <- Task(agents.show)
      _      <- dbService.getCustomers.map(_.show())
      _      <- dbService.getOrders.map(_.show())
      _      <- dbService.getAgentsStatistics.map(_.show())
    } yield agents

  private val program2: DataFrame => ZIO[SparkEnvironment with R, Throwable, Unit] = df => {
    import df.sparkSession.implicits._

    val agentCode = "agentCode"
    val takeRandomAgentCode = udf((_: String) => UUID.randomUUID().toString.take(5))
    val agents = Task(df.withColumn(agentCode, takeRandomAgentCode(col(agentCode))).as[Agent].sort(agentCode))

    (agents >>= dbService.insertAgents) *> dbService.getAgents.map(_.show())
  }

}
