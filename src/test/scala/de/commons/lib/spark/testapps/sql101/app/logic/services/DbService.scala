package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.DataFrameWriter
import de.commons.lib.spark.environments.io.{SparkDataFrameWriter, SparkDbDataFrameReader}
import de.commons.lib.spark.models.SqlQuery
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, Dataset}
import zio.{URIO, ZIO}

private[sql101] final class DbService(url: String, properties: java.util.Properties) {

  private type ReaderR = SparkEnvironment with SparkDbDataFrameReader
  private type WriterR = SparkEnvironment with SparkDataFrameWriter

  private val readerIO: ZIO[ReaderR, Throwable, SqlQuery => DataFrame] =
    for {
      env   <- ZIO.environment[ReaderR]
      spark <- env.sparkM
    } yield env.reader(spark)(url, properties)(_)

  private val writerIO: URIO[WriterR, DataFrameWriter] =
    ZIO.environment[WriterR].map(_.insert(url, properties))

  // scalastyle:off
  def getAgents: ZIO[ReaderR, Throwable, DataFrame] =
    for {
      _  <- ZIO.environment[ReaderR].map(_.loggerM.map(_.debug("select all agents")))
      df <- readerIO.map(Agent.select).map { ds =>
        import ds.sparkSession.implicits._
        ds.withColumn(
          colName = "country",
          col     = when(
            condition = $"country".isNull || $"country".isin("null"),
            value     = lit(null).cast("string"))
        )
      }
    } yield df
  // scalastyle:on

  def insertAgents(ds: Dataset[Agent]): ZIO[WriterR, Throwable, Unit] =
    ZIO.environment[WriterR].map(_.loggerM.map(_.debug("insert agents"))) *>
    writerIO.map(Agent.insert(ds))

  def getCustomers: ZIO[ReaderR, Throwable, Dataset[Customer]] =
    ZIO.environment[ReaderR].map(_.loggerM.map(_.debug("select all customers"))) *> readerIO.map(Customer.select)

  def getOrders: ZIO[ReaderR, Throwable, Dataset[Order]] =
    ZIO.environment[ReaderR].map(_.loggerM.map(_.debug("select all orders"))) *> readerIO.map(Order.select)

  def getAgentsStatistics: ZIO[ReaderR, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    ZIO.environment[ReaderR].map(_.loggerM.map(_.debug("join agents via order via customers"))) *>
    readerIO.map(JoinedAgentsOrdersCustomers.select)

}
