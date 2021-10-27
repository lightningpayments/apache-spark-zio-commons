package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.DataFrameWriter
import de.commons.lib.spark.environments.io.{SparkDbDataFrameReader, SparkDataFrameWriter}
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

  def getAgents: ZIO[ReaderR, Throwable, DataFrame] =
    for {
      env <- ZIO.environment[ReaderR]
      _   <- env.loggerM.map(_.debug("select all agents"))
      df  <- readerIO.map(Agent.select).map { ds =>
        import ds.sparkSession.implicits._
        ds.withColumn("country", when($"country".isNull, lit("null")))
      }
    } yield df

  def insertAgents(ds: Dataset[Agent]): ZIO[WriterR, Throwable, Dataset[Agent]] =
    for {
      env <- ZIO.environment[WriterR]
      _   <- env.loggerM.map(_.debug("insert agents"))
      _   <- writerIO.map(Agent.insert(ds))
    } yield ds

  def getCustomers: ZIO[ReaderR, Throwable, Dataset[Customer]] =
    for {
      env <- ZIO.environment[ReaderR]
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- readerIO.map(Customer.select)
    } yield ds

  def getOrders: ZIO[ReaderR, Throwable, Dataset[Order]] =
    for {
      env <- ZIO.environment[ReaderR]
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- readerIO.map(Order.select)
    } yield ds

  def getAgentsStatistics: ZIO[ReaderR, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- ZIO.environment[ReaderR]
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- readerIO.map(JoinedAgentsOrdersCustomers.select)
    } yield ds

}
