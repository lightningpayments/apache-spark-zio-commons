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

  def getAgents: ZIO[SparkEnvironment with SparkDbDataFrameReader, Throwable, DataFrame] =
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader]
      _   <- env.loggerM.map(_.debug("select all agents"))
      df  <- readerM.map(Agent.select).map { ds =>
        import ds.sparkSession.implicits._
        ds.withColumn("country", when($"country".isNull, lit("null")))
      }
    } yield df

  def insertAgents(ds: Dataset[Agent]): ZIO[SparkEnvironment with SparkDataFrameWriter, Throwable, Dataset[Agent]] =
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDataFrameWriter]
      _   <- env.loggerM.map(_.debug("insert agents"))
      _   <- writerM.map(Agent.insert(ds))
    } yield ds

  def getCustomers: ZIO[SparkEnvironment with SparkDbDataFrameReader, Throwable, Dataset[Customer]] = {
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader]
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- readerM.map(Customer.select)
    } yield ds
  }

  def getOrders: ZIO[SparkEnvironment with SparkDbDataFrameReader, Throwable, Dataset[Order]] =
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader]
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- readerM.map(Order.select)
    } yield ds

  def getAgentsStatistics:
  ZIO[SparkEnvironment with SparkDbDataFrameReader, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader]
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- readerM.map(JoinedAgentsOrdersCustomers.select)
    } yield ds

  private val readerM: ZIO[SparkEnvironment with SparkDbDataFrameReader, Throwable, SqlQuery => DataFrame] =
    for {
      env    <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader]
      spark  <- env.sparkM
      reader  = env.reader(spark)(url, properties)(_)
    } yield reader

  private val writerM: URIO[SparkEnvironment with SparkDataFrameWriter, DataFrameWriter] =
    ZIO.environment[SparkEnvironment with SparkDataFrameWriter].map(_.insert(url, properties))

}
