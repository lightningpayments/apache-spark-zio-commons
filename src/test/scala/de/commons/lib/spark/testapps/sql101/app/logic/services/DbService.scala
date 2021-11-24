package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.services.Spark.HasSpark
import de.commons.lib.spark.services.SparkT
import de.commons.lib.spark.io.SparkDataFrameReader.{DataFrameQueryReader, DatabaseReader}
import de.commons.lib.spark.io.SparkDataFrameWriter.{DataFrameDatabaseWriter, DatabaseInsert}
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.Dataset
import zio.ZIO

private[sql101] final class DbService(
    url: String,
    properties: java.util.Properties,
    sparkIO: ZIO[HasSpark, Throwable, SparkT]
) {

  private val databaseReader: DataFrameQueryReader = DatabaseReader(url, properties)
  private val databaseInsert: DataFrameDatabaseWriter = DatabaseInsert(url, properties)

  def getAgents: ZIO[HasSpark, Throwable, Dataset[Agent]] =
    for {
      env <- sparkIO
      _   <- env.loggerM.map(_.debug("select all agents"))
      ds  <- env.sparkM.map(implicit spark => Agent.select(databaseReader))
    } yield ds

  def insertAgents(ds: Dataset[Agent]): ZIO[HasSpark, Throwable, Unit] =
    sparkIO
      .map(_.loggerM.map(_.debug("insert agents")))
      .map(_ => Agent.insert(ds)(databaseInsert))

  def getCustomers: ZIO[HasSpark, Throwable, Dataset[Customer]] =
    for {
      env <- sparkIO
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- env.sparkM.map(implicit spark => Customer.select(databaseReader))
    } yield ds

  def getOrders: ZIO[HasSpark, Throwable, Dataset[Order]] =
    for {
      env <- sparkIO
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- env.sparkM.map(implicit spark => Order.select(databaseReader))
    } yield ds

  def getAgentsStatistics: ZIO[HasSpark, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- sparkIO
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- env.sparkM.map(implicit spark => JoinedAgentsOrdersCustomers.select(databaseReader))
    } yield ds

}
