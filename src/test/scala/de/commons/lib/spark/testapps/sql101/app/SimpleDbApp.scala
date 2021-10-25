package de.commons.lib.spark.testapps.sql101.app

import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.{SparkDbDataFrameReader, SparkDataFrameWriter}
import de.commons.lib.spark.runnable.SparkIO
import de.commons.lib.spark.testapps.sql101.app.logic.services.DbService
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import org.apache.spark.sql.functions.{col, udf}
import zio.{ExitCode, URIO, ZIO}

import java.util.UUID

/**
 * NOTICE: check db connection before you run this app!!!
 *
 * ### Local MySql DB Deployment ###
 *
 * docker run --name budni_dev_local_mysql -p 3306:3306 \
 * -e MYSQL_ROOT_PASSWORD=password -d mysql --default-authentication-plugin=mysql_native_password
 * docker exec -it <container-id> bash -l
 *
 * 1 pen & Edit /etc/my.cnf or /etc/mysql/my.cnf, depending on your distro.
 * 2 Add skip-grant-tables under [mysqld]
 * 3 Restart Mysql
 * 4 You should be able to login to mysql now using the below command mysql -u root -p
 * 5 Run mysql> flush privileges;
 * 6 Set new password by ALTER USER 'root' IDENTIFIED BY 'NewPassword';
 * 7 Go back to /etc/my.cnf and remove/comment skip-grant-tables
 * 8 Restart Mysql
 * 9 Now you will be able to login with the new password mysql -u root -p
 *
 * https://medium.com/tech-learn-share/docker-mysql-access-denied-for-user-172-17-0-1-using-password-yes-c5eadad582d3
 *
 * #################################
 */
private[testapps] object SimpleDbApp extends zio.App with AppConfig {

  private val env = new SparkEnvironment(configuration, logger) with SparkDbDataFrameReader with SparkDataFrameWriter

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    SparkIO[SparkEnvironment, SparkDbDataFrameReader with SparkDataFrameWriter, Unit](program1 *> program2)
      .run
      .provide(env)
      .exitCode

  private val program1: ZIO[SparkEnvironment with SparkDbDataFrameReader with SparkDataFrameWriter, Throwable, Unit] =
    for {
      _ <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader with SparkDataFrameWriter]
      dbService = new DbService(url, properties)

      _ <- dbService.getAgents.map(_.show())
      _ <- dbService.getCustomers.map(_.show())
      _ <- dbService.getOrders.map(_.show())
      _ <- dbService.getAgentsStatistics.map(_.show())
    } yield ()

  private val program2: ZIO[SparkEnvironment with SparkDbDataFrameReader with SparkDataFrameWriter, Throwable, Unit] =
    for {
      _         <- ZIO.environment[SparkEnvironment with SparkDbDataFrameReader with SparkDataFrameWriter]
      dbService  = new DbService(url, properties)
      agents0   <- dbService.getAgents.map { df =>
        import df.sparkSession.implicits._
        val f = udf((_: String) => UUID.randomUUID().toString.take(5))
        df.withColumn(colName = "agentCode", f(col(colName = "agentCode"))).as[Agent]
      }
      _         <- dbService.insertAgents(agents0)
      agents1   <- dbService.getAgents
    } yield agents1.show()

}
