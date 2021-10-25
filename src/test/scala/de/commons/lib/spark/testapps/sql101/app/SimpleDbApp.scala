package de.commons.lib.spark.testapps.sql101.app

import de.commons.lib.spark.SparkIO
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.{SparkDataFrameWriter, SparkDbDataFrameReader}
import de.commons.lib.spark.testapps.sql101.app.logic.services.DbService
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import zio.{ExitCode, Task, URIO, ZIO}

import java.util.UUID
import scala.language.postfixOps

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

  private type R = SparkDbDataFrameReader with SparkDataFrameWriter

  private val env = new SparkEnvironment(configuration, logger) with SparkDbDataFrameReader with SparkDataFrameWriter

  private val dbService = new DbService(url, properties)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    SparkIO[SparkEnvironment, R, Unit](program1 >>= program2)
      .run
      .provide(env)
      .exitCode

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
    val takeRandomAgentCode = udf((_: String) => UUID.randomUUID().toString.take(5))

    for {
      agents0 <- Task(df.withColumn(colName = "agentCode", takeRandomAgentCode(col(colName = "agentCode"))).as[Agent])
      _       <- dbService.insertAgents(agents0)
      agents1 <- dbService.getAgents
    } yield agents1.show()
  }

}
