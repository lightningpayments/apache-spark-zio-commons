package de.commons.lib.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import play.api.Configuration

class SparkSessionLoaderSpec extends TestSpec {

  "SparkSessionLoader#getSpark" must {
    "return a sparkSession instance" in {
      val configuration: Configuration = Configuration(ConfigFactory.parseString(
        """
          |spark {
          |  appName = "SparkSessionLoaderApp"
          |  master = "local[*]"
          |  config {}
          |}
          |""".stripMargin))
      new SparkSessionLoader(configuration).getSpark mustBe a[SparkSession]
    }
    "return a sparkSession instance - minimal case" in {
      val configuration: Configuration = Configuration(ConfigFactory.parseString(
        """
          |spark {}
          |""".stripMargin))
      new SparkSessionLoader(configuration).getSpark mustBe a[SparkSession]
    }
  }

}
