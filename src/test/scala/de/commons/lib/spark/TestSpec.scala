package de.commons.lib.spark

import ch.qos.logback.classic.LoggerContext
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterEach, OptionValues}
import org.scalatestplus.play.WsScalaTestClient
import org.slf4j.LoggerFactory
import zio.Task

import scala.language.higherKinds
import scala.util.Try

class TestSpec
  extends AnyWordSpec
  with Matchers
  with OptionValues
  with WsScalaTestClient
  with ScalaFutures
  with BeforeAndAfterEach {

  /**
   * The timeout to wait for the future before declaring it as failed.
   * Note: Seconds
   */
  protected val futurePatienceTimeout: Int = 5

  /**
   * The interval for the checking whether the future has completed
   * Note: Milliseconds
   */
  protected val futurePatienceInterval: Int = 20

  /**
   * must be implemented to test functions outer spark scope
   */
  implicit val outerScope: Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  /**
   * To prevent tests to fail because of future timeout issues, we override the default behavior.
   * Note: it is an implicit Option for the ScalaFutures method 'whenReady'
   */
  implicit val defaultPatience: PatienceConfig = PatienceConfig(
    timeout = Span(futurePatienceTimeout, Seconds),
    interval = Span(futurePatienceInterval, Millis)
  )

  override def beforeEach(): Unit = {
    try super.beforeEach()
    finally {
      // shutdown logger
      Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
        ctx.stop()
        org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
      }
    }
  }

  /**
   * ZIO default runtime only for testing.
   */
  lazy implicit val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default

  def whenReady[T, U](task: Task[T])(f: Either[Throwable, T] => U): U =
    runtime.unsafeRun[Throwable, U](task.either.map(f))
}
