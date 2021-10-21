package de.commons.lib.spark.util

import cats.Id
import cats.implicits._
import de.commons.lib.spark._
import zio.Task
import zio.interop.catz._

class MonadControlSpec extends TestSpec with SparkTestSupport {

  "MonadControl#foldM" must {
    "work for different kinds of Monads" in {
      MonadControl.foldM[List, Int](List(Some(1), None, Some(2)))(List(2, 3)) mustBe List(1, 2, 3, 2)
      MonadControl.foldM[Option, String](Some(Some("a")))(Some("orValue")) mustBe Some("a")
      MonadControl.foldM[Option, String](Some(None))(Some("orValue")) mustBe Some("orValue")
    }
    "work for Id" in {
      val withValue = MonadControl.foldM[Id, String](Some("foo"))("orValue")
      withValue mustBe "foo"
      val withoutValue = MonadControl.foldM[Id, String](None)("orValue")
      withoutValue mustBe "orValue"
    }
  }

  "MonadControl#pureOrElseA" must {
    "work for different kinds of Monads" in {
      MonadControl.pureOrElse[List, Int](Some(2))(List(3, 1)) mustBe List(2)
      MonadControl.pureOrElse[List, Int](None)(List(1, 3)) mustBe List(1, 3)

      MonadControl.pureOrElse[Option, Int](Some(1))(Some(2)) mustBe Some(1)
      MonadControl.pureOrElse[Option, Int](None)(Some(2)) mustBe Some(2)
    }
    "work for Id" in {
      val withValue = MonadControl.pureOrElse[Id, String](Some("foo"))("bar")
      withValue mustBe "foo"

      val withoutValue = MonadControl.pureOrElse[Id, String](None)("bar")
      withoutValue mustBe "bar"
    }
  }

  "MonadControl#optional" must {
    "work for different kinds of Applicatives" in {
      MonadControl.optional[List, String, String](Some("a")) {
        List(_, "b")
      } mustBe List(Some("a"), Some("b"))
      MonadControl.optional[List, String, String](None) {
        List(_, "b")
      } mustBe List(None)
      MonadControl.optional[Option, String, String](Some("a")) { x => Some(s"b$x") } mustBe Some(Some("ba"))
      MonadControl.optional[Option, String, String](None) { _ => Some("b") } mustBe Some(None)
    }
    "work for IO" in {
      val withValue = MonadControl.optional[Task, String, String](Some("foo"))(Task.succeed(_))
      whenReady(withValue)(_ mustBe Right(Some("foo")))

      val withoutValue = MonadControl.optional[Task, String, String](None)(Task.succeed(_))
      whenReady(withoutValue)(_ mustBe Right(None))
    }
  }

}

