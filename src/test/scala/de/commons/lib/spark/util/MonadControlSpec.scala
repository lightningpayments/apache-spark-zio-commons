package de.commons.lib.spark.util

import cats.Id
import cats.implicits._
import de.commons.lib.spark._
import zio.Task
import zio.interop.catz._
import MonadControl._

class MonadControlSpec extends TestSpec with SparkTestSupport {

  "MonadControl#foldM" must {
    "work for different kinds of Monads" in {
      MonadControl.foldM[List, Int](List(Some(1), None, Some(2)))(List(2, 3)) mustBe List(1, 2, 3, 2)
      MonadControl.foldM[Option, String](Some(Some("a")))(Some("orValue")) mustBe Some("a")
      MonadControl.foldM[Option, String](Some(None))(Some("orValue")) mustBe Some("orValue")

      whenReady(MonadControl.foldM[Task, String](Task(None))(Task.succeed("orValue")))(_ mustBe Right("orValue"))
    }
    "work for Id" in {
      val withValue = MonadControl.foldM[Id, String](Some("foo"))("orValue")
      withValue mustBe "foo"
      val withoutValue = MonadControl.foldM[Id, String](None)("orValue")
      withoutValue mustBe "orValue"
    }
  }

  "MonadControl#optionLiftMOrElse" must {
    "work for different kinds of Monads" in {
      MonadControl.optionLiftMOrElse[List, Int](Some(2))(List(3, 1)) mustBe List(2)
      MonadControl.optionLiftMOrElse[List, Int](None)(List(1, 3)) mustBe List(1, 3)
      MonadControl.optionLiftMOrElse[Option, Int](Some(1))(Some(2)) mustBe Some(1)
      MonadControl.optionLiftMOrElse[Option, Int](None)(Some(2)) mustBe Some(2)

      whenReady(MonadControl.optionLiftMOrElse[Task, Int](None)(Task.succeed(2)))(_ mustBe Right(2))
    }
    "convenience: work for different kinds of Monads" in {
      Some(2).liftMOrElse(List(3, 1)) mustBe List(2)
      none[Int].liftMOrElse[List](List(1, 3)) mustBe List(1, 3)

      Some(1).liftMOrElse[Option](Some(2)) mustBe Some(1)
      none[Int].liftMOrElse[Option](Some(2)) mustBe Some(2)
    }
    "work for Id" in {
      val withValue = MonadControl.optionLiftMOrElse[Id, String](Some("foo"))("bar")
      withValue mustBe "foo"

      val withoutValue = MonadControl.optionLiftMOrElse[Id, String](None)("bar")
      withoutValue mustBe "bar"
    }
  }

  "MonadControl#optionLiftM" must {
    "work for different kinds of Applicatives" in {
      MonadControl.optionLiftM[List, String, String](Some("a")) { List(_, "b") } mustBe List(Some("a"), Some("b"))
      MonadControl.optionLiftM[List, String, String](None) { List(_, "b") } mustBe List(None)
      MonadControl.optionLiftM[Option, String, String](Some("a")) { x => Some(s"b$x") } mustBe Some(Some("ba"))
      MonadControl.optionLiftM[Option, String, String](None) { _ => Some("b") } mustBe Some(None)

      whenReady(MonadControl.optionLiftM[Task, String, String](Some("a")) { _ => Task.succeed("b") })(
        _ mustBe Right(Some("b"))
      )
    }
    "convenience: work for different kinds of Applicatives" in {
      Some("a").liftM { List(_, "b") } mustBe List(Some("a"), Some("b"))
      none[String].liftM[List, String] { List(_, "b") } mustBe List(None)
      Some("a").liftM[Option, String] { x => Some(s"b$x") } mustBe Some(Some("ba"))
      none[String].liftM[Option, String] { _ => Some("b") } mustBe Some(None)

      whenReady(none[String].liftM[Task, String] { _ => Task.succeed("b") })(_ mustBe Right(None))
    }
    "work for Task" in {
      val withValue = MonadControl.optionLiftM[Task, String, String](Some("foo"))(Task.succeed(_))
      whenReady(withValue)(_ mustBe Right(Some("foo")))

      val withoutValue = MonadControl.optionLiftM[Task, String, String](None)(Task.succeed(_))
      whenReady(withoutValue)(_ mustBe Right(None))
    }
  }

}
