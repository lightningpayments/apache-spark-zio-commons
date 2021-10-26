package de.commons.lib.spark.util

import cats.{Applicative, Monad}

import scala.language.higherKinds

object MonadControl {

  /**
   * replaces absent values inside [[Monad]]s. This is analogue to Option[T].getOrElse() but within the monadic context.
   */
  def foldM[F[_]: Monad, A](ffa: F[Option[A]])(orElse: => F[A]): F[A] = Monad[F].flatMap[Option[A], A](ffa) {
    case Some(a) => Monad[F].pure(a)
    case None    => orElse
  }

  /**
   * purifies Some[_] optional value or uses a given applicative value.
   */
  def optionLiftMOrElseM[F[_]: Applicative, A](option: Option[A])(orElse: => F[A]): F[A] = option match {
    case Some(a) => Applicative[F].pure(a)
    case None    => orElse
  }

  /**
   * If a given optional provides Some[_] value, then the value is applied to an Applicative[_] function fa.
   * otherwise, the Applicative[_] is purely initialized with None.
   */
  def optionLiftM[F[_], A, B](
    option: Option[A])(
    f: A => F[B])(
    implicit A: Applicative[F]
  ): F[Option[B]] = option match {
    case Some(a) => A.fmap[B, Option[B]](f(a))(Some(_))
    case None    => A.pure(None)
  }

  implicit class RichOption[A](option: Option[A]) {
    def liftMOrElseM[F[_]](orElse: => F[A])(implicit A: Applicative[F]): F[A] =
      MonadControl.optionLiftMOrElseM(option)(orElse)
    def liftM[F[_], B](f: A => F[B])(implicit A: Applicative[F]): F[Option[B]] =
      MonadControl.optionLiftM(option)(f)
  }
}
