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
  def optionLiftInOrElse[F[_]: Applicative, A](maybe: Option[A])(orElse: => F[A]): F[A] = maybe match {
    case Some(a) => Applicative[F].pure(a)
    case None    => orElse
  }

  /**
   * If a given optional provides Some[_] value, then the value is applied to an Applicative[_] function fa.
   * otherwise, the Applicative[_] is purely initialized with None.
   */
  def optionLiftIn[F[_]: Applicative, A, B](maybe: Option[A])(f: A => F[B]): F[Option[B]] = maybe match {
    case Some(a) => Applicative[F].map[B, Option[B]](f(a))(Some(_))
    case None    => Applicative[F].pure(None)
  }

}
