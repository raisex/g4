package com.wavesplatform.lang.v1.task

import cats.data.Kleisli
import monix.eval.Coeval
import cats.implicits._

trait TaskMFunctions {

  def pure[S, E, R](x: R): TaskM[S, E, R] = TaskM(_ => Coeval.pure(x.asRight))

  def raiseError[S, E, R](e: E): TaskM[S, E, R] = TaskM(_ => Coeval.pure(e.asLeft))

  def get[S, E]: TaskM[S, E, S] = TaskM(s => Coeval.pure(s.asRight))

  def set[S, E](s: S): TaskM[S, E, Unit] =
    TaskM.fromKleisli(Kleisli(ref => {
      ref.write(s).map(_.asRight)
    }))

  def inspect[S, E, A](f: S => A): TaskM[S, E, A] = get[S, E].map(f)

  def modify[S, E](f: S => S): TaskM[S, E, Unit] = get[S, E].flatMap(f andThen set)
}
