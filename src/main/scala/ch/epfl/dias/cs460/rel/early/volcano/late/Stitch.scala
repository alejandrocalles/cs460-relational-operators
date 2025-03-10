package ch.epfl.dias.cs460.rel.early.volcano.late

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{LateTuple, NilLateTuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                              left: ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator,
                              right: ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
                            ) extends skeleton.Stitch[
  ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator {

  /**
    * @inheritdoc
    */
  override def open(): Unit = ???

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = ???

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
