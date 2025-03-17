package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  private var currentLeft = Option.empty[Tuple]

  private def mergeTuples(left: Tuple, right: Tuple): Option[Tuple] =
    val keys = getLeftKeys lazyZip getRightKeys
    if keys.forall (left(_) equals right(_)) then
      Some(left ++ right.zipWithIndex.collect(
        (elem, idx) => Option.when (!(getRightKeys contains idx)) (elem)
      ))
    else
      None

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    left.open()
    right.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    currentLeft match
      case None =>
        // Pick the next tuple from the left relation
        left.next() match
          case None => None
          case nextLeft: Some[Tuple] =>
            currentLeft = nextLeft
            next()
      case Some(leftTuple) =>
        // Pick the next tuple from the right relation
        right.next() match
          case None =>
            currentLeft = None
            next()
          case Some(rightTuple) =>
            mergeTuples(leftTuple, rightTuple) orElse next()

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    left.close()
    right.close()
}
