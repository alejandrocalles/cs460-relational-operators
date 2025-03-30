package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  private val rightBuffer = mutable.Queue.empty[Tuple]

  private def mergeTuples(leftTuple: Tuple, rightTuple: Tuple): Option[Tuple] =
    Option.when (
      (getLeftKeys lazyZip getRightKeys) forall (leftTuple(_) equals rightTuple(_))
    ) (leftTuple ++ rightTuple)

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    rightBuffer.clear()
    left.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if rightBuffer.isEmpty then
      // Get the next left tuple and fill the right buffer
      left.next() flatMap { leftTuple =>
        rightBuffer ++= (right map (mergeTuples(leftTuple, _)) collect { case Some(tuple) => tuple })
        next()
      }
    else
      Some(rightBuffer.dequeue())

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    left.close()
    rightBuffer.clear()
}
