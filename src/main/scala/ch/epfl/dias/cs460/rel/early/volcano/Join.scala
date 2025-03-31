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

  private val rightHashMap = mutable.HashMap.empty[Tuple, Iterable[Tuple]]
  private val outputBuffer = mutable.Queue.empty[Tuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    rightHashMap.clear()
    rightHashMap ++= (
      right groupBy { rightTuple => getRightKeys map (rightTuple(_)) }
    )
    outputBuffer.clear()
    left.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if outputBuffer.isEmpty then
      // Find the next left tuple that matches some right tuple
      val matchingTupleBuffers = LazyList continually left.next() takeWhile (_.isDefined) map (_.get) map { leftTuple =>
        val key = getLeftKeys map (leftTuple(_))
        rightHashMap get key map {
          for rightTuple <- _
            yield leftTuple ++ rightTuple
        }
      } collect { case Some(matchingTuples) => matchingTuples }
      matchingTupleBuffers.headOption flatMap { matchingTuples =>
        outputBuffer ++= matchingTuples
        next()
      }
    else
      Some(outputBuffer.dequeue())

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    left.close()
    outputBuffer.clear()
    rightHashMap.clear()
}
