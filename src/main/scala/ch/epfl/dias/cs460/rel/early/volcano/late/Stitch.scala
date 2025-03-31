package ch.epfl.dias.cs460.rel.early.volcano.late

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{LateTuple, NilLateTuple}

import scala.collection.mutable

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
   * This is terrible practice because the VID type is defined somewhere else, but the provided skeleton
   * leaves us no choice.
   */
  private type VID = Long

  private val rightHashMap = mutable.HashMap.empty[VID, Iterable[LateTuple]]
  private val outputBuffer = mutable.Queue.empty[LateTuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    rightHashMap.clear()
    rightHashMap ++= right groupBy (_.vid)
    outputBuffer.clear()
    left.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] =
    if outputBuffer.isEmpty then
      val matchingTupleBuffers = LazyList continually left.next() takeWhile (_.isDefined) map (_.get) map {
        leftTuple =>
          rightHashMap get leftTuple.vid map {
            for rightTuple <- _
              yield LateTuple(leftTuple.vid, leftTuple.value ++ rightTuple.value)
          }
      } collect { case Some(outputTuples) => outputTuples }
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
