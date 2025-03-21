package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs460.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  private def groupByKey(t: Tuple): Iterable[Elem] =
    groupSet.asScala.map(t(_))

  private val groups = mutable.Map.empty[Iterable[Elem], Iterable[Tuple]]

  private val emptyAggregatedTuple = aggCalls map (_.emptyValue)

  private val hasEmptyInput = input.isEmpty

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    groups.clear()
    if hasEmptyInput then
      groups += ((Iterable.empty[Elem], Iterable.empty[Tuple]))
    else
      groups ++= input.groupBy(tuple => groupSet.asScala.map(tuple(_)))


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    groups.keysIterator.nextOption map { key =>
      val tuples = (groups remove key).get
      tuples map {
        tuple => aggCalls map (_ getArgument tuple)
      } reduceOption { (t1, t2) =>
        for (call, idx) <- aggCalls.zipWithIndex
          yield call reduce (t1(idx), t2(idx))
      } map (key.toIndexedSeq ++ _) getOrElse emptyAggregatedTuple
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    groups.clear()
}
