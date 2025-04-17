package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.jdk.CollectionConverters.*

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */
  private val relFieldCollations = LazyList.from(collation.getFieldCollations.asScala)

  private object TupleOrder extends Ordering[Tuple] {
    /**
     * Compares `x` to `y` according to this relation's `collation`.
     * @param x The first tuple.
     * @param y The second tuple.
     * @return A positive integer if `x > y`, `0` if they are equal, and a negative integer if `x < y`.
     */
    def compare(x: Tuple, y: Tuple): Int =
      def compareFields(x: Tuple, y: Tuple, fieldCollation: RelFieldCollation): Int =
        val index = fieldCollation.getFieldIndex
        (x(index), y(index)) match
          case (x: Comparable[_], y: Comparable[_]) =>
            (if fieldCollation.direction.isDescending then -1 else 1)
              * RelFieldCollation.compare(x, y, fieldCollation.nullDirection.nullComparison)
          case _ => throw IllegalArgumentException("🚩 Cannot sort on fields that are not comparable.")
      relFieldCollations map (compareFields(x, y, _)) find (_ != 0) getOrElse 0
  }

  // A min heap (i.e. a priority queue with the reverse order) to sort the input
  private val queue = mutable.PriorityQueue.empty[Tuple](TupleOrder.reverse)
  private var index = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    input.open()
    queue.clear()
    queue addAll input.drop(offset getOrElse 0)
    index = 0

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    Option.when (queue.nonEmpty && (fetch forall (index < _))) {
      index += 1
      queue.dequeue()
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    index = 0
    queue.clear()
    input.close()
}
