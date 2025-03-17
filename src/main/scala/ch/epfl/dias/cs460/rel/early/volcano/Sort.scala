package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{Elem, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters.*
import scala.util.Sorting
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

  private object LocalOrder extends Ordering[Tuple] {
    def compare(x: Tuple, y: Tuple): Int =
      collation.getFieldCollations.asScala.map(
        fieldCollation =>
          val index = fieldCollation.getFieldIndex
          (x(index), y(index)) match
            case (x: Comparable[_], y: Comparable[_]) =>
              (if fieldCollation.direction.isDescending then -1 else 1)
              * RelFieldCollation.compare(x, y, fieldCollation.nullDirection.nullComparison)
            case _ => throw IllegalArgumentException("🚩 Cannot sort on fields that are not comparable.")
      ).foldLeft(0)((comparisonResult, accumulator) => if accumulator != 0 then accumulator else comparisonResult)
  }

  private var internalArray = Array.empty[Tuple]
  private var index = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    input.open()
    internalArray = input.drop(offset.getOrElse(0)).toArray
    Sorting.stableSort(internalArray)(LocalOrder)
    index = 0

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if index < fetch.getOrElse(internalArray.length) then
      index += 1
      Some(internalArray(index - 1))
    else
      None

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    index = 0
    internalArray = Array.empty[Tuple]
    input.close()
}
