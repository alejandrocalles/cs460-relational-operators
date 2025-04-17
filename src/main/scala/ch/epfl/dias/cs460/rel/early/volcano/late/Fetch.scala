package ch.epfl.dias.cs460.rel.early.volcano.late

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.builder.skeleton.logical.LogicalFetch
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
import ch.epfl.dias.cs460.helpers.store.late.LateStandaloneColumnStore
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters.CollectionHasAsScala

class Fetch protected (
                              input: ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator,
                              fetchType: RelDataType,
                              column: LateStandaloneColumnStore,
                              projects: Option[java.util.List[_ <: RexNode]],
                            ) extends skeleton.Fetch[
  ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
](input, fetchType, column, projects)
  with ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator {
  private lazy val evaluator: Tuple => Tuple =
    projects map { ps => eval(ps.asScala.toIndexedSeq, fetchType) } getOrElse (x => x)

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    input.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] =
    input.next() map { inputTuple =>
      val (prefix, suffix) = inputTuple.value splitAt column.getColumnIndex
      val infix = IndexedSeq(column.getElement(inputTuple.vid).get)
      LateTuple(inputTuple.vid, prefix ++ evaluator(infix) ++ suffix)
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    input.close()
}

object Fetch {
  def create(
              input: Operator,
              fetchType: RelDataType,
              column: LateStandaloneColumnStore,
              projects: Option[java.util.List[_ <: RexNode]]
            ): LogicalFetch = {
    new Fetch(input, fetchType, column, projects)
  }
}

