package ch.epfl.dias.cs460.rel.early.volcano.late

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{LateTuple, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  *
  * Note that this in a Late materialization operator,
  * so it receives [[ch.epfl.dias.cs460.helpers.rel.RelOperator.LateTuple]] and
  * produces [[ch.epfl.dias.cs460.helpers.rel.RelOperator.LateTuple]]
  *
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator]]
  */
class LateProject protected (
                             input: ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator,
                             projects: java.util.List[_ <: RexNode],
                             rowType: RelDataType
                           ) extends skeleton.Project[
  ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
](input, projects, rowType)
  with ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    input.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] =
    input.next() map { case LateTuple(vid, value) => LateTuple(vid, evaluator(value)) }

  /**
    * @inheritdoc
    */
  override def close(): Unit =
    input.close()
}
