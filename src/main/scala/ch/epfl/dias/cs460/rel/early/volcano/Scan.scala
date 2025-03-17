package ch.epfl.dias.cs460.rel.early.volcano

import ch.epfl.dias.cs460.helpers.builder.skeleton
import ch.epfl.dias.cs460.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs460.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs460.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs460.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var prog = getRowType.getFieldList.asScala.map(_ => 0)
  private var index = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit =
    index = 0

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    scannable match
      case r: RowStore => 
        if index < scannable.getRowCount then
          index += 1
          Some(r.getRow(index - 1))
        else
          None
      case _ => throw NotImplementedError("🚩 Stores other than RowStore are not yet implemented.")
    

  /**
    * @inheritdoc
    */
  override def close(): Unit = ()
}
