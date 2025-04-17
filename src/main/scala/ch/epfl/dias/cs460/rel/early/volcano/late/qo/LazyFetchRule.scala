package ch.epfl.dias.cs460.rel.early.volcano.late.qo

import ch.epfl.dias.cs460.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs460.helpers.qo.rules.skeleton.LazyFetchRuleSkeleton
import ch.epfl.dias.cs460.helpers.rel.late.volcano.naive.Operator
import ch.epfl.dias.cs460.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.{InvalidRelException, RelNode}
import org.apache.calcite.rex.{RexNode, RexUtil}

import scala.jdk.CollectionConverters.*


/**
  * RelRule (optimization rule) that finds an operator that stitches a new column
  * to the late materialized tuple and transforms stitching into a fetch operator.
  *
  * To use this rule: LazyFetchRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchRule protected (config: RelRule.Config)
  extends LazyFetchRuleSkeleton(
    config
  ) {
  override def onMatchHelper(call: RelOptRuleCall): RelNode =
    val (input, column) = (call.rel[RelNode](1), call.rel[RelNode](2)) match
      // It's important that we match a column on the right before matching a column on the left
      // because, when matching two columns, the specification expects the right one to be
      // the one that has to be fetched.
      case (left, right: LateColumnScan) => (left, right)
      case (left: LateColumnScan, right) => (right, left)
      case _ => throw InvalidRelException("🚩 Expected a LateColumnScan as a child of Stitch, but none were found.")
    LogicalFetch(input, column.deriveRowType, column.getColumn, None)
}

object LazyFetchRule {

  /**
    * Instance for a [[LazyFetchRule]]
    */
  val INSTANCE = new LazyFetchRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
      // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalStitch]
        b.operand(classOf[LogicalStitch])
          // that has inputs:
          .inputs(
            b1 =>
              // A node that is a LateColumnScan
              b1.operand(classOf[RelNode])
                // of any inputs
                .anyInputs(),
            b2 =>
              // A node that is a LateColumnScan
              b2.operand(classOf[LateColumnScan])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
