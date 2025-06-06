package ch.epfl.dias.cs460.rel.early.volcano.late.qo

import ch.epfl.dias.cs460.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs460.helpers.qo.rules.skeleton.LazyFetchFilterRuleSkeleton
import ch.epfl.dias.cs460.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.{InvalidRelException, RelNode}
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexUtil

/**
  * RelRule (optimization rule) that finds a reconstruct operator that
  * stitches a filtered column (scan then filter) with the late materialized
  * tuple and transforms stitching into a fetch operator followed by a filter.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchFilterRule protected (config: RelRule.Config)
  extends LazyFetchFilterRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode =
    val (filter, scan, input) = (call.rel[RelNode](1), call.rel[RelNode](2), call.rel[RelNode](3)) match
      case (filter: LogicalFilter, scan: LateColumnScan, input) => (filter, scan, input)
      case (input, filter: LogicalFilter, scan: LateColumnScan) => (filter, scan, input)
      case _ => throw InvalidRelException("🚩 Expected a Filter as a child of Stitch, but none were found.")
    // The filter is on the standalone column, so we have to shift it to refer to place where the column will be inserted.
    val shiftedCondition = RexUtil.shift(filter.getCondition, scan.getColumn.getColumnIndex)
    filter.copy(filter.getTraitSet, LogicalFetch(input, scan.deriveRowType, scan.getColumn, None), shiftedCondition)
}

object LazyFetchFilterRule {

  /**
    * Instance for a [[LazyFetchFilterRule]]
    */
  val INSTANCE = new LazyFetchFilterRule(
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
              b2.operand(classOf[LogicalFilter])
                // of any inputs
                .oneInput(
                  b3 =>
                    b3.operand(classOf[LateColumnScan])
                      .anyInputs()
                )
          )
      )
  )
}
