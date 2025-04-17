package ch.epfl.dias.cs460.rel.early.volcano.late.qo

import ch.epfl.dias.cs460.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs460.helpers.qo.rules.skeleton.LazyFetchProjectRuleSkeleton
import ch.epfl.dias.cs460.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.{InvalidRelException, RelNode}
import org.apache.calcite.rel.logical.LogicalProject

/**
  * RelRule (optimization rule) that finds a reconstruct operator that stitches
  * a new expression (projection over one column) to the late materialized tuple
  * and transforms stitching into a fetch operator with projections.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchProjectRule protected (config: RelRule.Config)
  extends LazyFetchProjectRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode =
    val (project, scan, input) = (call.rel[RelNode](1), call.rel[RelNode](2), call.rel[RelNode](3)) match
      case (project: LogicalProject, scan: LateColumnScan, input) => (project, scan, input)
      case (input, project: LogicalProject, scan: LateColumnScan) => (project, scan, input)
      case _ => throw InvalidRelException("🚩 Expected a LateColumnScan as a child of Stitch, but none were found.")
    LogicalFetch(input, scan.deriveRowType, scan.getColumn, Some(project.getProjects))
}

object LazyFetchProjectRule {

  /**
    * Instance for a [[LazyFetchProjectRule]]
    */
  val INSTANCE = new LazyFetchProjectRule(
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
              b2.operand(classOf[LogicalProject])
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