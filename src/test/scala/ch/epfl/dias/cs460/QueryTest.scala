package ch.epfl.dias.cs460

import ch.epfl.dias.cs460.QueryTest.defaultOptimizationRules
import ch.epfl.dias.cs460.helpers.SqlPrepare
import ch.epfl.dias.cs460.helpers.builder.Factories
import ch.epfl.dias.cs460.helpers.qo.rules._
import ch.epfl.dias.cs460.rel.early.volcano.late.qo
import ch.epfl.dias.cs460.rel.early.volcano.late.qo.{LazyFetchFilterRule, LazyFetchProjectRule, LazyFetchRule}
import org.apache.calcite.rel.rules.CoreRules
import org.junit.jupiter.api.{DynamicNode, TestFactory}

import java.io.IOException
import java.util

object QueryTest{
  val defaultOptimizationRules = List(
    CoreRules.PROJECT_FILTER_TRANSPOSE,
    CoreRules.PROJECT_JOIN_TRANSPOSE,
    new ProjectDropTransposeRule,
    new ProjectStitchTransposeRule,
    FilterDropTransposeRule.INSTANCE,
    FilterStitchTransposeRule.INSTANCE,
    JoinDropTransposeRule.INSTANCE,
  )
}

class QueryTest extends ch.epfl.dias.cs460.util.QueryTest {

  @TestFactory
  @throws[IOException]
  override protected[cs460] def tests: util.List[DynamicNode] = {
    runTests(
      List(
        "volcano (row store)" -> SqlPrepare(
          Factories.VOLCANO_INSTANCE,
          "rowstore"
        ),
        "volcano Late (Late Column store)" -> SqlPrepare(
          Factories.VOLCANO_LATE_INSTANCE,
          "columnstore"
        ),
        "volcano Late with rules (Late Column store)" -> SqlPrepare(
          Factories.VOLCANO_LATE_INSTANCE,
          "columnstore",
          defaultOptimizationRules ++ qo.selectedOptimizations ++
            List(
              LazyFetchRule.INSTANCE,
              LazyFetchProjectRule.INSTANCE,
              LazyFetchFilterRule.INSTANCE
            )
        )
      )
    )
  }
}
