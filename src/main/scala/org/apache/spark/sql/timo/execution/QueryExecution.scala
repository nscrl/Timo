package org.apache.spark.sql.timo.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan,QueryExecution=>SQLQueryExecution}
import org.apache.spark.sql.timo.TimoSession

/**
  * Created by houkailiu on 6/7/17.
  */
class QueryExecution(val sasSession: TimoSession, override val logical: LogicalPlan)
  extends SQLQueryExecution(sasSession, logical) {

  lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    sasSession.sessionState.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    sasSession.sessionState.optimizer.execute(withIndexedData)
  }

  override lazy val sparkPlan: SparkPlan ={
    TimoSession.setActiveSession(sasSession)
    sasSession.sessionState.planner.plan(optimizedPlan).next()
  }
}