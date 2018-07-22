package org.apache.spark.sql.timo.execution

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.timo.TimoSession

/**
  * Created by Elroy on 6/7/17.
  */
abstract class TimoPlan extends SparkPlan {

  @transient
  protected[timo] final val TimoSessionState = TimoSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext = TimoSession.getActiveSession.map(_.sparkContext).orNull

}