package org.apache.spark.sql.timo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Strategy, catalyst}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, SparkPlanner}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.timo.execution.TopK.SlotTopk
import org.apache.spark.sql.timo.expression._
import org.apache.spark.sql.timo.index._

import scala.collection.immutable

/**
  * Created by houkailiu on 5/11/17.
  */

private[timo] class TimoSessionState(Timoession: TimoSession) extends SessionState(Timoession) {
  self =>
  protected[timo] lazy val TimoConf = new TimoConf

  protected[timo] val indexManager: IndexManager = new IndexManager

  override def executePlan(plan: LogicalPlan) =
    new execution.QueryExecution(Timoession, plan)

  def setConf(key: String, value: String): Unit = {
    if (key.startsWith("timo.")) TimoConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  def getConf(key: String): String = {
    if (key.startsWith("timo.")) TimoConf.getConfString(key)
    else conf.getConfString(key)
  }

  def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("timo.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ TimoConf.getAllConfs
  }

  override def planner: SparkPlanner = {
    new SparkPlanner(Timoession.sparkContext, conf,
      (TemporalTopk ::IndexRelationScans :: Nil) ++ experimentalMethods.extraStrategies)
    //TemporalTopk ::
  }

  object IndexRelationScans extends Strategy with PredicateHelper {

    import org.apache.spark.sql.catalyst.expressions._

    lazy val indexInfos = indexManager.getIndexInfo

    def lookupIndexInfo(attributes: Seq[Attribute]): IndexInfo = {
      var result: IndexInfo = null
      indexInfos.foreach(item => {
        if (item.indexType==HashType||item.indexType==STEIDType) {
          if (attributes.length == 1 && item.attributes.head == attributes.head) {
            result = item
          }
        }
      })
      result
    }

    def mapIndexedExpression(expression: Expression): Expression = {
      if (expression == null) {
        val attrs: Seq[Attribute] = expression match {
          case InTime(point: Expression, _, _) =>
            point match {
              case wrapper: TimeWrapper =>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p =>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case InTemporal(key:Expression,_)=>
            key match{
              case wrapper:TimeWrapper=>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p=>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case InRange(key:Expression,_,_)=>
            key match{
              case wrapper:TimeWrapper=>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p=>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case IntervalFind(key:Expression,_,_,_)=>
            key match{
              case wrapper:TimeWrapper=>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p=>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case _ =>
            null
        }
        if (attrs != null && lookupIndexInfo(attrs) != null) expression
        else null
      } else expression
    }

    def selectFilter(predicates: Seq[Expression]): Seq[Expression] = {

      val originalPredicate = predicates.reduceLeftOption(And).getOrElse(Literal(true))
      val predicateCanBeIndexed = originalPredicate.map(clause =>
        clause.map(mapIndexedExpression).filter(_ != null))
      predicateCanBeIndexed.map(pre => pre.reduceLeftOption(And).getOrElse(Literal(true)))

    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, indexed: IndexedRelation) =>
        val predicatesCanBeIndexed = selectFilter(filters)
        val parentFilter = // if all predicate can be indexed, then remove the predicate
          if (predicatesCanBeIndexed.toString // TODO ugly hack
            .compareTo(Seq(filters.reduceLeftOption(And).getOrElse(true)).toString) == 0) Seq[Expression]()
          else filters
        pruneFilterProjectionForIndex(
          projectList,
          parentFilter,
          identity[Seq[Expression]],
          IndexedRelationScan(_, predicatesCanBeIndexed, indexed)) :: Nil
      case _ => Nil
    }
  }

  object topK extends PredicateHelper with Logging{
    type ReturnType = (LogicalPlan,Expression,Literal,Literal,Literal,Literal)

    def unapply(plan:LogicalPlan): Option[ReturnType] ={
      plan match{
        case Filter(condition,child)=>
          try {
            val children=condition.children
            val attr=children(0)
            val starttime=children(1).asInstanceOf[Literal]
            val endtime = children(2).asInstanceOf[Literal]
            val k = children(3).asInstanceOf[Literal]
            val flag=children(4).asInstanceOf[Literal]
            Some(child, attr, starttime, endtime, k,flag)
          }catch{
            case e:IndexOutOfBoundsException =>None
          }
        case _ => None
      }
    }
  }

  object TemporalTopk extends Strategy with PredicateHelper{
    def apply(plan:LogicalPlan):Seq[SparkPlan]=plan match{
      case topK(logical,attr,startpoint,endpoint,k,flag)=>
        TimoConf.topk match{
          case "Slot"=>
            SlotTopk(planLater(logical),attr.children,startpoint,endpoint,k,flag)::Nil
        }
      case _=>Nil
    }}

  def pruneFilterProjectionForIndex(projectList: Seq[NamedExpression], filterPredicates: Seq[Expression],
                                    prunePushedDownFilters: Seq[Expression] => Seq[Expression],
                                    scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {
    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }
}
