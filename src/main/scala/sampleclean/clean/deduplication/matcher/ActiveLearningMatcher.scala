package sampleclean.clean.deduplication.matcher

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.deduplication.ActiveLearningStrategy

import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}


class ActiveLeaningMatcher( scc: SampleCleanContext, 
                            sampleTableName:String,
                            alstrategy:ActiveLearningStrategy) extends
							              Matcher(scc, sampleTableName) {

  val asynchronous = true

  val colMapper = (colNames: List[String]) => colNames.map(context.indexOf(_))
			
  def matchPairs(candidatePairs: RDD[(Row,Row)]): RDD[(Row,Row)] = {

      if(onReceiveNewMatches == null)
        throw new RuntimeException("For asynchronous matchers you need to specify a onReceiveNewMatches function")

      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      //val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
      alstrategy.asyncRun(emptyLabeledRDD, 
                                        candidatePairs, 
                                        colMapper, 
                                        colMapper, 
                                        onReceiveNewMatches(_))

      return scc.getSparkContext().parallelize(new Array[(Row, Row)](0))
  }

  def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
      return matchPairs(candidatePairs.flatMap(selfCartesianProduct))
  }
	
  override def updateContext(newContext:List[String]) ={
      super.updateContext(newContext)
      alstrategy.updateContext(newContext)
  }

}
