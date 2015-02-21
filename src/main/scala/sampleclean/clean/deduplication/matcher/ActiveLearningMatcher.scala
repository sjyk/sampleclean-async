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
                            alstrategy:ActiveLearningStrategy,
                            colMapper1 : List[String] => List[Int],
                            colMapper2 : List[String] => List[Int],
                            onReceiveNewMatches: RDD[(Row,Row)] => Unit) extends
							              Matcher(scc, sampleTableName) {
			
  def matchPairs(candidatePairs: RDD[(Row,Row)]): RDD[(Row,Row)] = {
      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      //val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
      alstrategy.asyncRun(emptyLabeledRDD, 
                                        candidatePairs, 
                                        colMapper1, 
                                        colMapper2, 
                                        onReceiveNewMatches)

      return scc.getSparkContext().parallelize(new Array[(Row, Row)](0))
  }

  def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
      return matchPairs(candidatePairs.flatMap(selfCartesianProduct))
  }
	

}
