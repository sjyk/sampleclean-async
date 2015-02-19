package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}


class ALAttributeDeduplication(params:AlgorithmParameters, 
							   scc: SampleCleanContext, sampleTableName: String) extends
							   AbstractSingleAttributeDeduplication(params, scc, sampleTableName) {
			
  val alstrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]

  override def validateParameters() ={

      if (!params.exists("activeLearningStrategy"))
        throw new RuntimeException("You need to specify an active learning strategy: activeLearningStrategy")
    
      super.validateParameters()
  }

  def matching(candidatePairs:RDD[(Row,Row)]): RDD[(Row,Row)] = {
      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      //val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
      alstrategy.asyncRun(emptyLabeledRDD, 
                                        candidatePairs, 
                                        colMapper, 
                                        colMapper, 
                                        this.apply(_))

      return scc.getSparkContext().parallelize(new Array[(Row, Row)](0))
  }
	

}
