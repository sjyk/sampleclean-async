package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

import sampleclean.simjoin._
import sampleclean.clean.featurize.BlockingFeaturizer

abstract class AbstractDeduplication(params:AlgorithmParameters, 
							scc: SampleCleanContext) extends
							SampleCleanAlgorithm(params, scc) {

	def blockingJoin(r1: RDD[Row],
				 r2: RDD[Row],
				 blocker: BlockingFeaturizer, 
				 projection:List[Int], 
				 weighted:Boolean = false,
				 smaller:Boolean = true, 
			 	 containment:Boolean = true,
				 joinImpl:String = "SimilarityJoin"):RDD[(Row,Row)] = {

		val joinImplementation:SimilarityJoin = new BroadcastJoin(scc.getSparkContext(), blocker, projection, true) //Class.forName("sampleclean.simjoin." + joinImpl).getConstructors()(0).newInstance(scc, blocker, projection,).asInstanceOf[SimilarityJoin]
		return joinImplementation.join(r1,r2,smaller,containment)
	}

	def onReceiveCandidatePairs(candidatePairs: RDD[(Row, Row)], 
                                sampleTableName:String):Unit

	
	def defer(sampleTableName:String):RDD[(String,Int)] = {
      return null
  	}

}