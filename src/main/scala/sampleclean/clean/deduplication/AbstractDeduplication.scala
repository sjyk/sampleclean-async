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


/**
 * The Abstract Deduplication class builds the structure for
 * subclasses that implement deduplication. It has two basic
 * primitives a blocking function and then an apply function (implemented)
 * in subclasses.
 */
abstract class AbstractDeduplication(params:AlgorithmParameters, 
							scc: SampleCleanContext, sampleTableName: String) extends
							SampleCleanAlgorithm(params, scc, sampleTableName) {

	if(! params.exists("blockingFeaturizer"))
      	throw new RuntimeException("BlockingFeaturizer not specified. Every deduplication operation must have a specified similarity operation. ")

	//All deduplication operations must have blocking featurizer
	val blocker = params.get("blockingFeaturizer").asInstanceOf[BlockingFeaturizer]

	/*
		Selects a similarity join implementation, this
		will error if the join strategy is not implemented
	 */
	def blockingJoin(r1: RDD[Row],
				     r2: RDD[Row],
				     projection:List[Int], 
				     weighted:Boolean = false,
				     smaller:Boolean = true, 
			 	     containment:Boolean = true,
				     joinImpl:String = "SimilarityJoin"):RDD[(Row,Row)] = {

		val joinImplementation:SimilarityJoin = joinImpl match {
        			case "SimilarityJoin" => new SimilarityJoin(scc.getSparkContext(), blocker, projection, true) 
        			case "BroadcastJoin" => new BroadcastJoin(scc.getSparkContext(), blocker, projection, true) 
        			case _ => throw new RuntimeException("Invalid Join Implementation. Acceptable Values are {SimilarityJoin, BroadcastJoin}: " + joinImpl)
      	}

		return joinImplementation.join(r1,r2,smaller,containment)
	}

	
	/*
	 *  Apply the candidate pairs back to a base table
	 */
	def apply(candidatePairs: RDD[(Row, Row)]):Unit

}