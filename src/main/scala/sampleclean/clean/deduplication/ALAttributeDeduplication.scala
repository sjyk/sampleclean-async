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

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

import sampleclean.simjoin.SimilarityJoin
import sampleclean.clean.featurize.BlockingFeaturizer

class ALAttributeDeduplication(params:AlgorithmParameters, 
							   scc: SampleCleanContext) extends
							   AbstractSingleAttributeDeduplication(params, scc) {
							   

	def exec(sampleTableName: String) = {

		val attr = params.get("attr").asInstanceOf[String]
    	val attrCol = scc.getColAsIndex(sampleTableName,attr)
    	val hashCol = scc.getColAsIndex(sampleTableName,"hash")
    	val sampleTableRDD = scc.getCleanSample(sampleTableName)
    	val mergeStrategy = params.get("mergeStrategy").asInstanceOf[String]
    	val alstrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
    	val blockingFeaturizer = params.get("blockingFeaturizer").asInstanceOf[BlockingFeaturizer]
      val attrCountGroup = sampleTableRDD.map(x => 
                                          (x(attrCol).asInstanceOf[String],
                                           x(hashCol).asInstanceOf[String])).
                                          groupByKey()
      val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))
      val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
                               (x._1, x._2.toSet)))
      val edgeRDD: RDD[(Long, Long, Double)] = scc.getSparkContext().parallelize(List())
      graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

    	val candidatePairs = blockingJoin(attrCountRdd,attrCountRdd, blockingFeaturizer, List(attrCol),true,true,true, "BroadcastJoin")
    	val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
      activeLearningStrategy.asyncRun(emptyLabeledRDD, 
                                      	candidatePairs, 
                                      	scc.getSampleTableColMapper(sampleTableName), 
                                      	scc.getSampleTableColMapper(sampleTableName), 
                                      	onReceiveCandidatePairs(_, sampleTableName))
	}

}
