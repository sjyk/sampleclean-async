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

class MachineAttributeDeduplication(params:AlgorithmParameters, 
							   scc: SampleCleanContext, sampleTableName: String) extends
							   AbstractSingleAttributeDeduplication(params, scc, sampleTableName) {
							   

  def matching(candidatePairs: RDD[(Row,Row)]):RDD[(Row,Row)] = {
      return candidatePairs
  }


}
