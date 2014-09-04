package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD

@serializable
abstract class SampleCleanFilterAlgorithm(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends   SampleCleanAlgorithm(params, scc){

	def defer(sampleName: String):RDD[String]

}