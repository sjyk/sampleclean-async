package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD

/**This class represents a generic deduplication algorithms
*/
@serializable
abstract class SampleCleanFilterAlgorithm(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends   SampleCleanAlgorithm(params, scc){

    /**Defer allows the write step of the algorithm to be defered
    */
	def defer(sampleName: String):RDD[String]

}