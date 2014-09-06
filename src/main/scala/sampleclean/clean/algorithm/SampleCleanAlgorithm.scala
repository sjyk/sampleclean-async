package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

@serializable
/** This defines the super class of all algorithms for data cleaning.
* This is an abstract class which is the most generic data cleaning 
* definition with just an exec method. By using a more generic definition
* the pipeline class cannot optimize or merge similar operations.
*/
abstract class SampleCleanAlgorithm(params:AlgorithmParameters, scc: SampleCleanContext) {

	/**Defines the pipeline with which this algorithm is associated
	 */
	var pipeline:SampleCleanPipeline = null

	/**Gives a logical name to this algorithms
	*/
	var name:String = null

	/**Execute this algorithm synchronously or asychronously
	*/
	var blocking:Boolean = true

	/**The execution function of the this algorithm
	*/
	def exec(sampleName: String)

}