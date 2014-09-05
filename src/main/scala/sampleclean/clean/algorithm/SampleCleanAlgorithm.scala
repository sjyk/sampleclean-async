package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

@serializable
abstract class SampleCleanAlgorithm(params:AlgorithmParameters, scc: SampleCleanContext) {

	var pipeline:SampleCleanPipeline = null

	var name:String = null

	var blocking:Boolean = true

	def exec(sampleName: String)

}