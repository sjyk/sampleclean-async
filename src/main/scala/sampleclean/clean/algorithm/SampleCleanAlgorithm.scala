package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

@serializable
abstract class SampleCleanAlgorithm(params:AlgorithmParameters, scc: SampleCleanContext) {

	def exec(sampleName: String)

}