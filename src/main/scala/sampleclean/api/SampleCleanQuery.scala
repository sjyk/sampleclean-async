package sampleclean.api

/**
 * This class defines a sampleclean query object
 * @type {[type]}
 */
class SampleCleanQuery(scc:SampleCleanContext, 
					  saqp:SampleCleanAQP,
		              sampleName: String, 
	  				  attr: String, 
	  				  expr: String, 
	  				  pred:String, 
	  				  group:String, 
	  				  rawSC:Boolean = true){

	/** The execute method provies a way to execute the query
	 *  the result is the current time and tuple result of estimate + confidence interval
	 *  this is in a list to support group by aggregates.
	 */
	def execute():(Long, List[(String, (Double, Double))])={

		var sampleRatio = scc.getSamplingRatio(scc.qb.getCleanSampleName(sampleName))
		var defaultPred = ""
		if(pred != "")
			defaultPred = pred

		if(group != "")
			return saqp.rawSCQueryGroup(scc,
										sampleName.trim(),
										attr.trim(),
										expr.trim(),
										pred.trim(),
										group.trim(), 
										sampleRatio)
		else
			return (System.nanoTime,
					List(("1",saqp.rawSCQuery( scc,
										  sampleName.trim(),
										  attr.trim(),
										  expr.trim(),
										  pred.trim(),
										  sampleRatio))))

	}


}