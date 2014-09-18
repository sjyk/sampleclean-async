package sampleclean.api

class SampleCleanQuery(scc:SampleCleanContext, 
					  saqp:SampleCleanAQP,
		              sampleName: String, 
	  				  attr: String, 
	  				  expr: String, 
	  				  pred:String, 
	  				  group:String, 
	  				  rawSC:Boolean = true){

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