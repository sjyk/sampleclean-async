package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

import sampleclean.api.SampleCleanAQP;

import SampleCleanPipeline._

@serializable
class SampleCleanPipeline(saqp: SampleCleanAQP,
						  var execList:List[SampleCleanAlgorithm]=List[SampleCleanAlgorithm](),
	                      var queryList:List[SampleCleanQuery]=List[SampleCleanQuery]()) {

	def execAllQueries()={
		var query = 1
		for(q <- queryList)
		{
			if(q.rawSC)
			{   
				println("Query " + query + " result: " +
					saqp.rawSCQuery(q.scc,
					q.sampleName,
					q.attr,
					q.expr,
					q.pred,
					q.sampleRatio))
			}
			else{

				println("Query " + query + " result: " +
					saqp.normalizedSCQuery(q.scc,
					q.sampleName,
					q.attr,
					q.expr,
					q.pred,
					q.sampleRatio))
			}
		}
	}

	def exec(sampleName: String)={
		var stage = 1
		for(l <- execList)
		{
			val before = System.nanoTime
			l.exec(sampleName)
			println("--------")
			println("Stage " + stage + 
				    " complete in " + 
				    (System.nanoTime-before)/1e9 + 
				    " (s) ")
			execAllQueries()
			stage = stage + 1

		}
	}

}

object SampleCleanPipeline{

	case class SampleCleanQuery(scc:SampleCleanContext, 
		              sampleName: String, 
	  				  attr: String, 
	  				  expr: String, 
	  				  pred:String, 
	  				  sampleRatio: Double,
	  				  rawSC:Boolean = true)

}