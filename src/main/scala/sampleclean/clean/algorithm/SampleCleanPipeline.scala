package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

import sampleclean.api.SampleCleanAQP;

import SampleCleanPipeline._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

@serializable
class SampleCleanPipeline(saqp: SampleCleanAQP,
						  var execList:List[SampleCleanAlgorithm]=List[SampleCleanAlgorithm](),
	                      var queryList:List[SampleCleanQuery]=List[SampleCleanQuery]()) {

	def setPipelineOnQueries()={
		for (l <- execList)
		{
			l.pipeline = this
		}
	}

	def execAllQueries(name:String=null)={

		if(name != null)
			println("Inside Algorithm: " + name)

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
		for(l <- execList)
		{
			val before = System.nanoTime
			var stageName = "Anon"
			if(l.name != null)
				stageName = l.name

			if(l.blocking){
					println("Queued " + stageName)
					l.exec(sampleName)
					println("Completed " + stageName)
				}
			else{
					val f = Future{
						println("Queued " + stageName)
						l.exec(sampleName)
					}

					f.onComplete {
						case Success(value) => println("Completed " + stageName)
						case Failure(e) => e.printStackTrace
					}
				}
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