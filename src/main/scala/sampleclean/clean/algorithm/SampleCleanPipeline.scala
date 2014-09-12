package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext

import sampleclean.api.SampleCleanAQP;

import SampleCleanPipeline._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**This class defines a "pipeline". A pipeline is a set of SampleCleanAlgorithms 
* to execute. The pipeline determines how the execute the algorithms and whether to
* optimize their executions
*/
@serializable
class SampleCleanPipeline(saqp: SampleCleanAQP,
						  var execList:List[SampleCleanAlgorithm]=List[SampleCleanAlgorithm](),
	                      var queryList:List[SampleCleanQuery]=List[SampleCleanQuery]()) {

	/**This associates the current object with the pipeline algorithms
	*/
	def setPipelineOnQueries()={
		for (l <- execList)
		{
			l.pipeline = this
		}
	}

	/**Executes all queries associated with the pipeline
	*/
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

	/**Executes the algorithms in the pipeline
	*/
	def exec(sampleName: String)={
		for(l <- execList)
		{
			val before = System.nanoTime
			var stageName = "Anon"
			if(l.name != null)
				stageName = l.name

			if(l.blocking){
					println("[SampleClean] Added " + stageName + " to the Pipeline")
					l.exec(sampleName)
					println("Completed " + stageName)
				}
			else{
					val f = Future{
						println("[SampleClean] Added " + stageName + " to the Pipeline")
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