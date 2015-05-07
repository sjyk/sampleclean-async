package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanAQP;
import sampleclean.api.SampleCleanQuery;
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * This class defines a "pipeline". A pipeline is a set of SampleCleanAlgorithms
 * to execute. The pipeline determines how to execute the algorithms and whether to
 * optimize their executions
 */
@serializable
class SampleCleanPipeline(saqp: SampleCleanAQP,
						  var execList:List[SampleCleanAlgorithm]=List[SampleCleanAlgorithm](),
	                      var queryList:Set[SampleCleanQuery]=Set[SampleCleanQuery]()) {

	//execute this on construction
	setPipelineOnQueries()

	/**
   * This associates the current object with the pipeline algorithms
	 */
	private def setPipelineOnQueries()={
		for (l <- execList)
		{
			l.pipeline = this
		}
	}

	/**
	 * This notifies the pipeline of an update
	 */
	private [sampleclean] def notification()={
		for(q <- queryList)
			q.execute()
	}

  /**
   * Registers a new query with the pipeline
   */
  def registerQuery(q: SampleCleanQuery) = {queryList += q}

	/**
   * Executes the algorithms in the pipeline
	 */
	def exec()={
		for(l <- execList)
		{
			val before = System.nanoTime
			var stageName = "Anon"
			if(l.name != null)
				stageName = l.name

			if(l.blocking){
					println("[SampleClean] Added " + stageName + " to the Pipeline")
					l.exec()
					println("Completed " + stageName)
				}
			else{
					val f = Future{
						println("[SampleClean] Added " + stageName + " to the Pipeline")
						l.exec()
					}

					f.onComplete {
						case Success(value) => println("Completed " + stageName)
						case Failure(e) => e.printStackTrace
					}
				}
		}
	}

}