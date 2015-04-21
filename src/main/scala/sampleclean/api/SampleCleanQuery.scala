package sampleclean.api


/**
 * This class defines a sampleclean query object
 *
 * The Sample Clean Query algorithm follows a cleaning
 * procedure on a sample before estimating the query result.
 * @param scc SampleClean Context
 * @param saqp Aproximate Query object
 * @param sampleName
 * @param attr attribute to query
 * @param expr aggregate function to use
 * @param pred predicate
 * @param group group by this attribute
 * @param rawSC perform a rawSC Query if true or a normalizedSC Query
 *              if false. See [[SampleCleanAQP]] for an explanation of
 *              this option.
 */
@serializable
class SampleCleanQuery(scc:SampleCleanContext,
					    saqp:SampleCleanAQP,
		          sampleName: String,
	  				  attr: String,
	  				  expr: String,
	  				  pred:String,
	  				  group:String,
	  				  rawSC:Boolean = true){

  var vizServer = "localhost:8000"

  /**
   * The execute method provides a straightforward way to execute the query.
   * @return (current time, List(aggregate, (estimate, +/- confidence value)))
   */
	def execute():(Long, List[(String, (Double, Double))])= {

		var sampleRatio = scc.getSamplingRatio(scc.qb.getCleanFactSampleName(sampleName))
		println(sampleRatio)

		var defaultPred = ""
		if(pred != "")
			defaultPred = pred

		var query:(Long, List[(String, (Double, Double))]) = null

		if(rawSC){
			query = saqp.rawSCQueryGroup(scc,
									sampleName.trim(),
									attr.trim(),
									expr.trim(),
									pred.trim(),
									group.trim(),
									sampleRatio)
		}
		else{
			query = saqp.normalizedSCQueryGroup(scc,
									sampleName.trim(),
									attr.trim(),
									expr.trim(),
									pred.trim(),
									group.trim(),
									sampleRatio)
		}


		return query

	}

	private [sampleclean] def query2Map(query:(Long, List[(String, (Double, Double))])):Map[String,Double] ={
      var listOfResults = query._2
      var result:Map[String,Double] = Map()
      listOfResults = listOfResults.sortBy(-_._2._1)
      for(r <- listOfResults.slice(0,Math.min(10, listOfResults.length)))
        result = result + (r._1 -> r._2._1)
      return result
  }


}
