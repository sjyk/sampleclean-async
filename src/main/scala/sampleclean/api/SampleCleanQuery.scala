package sampleclean.api

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http._
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpResponseStatus}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization

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
 * @param querystring
 */
@serializable
class SampleCleanQuery(scc:SampleCleanContext,
					    saqp:SampleCleanAQP,
		          sampleName: String,
	  				  attr: String,
	  				  expr: String,
	  				  pred:String,
	  				  group:String,
	  				  rawSC:Boolean = true,
              querystring:String = ""){

  var vizServer = "localhost:8000"

	/**
   * The execute method provides a way to execute the query.
	 * The result is the current time and tuple result of estimate + confidence interval
	 * this is in a list to support group by aggregates.
	 */
	def execute(dashboard:Boolean = false):(Long, List[(String, (Double, Double))])= {

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

		if(dashboard){
      val use_ssl = sys.env.getOrElse("SSL", "0") == "1"
			implicit val formats = Serialization.formats(NoTypeHints)
    		/*val requestData = compact(render(
      		("querystring" -> querystring) ~
        	("query_id" -> 1 ) ~
          	("pipeline_id" -> 1 ) ~
          	("grouped" -> true ) ~
          	("grouped_result" -> query2Map(query))))
          	println(requestData)*/

          	val builder = ClientBuilder()
      		.codec(Http())
      		.hosts(vizServer)
      		.hostConnectionLimit(1)

      val client: Service[HttpRequest, HttpResponse] = if (use_ssl) {
        builder.tlsWithoutValidation().build() } else builder.build()
      val url_scheme = if(use_ssl) "https" else "http"

    		val request = RequestBuilder()
      		.url(url_scheme + "://" +vizServer +"/dashboard/results/")
      		.addHeader("Charset", "UTF-8")
      		.addFormElement(("querystring", querystring))
      		.addFormElement(("result_col_name", "Query Result"))
      		.addFormElement(("query_id", querystring.hashCode()+""))
      		.addFormElement(("pipeline_id", "1"))
      		.addFormElement(("grouped", "true"))
      		.addFormElement(("results", compact(render(query2Map(query)))))
      		.buildFormPost()

    		val responseFuture = client(request)

    		responseFuture onSuccess { resp: HttpResponse =>
      val responseData = resp.getContent.toString("UTF-8")
      resp.getStatus  match {
        case HttpResponseStatus.OK =>
          implicit val formats = DefaultFormats
          (parse(responseData) \ "status").extract[String] match {
            case "ok" =>  println("[SampleClean] Sent Query")
            case other: String => println("Error! Bad request: " + other)
          }
        case other: HttpResponseStatus =>
          println("Error! Got unexpected response status " + other.getCode + ". Data: " + responseData)
      }

    } onFailure { exc: Throwable =>
      println("Failure!")
      throw exc
    }

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
