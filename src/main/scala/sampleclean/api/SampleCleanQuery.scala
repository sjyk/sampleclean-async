package sampleclean.api
import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext


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
	  				  pred:String = "",
	  				  group:String = "",
	  				  rawSC:Boolean = true){

  case class ResultSchemaRDD(val group: String, val agg: Double, val se: Double) extends Throwable

  /**
   * The execute method provides a straightforward way to execute the query.
   * @return (current time, List(aggregate, (estimate, +/- confidence value)))
   */
	def execute():SchemaRDD= {

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

		val sc = scc.getSparkContext()
		val sqlContext = new SQLContext(sc)
		val rddRes = sc.parallelize(query._2)
		val castRDD = rddRes.map(x => ResultSchemaRDD(x._1,x._2._1, x._2._2))
		return scc.getHiveContext.createDataFrame(castRDD)

	}

}
