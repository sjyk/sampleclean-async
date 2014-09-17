package sampleclean.clean.misc

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanFilterAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import sampleclean.util.TypeUtils._

/** Example!! 
*/
@serializable
class RuleFilter(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanFilterAlgorithm(params, scc) {

	def exec(tableName:String)={
		val attr = params.get("attr").asInstanceOf[String]
		val rule = params.get("rule").asInstanceOf[String]
		val filtered_rdd = scc.getCleanSampleAttr(tableName, attr, rule).map(x => x.getString(0))
		scc.filterTable(tableName, filtered_rdd)
	}

	def defer(tableName:String):RDD[String]={
		val attr = params.get("attr").asInstanceOf[String]
		val rule = params.get("rule").asInstanceOf[String]
		val filtered_rdd = scc.getCleanSampleAttr(tableName, attr, rule).map(x => x.getString(0))
		return filtered_rdd
	}

}