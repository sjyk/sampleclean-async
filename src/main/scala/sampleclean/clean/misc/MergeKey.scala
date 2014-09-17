package sampleclean.clean.misc

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import sampleclean.api.SampleCleanContext._

import sampleclean.util.TypeUtils._

/** Example!! 
*/
@serializable
class MergeKey(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanAlgorithm(params, scc) {

    def replaceIfEqual(x:String, test:String, out:String): String ={
    	if(x.equals(test))
    		return out
    	else
    		return x
    }

	def exec(tableName:String)={
		val attr = params.get("attr").asInstanceOf[String]
		val key1 = params.get("src").asInstanceOf[String]
		val key2 = params.get("target").asInstanceOf[String]
		val update_rdd = scc.getCleanSample(tableName).map(x =>(scc.getColAsString(x,tableName,"hash"),
																  replaceIfEqual(scc.getColAsString(x,tableName,attr),key1,key2)))
		scc.updateTableAttrValue(tableName, attr, update_rdd)
	}

}