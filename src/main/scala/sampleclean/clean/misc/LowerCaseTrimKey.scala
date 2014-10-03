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
class LowerCaseTrimKey(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanAlgorithm(params, scc) {

    def lowerCaseTrim(x:String): String ={
    	return x.replaceAll("\\s+"," ").toLowerCase().trim()
    }

	def exec(tableName:String)={
		val attr = params.get("attr").asInstanceOf[String]
		val attrCol = scc.getColAsIndex(tableName,attr)
    	val hashCol = scc.getColAsIndex(tableName,"hash")

		val update_rdd = scc.getCleanSample(tableName).map(x =>(x(hashCol).asInstanceOf[String],
																  lowerCaseTrim(x(attrCol).asInstanceOf[String])))
		scc.updateTableAttrValue(tableName, attr, update_rdd)

		this.onUpdateNotify()
	}

}