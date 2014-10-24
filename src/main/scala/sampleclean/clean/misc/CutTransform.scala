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
class CutTransform(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanAlgorithm(params, scc) {

    def cutTransform(x:String, delimiter:String, position:Int): String ={
    	val comps = x.split(delimiter)

    	return comps(Math.min(position,comps.length-1)).trim()
    }

	def exec(tableName:String)={
		val attr = params.get("attr").asInstanceOf[String]
		val attrCol = scc.getColAsIndex(tableName,attr)
		val delimiter = params.get("delimiter").asInstanceOf[String]
		val position = params.get("position").asInstanceOf[String].toInt
    	val hashCol = scc.getColAsIndex(tableName,"hash")

		val update_rdd = scc.getCleanSample(tableName).map(x =>(x(hashCol).asInstanceOf[String],
																  cutTransform(x(attrCol).asInstanceOf[String],
																  delimiter,position)))
		scc.updateTableAttrValue(tableName, attr, update_rdd)

		this.onUpdateNotify()
	}

}