package sampleclean.clean

import sampleclean.api.SampleCleanContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row

import sampleclean.util.TypeUtils._

/** Example!! 
*/
@serializable
class ParametricOutlier(scc: SampleCleanContext) {

	def getMean(rdd:SchemaRDD):Double = {
		return rdd.map( x => rowToNumber(x,1) ).reduce(_ + _)/rdd.count()
	}

	def getSTD(rdd:SchemaRDD, mean:Double):Double = {
		return Math.sqrt(rdd.map( x => Math.pow(rowToNumber(x,1)-mean,2) ).reduce(_ + _)/rdd.count())
	}

	def clean(tableName:String, attr: String, z_thresh:Double)={

		val sampleToClean = scc.getCleanSampleAttr(tableName, attr)
		val mean = getMean(sampleToClean)
		val std = getSTD(sampleToClean, mean)

		val filtered_rdd = sampleToClean.where(x => Math.abs(rowToNumber(x,1) - mean)/std <= z_thresh)
		scc.filterHiveTable(tableName, filtered_rdd)
	}

}