package sampleclean.clean

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
class ParametricOutlier(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanFilterAlgorithm(params, scc) {

	def getMean(rdd:SchemaRDD):Double = {
		return rdd.map( x => rowToNumber(x,1) ).reduce(_ + _)/rdd.count()
	}

	def getSTD(rdd:SchemaRDD, mean:Double):Double = {
		return Math.sqrt(rdd.map( x => Math.pow(rowToNumber(x,1)-mean,2) ).reduce(_ + _)/rdd.count())
	}

	def exec(tableName:String)={

		val attr = params.get("attr").asInstanceOf[String]
		val z_thresh = params.get("z_thresh").asInstanceOf[Double]
		val sampleToClean = scc.getCleanSampleAttr(tableName, attr)
		val mean = getMean(sampleToClean)
		val std = getSTD(sampleToClean, mean)

		val filtered_rdd = sampleToClean.filter(x => Math.abs(rowToNumber(x,1) - mean)/std <= z_thresh).map(x => x(0).asInstanceOf[String])
		scc.filterTable(tableName, filtered_rdd)
	}

	def defer(tableName:String):RDD[String]={
		val attr = params.get("attr").asInstanceOf[String]
		val z_thresh = params.get("z_thresh").asInstanceOf[Double]
		val sampleToClean = scc.getCleanSampleAttr(tableName, attr)
		val mean = getMean(sampleToClean)
		val std = getSTD(sampleToClean, mean)

		val filtered_rdd = sampleToClean.filter(x => Math.abs(rowToNumber(x,1) - mean)/std <= z_thresh).map(x => x(0).asInstanceOf[String])
		return filtered_rdd
	}

}