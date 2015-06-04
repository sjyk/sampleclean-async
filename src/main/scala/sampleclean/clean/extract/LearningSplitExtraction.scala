package sampleclean.clean.extraction

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

/**
 * The Abstract Deduplication class builds the structure for
 * subclasses that implement deduplication. It has two basic
 * primitives a blocking function and then an apply function (implemented)
 * in subclasses.
 */
class LearningSplitExtraction(params:AlgorithmParameters, 
							scc: SampleCleanContext, sampleTableName: String) extends
							AbstractExtraction(params, scc, sampleTableName) {

    var delimSet:Set[Char] = Set()

    if(!params.exists("attr"))
      	throw new RuntimeException("You need to specify an attribute to split")

    val attrCol = scc.getColAsIndex(sampleTableName, params.get("attr").asInstanceOf[String])
    val hashCol = scc.getColAsIndex(sampleTableName,"hash")

	def extractFunction(data:SchemaRDD): Map[String,RDD[(String,String)]] = {

		val extract = data.rdd.map(row => (row(hashCol).asInstanceOf[String], row(attrCol).toString().split(delimSet.toArray)))
		var result:Map[String,RDD[(String,String)]] = Map()
		for (col <- newCols)
		{
			result += (col -> extract.map(row => (row._1, getIndexIfExists(row._2,newCols.indexOf(col)) )))
		}

		return result
	}

	def addExample(attr:String, output:List[String]) = {

		val joined = output.mkString("").toCharArray().toSet
		val attrArray = attr.toCharArray().toSet
		delimSet = delimSet ++ (attrArray &~ joined)
	}

	def getIndexIfExists(seq:Array[String], i:Int):String = {
		if(i < seq.length)
			return seq(i)
		else
			return ""
	}

}