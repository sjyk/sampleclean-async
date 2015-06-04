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
class SplitExtraction(params:AlgorithmParameters, 
							scc: SampleCleanContext, sampleTableName: String) extends
							AbstractExtraction(params, scc, sampleTableName) {

	if(!params.exists("delimiter"))
      	throw new RuntimeException("You need to specify a delimiter: delimiter")

    if(!params.exists("attr"))
      	throw new RuntimeException("You need to specify an attribute to split")

    val attrCol = scc.getColAsIndex(sampleTableName, params.get("attr").asInstanceOf[String])
    val hashCol = scc.getColAsIndex(sampleTableName,"hash")
    val delim = params.get("delimiter").asInstanceOf[String]

	def extractFunction(data:SchemaRDD): Map[String,RDD[(String,String)]] = {

		var result:Map[String,RDD[(String,String)]] = Map()
		for (col <- newCols)
		{
			if (col != null)
			{
				val extract = data.rdd.map(row => (row(hashCol).asInstanceOf[String], row(attrCol).toString().split(delim)))
				//extract = extract.filter(x => (x._2.length >= newCols.length))		
				result += (col -> extract.map(row => (row._1, getIndexIfExists(row._2,newCols.indexOf(col)) )).cache())
			}
		}
		return result
	}

	def getIndexIfExists(seq:Array[String], i:Int):String = {
		if(i < seq.length)
			return seq(i)
		else
			return ""
	}

}

object SplitExtraction {

	def stringSplitAtDelimiter(scc:SampleCleanContext,
                     	 		sampleName:String,
                     	 	 	attribute: String,
                     	 	 	delimiter: String,
                     	 		outputCols: List[String]):SplitExtraction ={

		val algoPara = new AlgorithmParameters()
        algoPara.put("attr", attribute)
        algoPara.put("newSchema", outputCols)
        algoPara.put("delimiter", delimiter)
        return new SplitExtraction(algoPara,scc,sampleName)
	}
}