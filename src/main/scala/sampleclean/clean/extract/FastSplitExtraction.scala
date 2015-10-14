package sampleclean.clean.extraction

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}


/**
 * The Abstract Deduplication class builds the structure for
 * subclasses that implement deduplication. It has two basic
 * primitives a blocking function and then an apply function (implemented)
 * in subclasses.
 */
class FastSplitExtraction(params:AlgorithmParameters, 
							scc: SampleCleanContext, sampleTableName: String) extends
							SampleCleanAlgorithm(params, scc, sampleTableName) {

	if(!params.exists("newSchema"))
      	throw new RuntimeException("You need to specify a set of new cols: newSchema")

    val newCols = params.get("newSchema").asInstanceOf[List[String]]
    val attr = params.get("attr").asInstanceOf[String]
    val delimiter = params.get("delimiter").asInstanceOf[String]

	def exec()={
		val data = scc.getCleanSample(sampleTableName)
		val hc = scc.getHiveContext()
		val cleanSampleTable = scc.qb.getCleanSampleName(sampleTableName)
		val dirtySampleTable = scc.qb.getDirtySampleName(sampleTableName)
		val baseTable = scc.getParentTable(scc.qb.getDirtySampleName(sampleTableName))

		val existingCols = scc.getTableContext(sampleTableName)

		var actualNewCols = List[String]()
		for (col <- newCols)
			if (!existingCols.contains(col))
				actualNewCols = col :: actualNewCols

		actualNewCols = actualNewCols.reverse
		
		if(actualNewCols.length > 0){
			val query1 = scc.qb.addColsToTable(actualNewCols, cleanSampleTable)
			println(query1)
			scc.hql(query1)

			val query2 = scc.qb.addColsToTable(actualNewCols, dirtySampleTable)
			println(query2)
			scc.hql(query2)

			val query3 = scc.qb.addColsToTable(actualNewCols, baseTable)
			println(query3)
			scc.hql(query3)
		}

		var selectionString = List[String]()
		var count = 0
		for (col <- actualNewCols)
		{
			selectionString = "split("+attr+", '"+delimiter+"')["+count+"] as " + col :: selectionString
			count = count + 1
		}

		selectionString = existingCols ::: selectionString.reverse
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		
		val query  = scc.qb.createTableAs(tmpTableName)+
					 scc.qb.buildSelectQuery(selectionString,cleanSampleTable)
		
		scc.hql(query)

		println(query)

		scc.hql("drop table "+cleanSampleTable);

   		scc.hql("ALTER TABLE " + tmpTableName + " RENAME TO " +cleanSampleTable);


		/*val extract = extractFunction(data)

		for (col <- newCols){
			//println(col)
			//extract(col).collect().foreach(println)
			var start_time = System.nanoTime()
			scc.updateTableAttrValue(sampleTableName,col,extract(col))
			println("Extract Apply Time: " + (System.nanoTime() - start_time)/ 1000000000)
		}*/
	}

	//def extractFunction(data:SchemaRDD): Map[String,RDD[(String,String)]]
}

object FastSplitExtraction {

	def stringSplitAtDelimiter(scc:SampleCleanContext,
                     	 		sampleName:String,
                     	 	 	attribute: String,
                     	 	 	delimiter: String,
                     	 		outputCols: List[String]):FastSplitExtraction ={

		val algoPara = new AlgorithmParameters()
        algoPara.put("attr", attribute)
        algoPara.put("newSchema", outputCols)
        algoPara.put("delimiter", delimiter)
        return new FastSplitExtraction(algoPara,scc,sampleName)
	}
}