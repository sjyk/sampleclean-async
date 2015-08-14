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

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}


/**
 * The Abstract Deduplication class builds the structure for
 * subclasses that implement deduplication. It has two basic
 * primitives a blocking function and then an apply function (implemented)
 * in subclasses.
 */
abstract class AbstractExtraction(params:AlgorithmParameters, 
							scc: SampleCleanContext, sampleTableName: String) extends
							SampleCleanAlgorithm(params, scc, sampleTableName) {

	if(!params.exists("newSchema"))
      	throw new RuntimeException("You need to specify a set of new cols: newSchema")

    val newCols = params.get("newSchema").asInstanceOf[List[String]]

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

		//scc.hql("select split(name, ',') from "+cleanSampleTable).collect().foreach(println)

		val extract = extractFunction(data)

		for (col <- newCols){
			//println(col)
			//extract(col).collect().foreach(println)
			var start_time = System.nanoTime()
			scc.updateTableAttrValue(sampleTableName,col,extract(col))
			println("Extract Apply Time: " + (System.nanoTime() - start_time)/ 1000000000)
		}
	}

	def extractFunction(data:SchemaRDD): Map[String,RDD[(String,String)]]

}