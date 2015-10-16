package sampleclean.util

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType};

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

@serializable
/**
 * The abstract SampleCleanAlgorithm defines the super class of
 *  all algorithms for data cleaning. Every algorithm is defined
 *  on a sample of data.
*/
class CachedSchemaRDD(rdd:SchemaRDD, var schema:List[String], scc:SampleCleanContext) {

	val hashCol = schema.indexOf("hash")
	val tupleizedRDD = rdd.map( row => (row(hashCol).toString(),row))
	var indexedRdd:IndexedRDD[String, Row] = IndexedRDD(tupleizedRDD).cache()

	def update(hash:String, attr:String, newVal: String) = {
		var row = indexedRdd.get(hash).get.toSeq
		var mutableRow:Array[String] = new Array(row.length)
		val attrCol = schema.indexOf(attr)

		if(mutableRow(attrCol) != newVal)
		{	
			for(i <- 0 until row.length)
				mutableRow(i) = row(i).toString()

			mutableRow(attrCol) = newVal
			indexedRdd.put(hash, Row.fromSeq(mutableRow))
		}
	}

	def collapse(row:Row, cval:String):Row = {
		val rowSeq = row.toSeq :+ cval
		return Row.fromSeq(rowSeq)
	} 

	def addTransformColumn(sourceColumn:String, destColumn:String, func:String => String) = {
		val attrCol = schema.indexOf(sourceColumn)
		val tf = indexedRdd.map(t => func(t._2(attrCol).toString()))
		indexedRdd = IndexedRDD(indexedRdd.zip(tf).map(t => (t._1._1, collapse(t._1._2,t._2)))).cache()
	}

	def updateSchema(newSchema:List[String]) = {
		schema = newSchema
	}

	def getAll():SchemaRDD = {
		val sc = scc.getSparkContext() // An existing SparkContext.
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		// this is used to implicitly convert an RDD to a DataFrame.
		//import sqlContext.implicits._
		val rddschema = StructType( schema.map(fieldName => StructField(fieldName, StringType, true)))
		return sqlContext.createDataFrame(indexedRdd.map(x => x._2),rddschema)
	}

}