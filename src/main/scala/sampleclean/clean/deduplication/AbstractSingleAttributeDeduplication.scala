package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

import sampleclean.simjoin.SimilarityJoin

abstract class AbstractSingleAttributeDeduplication(params:AlgorithmParameters, 
							scc: SampleCleanContext) extends
							AbstractDeduplication(params, scc) {

	var graphXGraph:Graph[(String, Set[String]), Double] = null


	def onReceiveCandidatePairs(candidatePairs: RDD[(Row, Row)], 
                                sampleTableName:String):Unit = {

		val attr = params.get("attr").asInstanceOf[String]
    	val attrCol = scc.getColAsIndex(sampleTableName,attr)
    	val hashCol = scc.getColAsIndex(sampleTableName,"hash")
    	val sampleTableRDD = scc.getCleanSample(sampleTableName)
    	val mergeStrategy = params.get("mergeStrategy").asInstanceOf[String]
    	
    	doMerge(  candidatePairs.collect(), 
                sampleTableRDD, 
                sampleTableName,
                attr, 
                mergeStrategy, 
                hashCol, 
                attrCol)
	}
	
  	def doMerge(candidatePairs: Array[(Row, Row)], 
                sampleTableRDD:RDD[Row], 
                sampleTableName:String,
                attr:String, 
                mergeStrategy:String, 
                hashCol:Int, 
                attrCol:Int):Unit = {

    var resultRDD = sampleTableRDD.map(x =>
      (x(hashCol).asInstanceOf[String], x(attrCol).asInstanceOf[String]))

    // Add new edges to the graph
    val edges = candidatePairs.map( x => (x._1(0).asInstanceOf[String].hashCode.toLong,
      x._2(0).asInstanceOf[String].hashCode.toLong, 1.0) )
    graphXGraph = GraphXInterface.addEdges(graphXGraph, scc.getSparkContext().parallelize(edges))

    // Run the connected components algorithm
    def merge_vertices(v1: (String, Set[String]), v2: (String, Set[String])): (String, Set[String]) = {
      val winner:String = mergeStrategy.toLowerCase.trim match {
        case "mostconcise" => if (v1._1.length < v2._1.length) v1._1 else v2._1
        case "mostfrequent" => if (v1._2.size > v2._2.size) v1._1 else v2._1
        case _ => throw new RuntimeException("Invalid merge strategy: " + mergeStrategy)
      }
      (winner, v1._2 ++ v2._2)
    }
    val connectedPairs = GraphXInterface.connectedComponents(graphXGraph, merge_vertices)
    println("[Sampleclean] Merging values from "
      + connectedPairs.map(v => (v._2, 1)).reduceByKey(_ + _).filter(x => x._2 > 1).count
      + " components...")

    // Join with the old data to merge in new values.
    val flatPairs = connectedPairs.flatMap(vertex => vertex._2._2.map((_, vertex._2._1)))
    val newAttrs = flatPairs.asInstanceOf[RDD[(String, String)]].reduceByKey((x, y) => x)
    val joined = resultRDD.leftOuterJoin(newAttrs).mapValues(tuple => {
      tuple._2 match {
        case Some(newAttr) => {
          if (tuple._1 != newAttr) println(tuple._1 + " => " + newAttr)
          newAttr
        }
        case None => tuple._1
      }
    })
    
    scc.updateTableAttrValue(sampleTableName, attr, joined)
    this.onUpdateNotify()

  	}

}