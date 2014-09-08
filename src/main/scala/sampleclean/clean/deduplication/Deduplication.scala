package sampleclean.clean.deduplication

import sampleclean.activeml._
import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

// To Sanjay: add this into sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}



class Deduplication(params:AlgorithmParameters, scc: SampleCleanContext)
      extends SampleCleanDeduplicationAlgorithm(params,scc) {

  case class Record(hash: String, dup: Integer)

  def onUpdateDupCounts(sampleTableName: String, dupPairs: RDD[(Row, Row)]) {
    // Need to discuss with Sanjay. The code may not work if he changes the position of hash

    val dupCounts = dupPairs.map(x => (x._1.getString(0),1)).reduceByKey(_ + _)
    scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
  }


  def exec(sampleTableName:String) ={//,
            //blockingStrategy: BlockingStrategy,
            //activeLearningStrategy: ActiveLearningStrategy) {

    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)
    val blockingStrategy = params.get("blockingStrategy").asInstanceOf[BlockingStrategy]
    val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)

    val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
    activeLearningStrategy.asyncRun(emptyLabeledRDD, candidatePairs, onUpdateDupCounts(sampleTableName,_: RDD[(Row,Row)]))
      //featureVectors.map(println(_))

    //candidatePairs.map(println(_))
    /*val updateDupCount = duplicateCount(candidatePairs).filter(_._2 > 1)
    scc.updateHiveTableDuplicateCounts(sampleTableName, updateDupCount)
    println(sampleTable.count())
    println(candidatePairs.count())
    println(candidatePairs.first())*/
  }

  
  def defer(sampleTableName:String):RDD[(String,Int)] = {
      return null
  }


  /*def clean(sampleTableName:String, blockingStrategy: BlockingStrategy): RDD[(Row, Row)] = {

    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)

    val similarPairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)
    similarPairs
  }*/



  }
