package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._ // To Sanjay: add this into sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters



class Deduplication(params:AlgorithmParameters, scc: SampleCleanContext)
      extends SampleCleanDeduplicationAlgorithm(params,scc) {

  case class Record(hash: String, dup: Integer)
  def duplicateCount(candidatePairs: RDD[(Row, Row)]): RDD[(String, Int)] = {
    // Need to discuss with Sanjay. The code may not work if he changes the position of hash
    candidatePairs.map(x => (x._1.getString(0),1)).reduceByKey(_ + _)
  }

  def exec(sampleTableName:String) ={//,
            //blockingStrategy: BlockingStrategy,
            //featureVectorStrategy: FeatureVectorStrategy) {

    // To Sanjay: 1. Will I generate a new RDD when I call this function?
    //            2. Can you provide a function that can get a full table name based on a given sample name
    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)
    val blockingStrategy = params.get("blockingStrategy").asInstanceOf[BlockingStrategy]
    val featureVectorStrategy = params.get("featureVectorStrategy").asInstanceOf[FeatureVectorStrategy]

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)
    val featureVectors = featureVectorStrategy.toFeatureVectors(candidatePairs)
    val updateDupCount = duplicateCount(candidatePairs).filter(_._2 > 1)
    scc.updateTableDuplicateCounts(sampleTableName, updateDupCount)
  }

  def defer(sampleTableName:String):RDD[(String,Int)] ={

    // To Sanjay: 1. Will I generate a new RDD when I call this function?
    //            2. Can you provide a function that can get a full table name based on a given sample name
    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)
    val blockingStrategy = params.get("blockingStrategy").asInstanceOf[BlockingStrategy]
    val featureVectorStrategy = params.get("featureVectorStrategy").asInstanceOf[FeatureVectorStrategy]

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)
    val featureVectors = featureVectorStrategy.toFeatureVectors(candidatePairs)
    val updateDupCount = duplicateCount(candidatePairs).filter(_._2 > 1)
    return updateDupCount

  }

}
