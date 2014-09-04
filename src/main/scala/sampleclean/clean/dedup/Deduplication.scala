package sampleclean.clean.dedup

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._ // To Sanjay: add this into sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}



class Deduplication(@transient scc: SampleCleanContext) {
  case class Record(hash: String, dup: Integer)
  def duplicateCount(candidatePairs: RDD[(Row, Row)]): RDD[(String, Int)] = {
    // Need to discuss with Sanjay. The code may not work if he changes the position of hash

    candidatePairs.map(x => (x._1.getString(0),1)).reduceByKey(_ + _)
  }

  def clean(//fullTableName:String,
            sampleTableName:String,
            blockingStrategy: BlockingStrategy,
            featureVectorStrategy: FeatureVectorStrategy) {

    // To Sanjay: 1. Will I generate a new RDD when I call this function?
    //            2. Can you provide a function that can get a full table name based on a given sample name
    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)
    val featureVectors = featureVectorStrategy.toFeatureVectors(candidatePairs)
    println(featureVectors.count())
    println(featureVectors.first())
    //featureVectors.map(println(_))

    //candidatePairs.map(println(_))
    /*val updateDupCount = duplicateCount(candidatePairs).filter(_._2 > 1)
    scc.updateHiveTableDuplicateCounts(sampleTableName, updateDupCount)
    println(sampleTable.count())
    println(candidatePairs.count())
    println(candidatePairs.first())*/
  }

}
