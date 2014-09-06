package sampleclean.clean.dedup

import sampleclean.activeml._
import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint


// To Sanjay: add this into sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}



class Deduplication(@transient scc: SampleCleanContext) {
  case class Record(hash: String, dup: Integer)
  def onUpdateDupPairs(dupPairs: RDD[(Row, Row)]): RDD[(String, Int)] = {
    // Need to discuss with Sanjay. The code may not work if he changes the position of hash

    dupPairs.map(x => (x._1.getString(0),1)).reduceByKey(_ + _)
  }


  def clean(sampleTableName:String,
            blockingStrategy: BlockingStrategy,
            activeLearningStrategy: ActiveLearningStrategy) {

    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)

    val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
    activeLearningStrategy.run(emptyLabeledRDD, candidatePairs, onUpdateDupPairs)


      //featureVectors.map(println(_))

    //candidatePairs.map(println(_))
    /*val updateDupCount = duplicateCount(candidatePairs).filter(_._2 > 1)
    scc.updateHiveTableDuplicateCounts(sampleTableName, updateDupCount)
    println(sampleTable.count())
    println(candidatePairs.count())
    println(candidatePairs.first())*/
  }


  def clean(sampleTableName:String, blockingStrategy: BlockingStrategy): RDD[(Row, Row)] = {

    val sampleTable = scc.getCleanSample(sampleTableName)
    val fullTable = scc.getFullTable(sampleTableName)

    val similarPairs = blockingStrategy.blocking(scc.getSparkContext(), sampleTable, fullTable)
    onUpdateDupPairs(similarPairs)
    similarPairs
  }



  }
