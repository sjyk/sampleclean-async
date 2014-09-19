package sampleclean.clean.deduplication

import org.apache.spark.sql.catalyst.expressions.Ascending
import sampleclean.activeml._
import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

// To Sanjay: add this into sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

/**
 * This class executes a deduplication algorithm on a data set.
 * @param params algorithm parameters
 * @param scc SampleClean context
 */
class Deduplication(params:AlgorithmParameters, scc: SampleCleanContext)
      extends SampleCleanDeduplicationAlgorithm(params,scc) {

  case class Record(hash: String, dup: Integer)

  def exec(smallTableName:String) = {


    val rowId = params.get("dedupParams").asInstanceOf[Row => String]
    val cp =
      candidatePairs(smallTableName, rowId)


    val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
    val joinType = params.get("blockingStrategy").asInstanceOf[BlockingStrategy].getJoinType
    val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
    activeLearningStrategy.asyncRun(emptyLabeledRDD,
                                    cp,
                                    onUpdateDupCounts(smallTableName,_: RDD[(Row,Row)],joinType, rowId))
  }

  def onUpdateDupCounts(smallTable: String, dupPairs: RDD[(Row, Row)], joinType: String, rowId: Row => String) {
    val dupCounts = dupPairs.flatMap(pair => {
      Seq((rowId(pair._1),1), (rowId(pair._2),1))
    }).reduceByKey(_ + _)

    val offset = joinType match {
      case "sampleJoin" => counts: RDD[(String, Int)] => counts.map(x => (x._1,(x._2 / 2) + 1))
      case "selfJoin" => counts: RDD[(String, Int)] => counts.map(x => (x._1,x._2 + 1))
    }
    println("[SampleClean] Updating Sample Using Predicted Counts")
    scc.updateTableDuplicateCounts(smallTable, offset(dupCounts))
  }


  def candidatePairs(smallTableName:String, rowId: Row => String) = {
    val blockingStrategy = params.get("blockingStrategy").asInstanceOf[BlockingStrategy]
    val joinType = blockingStrategy.getJoinType

    val smallTableRDD = joinType match {
      case "sampleJoin" => scc.getCleanSample(smallTableName)
      case "selfJoin" => null
    }
    val largeTableRDD = scc.getFullTable(smallTableName)

    val candidatePairs = blockingStrategy.blocking(scc.getSparkContext(), largeTableRDD, smallTableRDD)

    // filtering
    joinType match {
      case "selfJoin" => candidatePairs
      case "sampleJoin" => candidatePairs.filter(kv => rowId(kv._1) != rowId(kv._2))
    }

  }

  
  def defer(sampleTableName:String):RDD[(String,Int)] = {
      return null
  }


  }
