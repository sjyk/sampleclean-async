package sampleclean.clean.deduplication


import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

/**
 * This class executes a deduplication algorithm on a data set.
 * @param params algorithm parameters
 * @param scc SampleClean context
 */
class RecordDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
      extends SampleCleanDeduplicationAlgorithm(params,scc) {

  def exec(sampleTableName: String) = {

    val idCol = params.get("id").asInstanceOf[String]

    /* Blocking stage */
    val blockingStrategy = params.get("blockingStrategy").asInstanceOf[BlockingStrategy]
    val sc = scc.getSparkContext()
    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val fullTableRDD = scc.getFullTable(sampleTableName)
    val sampleTableColMapper = scc.getSampleTableColMapper(sampleTableName)
    val fullTableColMapper = scc.getFullTableColMapper(sampleTableName)

    val candidatePairs = blockingStrategy.blocking(sc, fullTableRDD, fullTableColMapper, sampleTableRDD, sampleTableColMapper)

    //println("Candidate Pairs: " + candidatePairs.count().toString)
    // Remove those pairs that have the same id from candidatePairs because they must be duplicate
    val shrinkedCandidatePairs = candidatePairs.filter{ case (fullRow, sampleRow) =>
      scc.getColAsStringFromBaseTable(fullRow, sampleTableName, idCol) != scc.getColAsString(sampleRow, sampleTableName, idCol)
    }
    //println("Shrinked Candidate Pairs: " + shrinkedCandidatePairs.count().toString)
   // println("Matching Pairs: " + shrinkedCandidatePairs.filter(x => scc.getColAsStringFromBaseTable(x._1, sampleTableName, "entity_id") == scc.getColAsString(x._2, sampleTableName, "entity_id")).count().toString)

    //This is a call-back function that will be called by active learning for each iteration
    def onUpdateDupCounts(dupPairs: RDD[(Row, Row)]) {

      val dupCounts = dupPairs.map{case (fullRow, sampleRow) =>
        (scc.getColAsString(sampleRow, sampleTableName, "hash"),1)} // SHOULD unify hash and idCol
        .reduceByKey(_ + _)
        .map(x => (x._1,x._2+1)) // Add back the pairs that are removed above

      println("[SampleClean] Updating Sample Using Predicted Counts")
      scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
    }

    if (!params.exist("activeLearningStrategy")){
       // machine-only deduplication
      onUpdateDupCounts(shrinkedCandidatePairs)
    }
    else{
      // Refine candidate pairs using ActiveCrowd
      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]

      activeLearningStrategy.asyncRun(emptyLabeledRDD, shrinkedCandidatePairs, fullTableColMapper, sampleTableColMapper, onUpdateDupCounts)
    }

  }
  
  def defer(sampleTableName:String):RDD[(String,Int)] = {
      return null
  }

}

case class AttrDedup(attr: String, count: String)

class AttributeDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
  extends SampleCleanDeduplicationAlgorithm(params,scc) {

  def replaceIfEqual(x:String, test:String, out:String): String ={
      if(x.equals(test))
        return out
      else
        return x
    }

  def exec(sampleTableName: String) = {

    val attr = params.get("dedupAttr").asInstanceOf[String]

    println("attr = " + attr)

    val sampleTableRDD = scc.getCleanSample(sampleTableName)

    // Convert RDD[AttrDedup] to a schema RDD
    val sqlContext = new SQLContext(scc.getSparkContext())
    import sqlContext._

    // Get distinct attr values and their counts
    val attrDedup: SchemaRDD = sampleTableRDD.map(row =>
      (scc.getColAsString(row, sampleTableName, attr).trim, 1)).filter(_._1 != "")
      .reduceByKey(_ + _)
      .map(x => AttrDedup(x._1, x._2.toString)).cache()

    attrDedup.foreach(row => println(row.getString(0)+" "+row.getString(1)))

    val schema = List("attr", "count")
    val colMapper = (colNames: List[String]) => colNames.map(schema.indexOf(_))

    val similarityParameters = params.get("similarityParameters").asInstanceOf[SimilarityParameters]

    val sc = scc.getSparkContext()

    val candidatePairs = BlockingStrategy(List("attr"))
      .setSimilarityParameters(similarityParameters)
      .blocking(sc, attrDedup, colMapper)

    println("cand count = " + candidatePairs.count())

    var resultRDD = sampleTableRDD.map(x => 
                                          (scc.getColAsString(x,sampleTableName,"hash"), 
                                            scc.getColAsString(x,sampleTableName,attr)))

    

    for(pair <- candidatePairs.collect()){
        val row1 = pair._1
        val row2 = pair._2

        resultRDD = resultRDD.map(x => (x._1 ,
                                        replaceIfEqual(x._2,
                                                    row1.getString(0),
                                                    row2.getString(0))
                                     )
                                )
    }
    

    scc.updateTableAttrValue(sampleTableName, attr, resultRDD)
  }

  def defer(sampleTableName:String):RDD[(String,Int)] = {
    return null
  }

}


