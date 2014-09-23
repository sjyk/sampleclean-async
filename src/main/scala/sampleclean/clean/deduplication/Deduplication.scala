package sampleclean.clean.deduplication


import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import sampleclean.activeml.DeduplicationGroupLabelingContext
import sampleclean.activeml.DeduplicationPointLabelingContext
import org.apache.spark.mllib.regression.LabeledPoint


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

    // Remove those pairs that have the same id from candidatePairs because they must be duplicate
    val shrinkedCandidatePairs = candidatePairs.filter{ case (fullRow, sampleRow) =>
      scc.getColAsStringFromBaseTable(fullRow, sampleTableName, idCol) != scc.getColAsString(sampleRow, sampleTableName, idCol)
    }

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

case class AttrDedup(attr: String, count: Int)

class AttributeDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
  extends SampleCleanDeduplicationAlgorithm(params,scc) {

  def replaceIfEqual(x:String, test:String, out:String): String ={
      if(x.equals(test))
        return out
      else
        return x
    }


  var graph:Map[Row, Set[Row]] = Map[Row, Set[Row]]()

  def exec(sampleTableName: String) = {

    val attr = params.get("dedupAttr").asInstanceOf[String]

    //println("attr = " + attr)

    val sampleTableRDD = scc.getCleanSample(sampleTableName)

    // Convert RDD[AttrDedup] to a schema RDD
    val sqlContext = new SQLContext(scc.getSparkContext())
    import sqlContext._

    // Get distinct attr values and their counts
    val attrDedup: SchemaRDD = sampleTableRDD.map(row =>
      (scc.getColAsString(row, sampleTableName, attr).trim, 1)).filter(_._1 != "")
      .reduceByKey(_ + _)
      .map(x => AttrDedup(x._1, x._2)).cache()

    //attrDedup.foreach(row => println(row.getString(0)+" "+row.getString(1)))

    val schema = List("attr", "count")
    val colMapper = (colNames: List[String]) => colNames.map(schema.indexOf(_))

    val similarityParameters = params.get("similarityParameters").asInstanceOf[SimilarityParameters]

    val sc = scc.getSparkContext()

    var candidatePairs = BlockingStrategy(List("attr"))
      .setSimilarityParameters(similarityParameters)
      .blocking(sc, attrDedup, colMapper).collect()



    /* Use crowd to refine candidate pairs*/
    if (params.exist("crowdsourcingStrategy") && candidatePairs.size != 0){
      println("[SampleClean] Publish %d pairs to AMT".format(candidatePairs.size))
      val crowdsourcingStrategy = params.get("crowdsourcingStrategy").asInstanceOf[CrowdsourcingStrategy]

      val groupContext : GroupLabelingContext = DeduplicationGroupLabelingContext(
        taskType="er", data=Map("fields" ->List(attr, "count"))).asInstanceOf[GroupLabelingContext]

      // Assign a unique id for each candidate pair
      val candidatePairsWithId = candidatePairs.map{ pair =>
        val random_id = utils.randomUUID()
        (random_id, pair)
      }
      val contextMap = candidatePairsWithId.toMap

      // Construct the point labeling context with a unique id for each point
      val crowdData: Seq[(String, PointLabelingContext)] = candidatePairsWithId.map { case (id, (row1, row2)) =>
        val entity1Data = List(row1.getString(0), row1.getInt(1))
        val entity2Data = List(row2.getString(0), row2.getInt(1))
        val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data)).asInstanceOf[PointLabelingContext]
        (id, context)
      }
      val answers = crowdsourcingStrategy.run(crowdData, groupContext).answers
      candidatePairs = answers.withFilter(_.value > 0.5).map{ answer =>
        assert(contextMap.contains(answer.identifier))
        contextMap.apply(answer.identifier)
      }.toArray
    }
    println("[SampleClean] Crowd identified %d dup pairs".format(candidatePairs.size))

    candidatePairs.foreach{x=>
      println("[SampleClean] \"%s (%d)\" = \"%s (%d)\"".format(x._1.getString(0), x._1.getInt(1), x._2.getString(0), x._2.getInt(1)))
    }



    var resultRDD = sampleTableRDD.map(x =>
      (scc.getColAsString(x,sampleTableName,"hash"), scc.getColAsString(x,sampleTableName,attr)))

    for(pair <- candidatePairs)
        addToGraphUndirected(pair._1, pair._2)

    //println("Graph " + graph)

    val connectedPairs = connectedComponentsToExecOrder(connectedComponents())
    for(p <- connectedPairs)
    {
        resultRDD = resultRDD.map(x => (x._1, replaceIfEqual(x._2, p._1, p._2)))
        //println("Merging " + p._1 + " -> " + p._2)
    }

    //scc.updateTableAttrValue(sampleTableName, attr, resultRDD)
  }

  def addToGraphUndirected(vertex:Row, edgeTo:Row) ={

    if(graph contains vertex){
      graph = graph + (vertex -> (graph(vertex) + edgeTo))
    }
    else{
      graph = graph + (vertex -> Set(edgeTo))
    }

    if(graph contains edgeTo){
      graph = graph + (edgeTo -> (graph(edgeTo) + vertex))
    }
    else{
      graph = graph + (edgeTo -> Set(vertex))
    }

  }

  def dfs(vertex:Row, traverseSet:Set[Row]=Set[Row]()):Set[Row]={
    if(! (graph contains vertex))
      return Set()

    var resultSet = Set(vertex)
    for(neighbor <- graph(vertex)){
       if(! (traverseSet contains neighbor))
         resultSet = resultSet ++ (dfs(neighbor, traverseSet + vertex) + vertex)
    }

    return resultSet
  }

  def connectedComponentsToExecOrder(comps: Set[Set[Row]]): List[(String, String)] ={
    
    def compOperator(row1:Row, row2:Row) = (row1.getInt(1) < row2.getInt(1))
    var resultList = List[(String, String)]()
    for(comp <- comps){

      val sortedList = comp.toList.sortWith(compOperator)
      for(i <- 0 until (sortedList.length - 1) )
        resultList = (sortedList(i).getString(0),sortedList(sortedList.length - 1).getString(0))  :: resultList 

    }

    return resultList

  }

  def connectedComponents():Set[Set[Row]] = {
     var resultSet = Set[Set[Row]]()
     for(v <- graph.keySet){
        resultSet = resultSet + dfs(v)
     }

     return resultSet
  }

  def defer(sampleTableName:String):RDD[(String,Int)] = {
    return null
  }

}


