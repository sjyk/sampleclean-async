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
 * This class is used to execute a deduplication algorithm on a data set.
 * The algorithm uses a text-blocking strategy to find similar record
 * pairs and updates the corresponding SampleClean sample table accordingly.
 * If there is an Active Learning strategy identified, it executes the algorithm asynchronously.
 * @param params algorithm parameters
 * @param scc SampleClean context
 */
class RecordDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
      extends SampleCleanDeduplicationAlgorithm(params,scc) {

  /**
   * Executes the main deduplication algorithm on the SampleClean context.
   * @param sampleTableName name of the sample table stored in the SampleClean context.
   */
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

    /**
     * This is a call-back function that will be called by active learning for each iteration.
     * It updates the SampleClean sample table stored in the scc.
     * @param dupPairs duplicate pairs that will be used for updating.
     */
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

/**
 * This class is used to create a valid attribute count (i.e. it's a helper class)
 * @param attr a specific attribute value
 * @param count the count of the attribute in the data set.
 */
case class AttrDedup(attr: String, count: Int)

/**
 * This class is used to execute a deduplication algorithm on a data set.
 * The algorithm finds names of a specified attribute that are similar
 * and performs deduplication on the SampleClean context accordingly.
 * @param params algorithm parameters.
 * @param scc SampleClean context.
 */
class AttributeDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
  extends SampleCleanDeduplicationAlgorithm(params,scc) {

  /**
   * Tests whether two strings are equal and returns specified value if true.
   * @param x first string.
   * @param test second string.
   * @param out is returned if strings are equal.
   */
  def replaceIfEqual(x:String, test:Map[String,String]): String ={
      if(test.contains(x.trim().toLowerCase()))
        return test(x.trim().toLowerCase())
      else
        return x
    }


  var graph:Map[Row, Set[Row]] = Map[Row, Set[Row]]()


  /**
   * Executes the main deduplication function. If a crowdsourcing
   * strategy is identified, it performs an additional duplicate
   * refining using Amazon Mechanical Turk.
   * @param sampleTableName name of SampleClean sample table.
   */
  def exec(sampleTableName: String) = {

    val attr = params.get("dedupAttr").asInstanceOf[String]
    val attrCol = scc.getColAsIndex(sampleTableName,attr)
    val hashCol = scc.getColAsIndex(sampleTableName,"hash")
    //println("attr = " + attr)

    val sampleTableRDD = scc.getCleanSample(sampleTableName)

    // Convert RDD[AttrDedup] to a schema RDD
    val sqlContext = new SQLContext(scc.getSparkContext())
    import sqlContext._

    /*// Get distinct attr values and their counts
    val attrDedup: SchemaRDD = sampleTableRDD.map(row =>
      (scc.getColAsString(row, sampleTableName, attr).trim, 1)).filter(_._1 != "")
      .reduceByKey(_ + _)
      .map(x => AttrDedup(x._1, x._2)).cache()*/

    //attrDedup.foreach(row => println(row.getString(0)+" "+row.getString(1)))

    val schema = List("attr", "count")
    val colMapper = (colNames: List[String]) => colNames.map(schema.indexOf(_))

    val similarityParameters = params.get("similarityParameters").asInstanceOf[SimilarityParameters]
    val mergeStrategy = params.get("mergeStrategy").asInstanceOf[String]

    val sc = scc.getSparkContext()
    val attrCountRdd = sampleTableRDD.map(x => 
                                          (x(attrCol),1)).
                                          reduceByKey(_ + _).
                                          map(x => AttrDedup(x._1.asInstanceOf[String], x._2))

    // Attribute pairs that are similar
    var candidatePairs = BlockingStrategy(List("attr"))
      .setSimilarityParameters(similarityParameters)
      .coarseBlocking(sc, attrCountRdd , 0).collect()


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
      (x(hashCol).asInstanceOf[String], x(attrCol).asInstanceOf[String]))

    for(pair <- candidatePairs){
        println("Added " + pair)
        addToGraphUndirected(pair._1, pair._2)
    }

    //println("Graph " + graph)

    val connectedPairs = connectedComponentsToExecOrder(connectedComponents(), mergeStrategy)
    resultRDD = resultRDD.map(x => (x._1, replaceIfEqual(x._2, connectedPairs)))

    scc.updateTableAttrValue(sampleTableName, attr, resultRDD)
    
  }

  /**
   *
   * @param vertex
   * @param edgeTo
   */
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

  /**
   *
   * @param vertex
   * @param traverseSet
   * @return
   */
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

  /**
   *
   * @param comps
   * @return
   */
  def connectedComponentsToExecOrder(comps: Set[Set[Row]], mergeStrategy:String): Map[String, String] ={
    
    def mfCompOperator(row1:Row, row2:Row) = (row1.getInt(1) < row2.getInt(1))
    def mcCompOperator(row1:Row, row2:Row) = (row1.getString(0).length > row2.getString(0).length)

    var resultList = List[(String, String)]()
    for(comp <- comps){

      var sortedList = comp.toList

      if(mergeStrategy.equals("MostConcise"))
          sortedList = comp.toList.sortWith(mcCompOperator)
      else if (mergeStrategy.equals("MostFrequent"))
          sortedList = comp.toList.sortWith(mfCompOperator)

      for(i <- 0 until (sortedList.length - 1) )
        resultList = (sortedList(i).getString(0).trim().toLowerCase(),sortedList(sortedList.length - 1).getString(0))  :: resultList 

    }

    return resultList.toMap

  }

  /**
   *
   * @return
   */
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


