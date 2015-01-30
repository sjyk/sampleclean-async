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
    val sampleCol = scc.getColAsIndex(sampleTableName, idCol)
    val fullTableCol = scc.getColAsIndexFromBaseTable(sampleTableName, idCol)

    // Remove those pairs that have the same id from candidatePairs because they must be duplicate
    val shrinkedCandidatePairs = candidatePairs.filter{ case (fullRow, sampleRow) =>
      fullRow(fullTableCol).asInstanceOf[String] != sampleRow(sampleCol).asInstanceOf[String]
    }

    //shrinkedCandidatePairs.collect().foreach(println)

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
 * This class is used to execute a deduplication algorithm on a data set.
 * The algorithm finds names of a specified attribute that are similar
 * and performs deduplication on the SampleClean context accordingly.
 * @param params algorithm parameters.
 * @param scc SampleClean context.
 */
class AttributeDeduplication(params:AlgorithmParameters, scc: SampleCleanContext)
  extends SampleCleanDeduplicationAlgorithm(params,scc) {

  //graph with the following vertex properties (String, (Set of Id's which have the string))
  var graphXGraph:Graph[(String, Set[String]), Double] = null

  /**
   * Executes the main deduplication function. If a crowdsourcing
   * strategy is identified, it performs an additional duplicate
   * refining using Amazon Mechanical Turk.
   * @param sampleTableName name of SampleClean sample table.
   */
  def exec(sampleTableName: String) = {

    val attr = params.get("attr").asInstanceOf[String]
    val attrCol = scc.getColAsIndex(sampleTableName,attr)
    val hashCol = scc.getColAsIndex(sampleTableName,"hash")
    //println("attr = " + attr)

    val sampleTableRDD = scc.getCleanSample(sampleTableName)

    // Convert RDD[AttrDedup] to a schema RDD
    val sqlContext = new SQLContext(scc.getSparkContext())
    import sqlContext._

    val schema = List("attr", "count")
    val colMapper = (colNames: List[String]) => colNames.map(schema.indexOf(_))

    val similarityParameters = params.get("similarityParameters").asInstanceOf[SimilarityParameters]
    val mergeStrategy = params.get("mergeStrategy").asInstanceOf[String]

    var iterations = 1
    if(params.exist("iterations"))
      iterations = params.get("iterations").asInstanceOf[Int]


    for(iter <- 0 until iterations) {

    val sc = scc.getSparkContext()
    val attrCountGroup = sampleTableRDD.map(x => 
                                          (x(attrCol).asInstanceOf[String],
                                           x(hashCol).asInstanceOf[String])).
                                          groupByKey()


    val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))

    
    // Attribute pairs that are similar
    
    var candidatePairs = BlockingStrategy(List("attr"))
      .setSimilarityParameters(similarityParameters)
      .blocking(sc, attrCountRdd, colMapper)
      //.coarseBlocking(sc, attrCountRdd , 0).collect()
      //
    
    //candidatePairs.collect().foreach(println)  

    // Initialize the graph (no edges yet)
    // Vertices are of the form (Long vertexId, (String attrValue, Set of record Ids with that string)
    // Edges are of the form (Long srcVertexId, Long dstVertexId, Double weight)
    val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
                               (x._1, x._2.toSet)))
    val edgeRDD: RDD[(Long, Long, Double)] = sc.parallelize(List())
    graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

    /* Use crowd to refine candidate pairs*/
    if (params.exist("crowdsourcingStrategy") && candidatePairs.count() != 0){

      val candidatesWithSortKeys = candidatePairs map { pair => -math.min(pair._1.getLong(1), pair._2.getLong(1)) -> pair }
      val sortedCandidates = candidatesWithSortKeys.sortByKey() map {kv => kv._2}
      //var candidatePairsArray = candidatePairs.collect().sortBy(pair => -math.min(pair._1.getLong(1), pair._2.getLong(1)))

      println("[SampleClean] Publish %d pairs to AMT".format(sortedCandidates.count()))
      val crowdsourcingStrategy = params.get("crowdsourcingStrategy").asInstanceOf[CrowdsourcingStrategy]

      val groupContext = DeduplicationGroupLabelingContext(
        taskType="er", data=Map("fields" ->List(attr, "count")))

      // Assign a unique id for each candidate pair
      val candidatePairsWithId = sortedCandidates.map{ pair =>
        val random_id = utils.randomUUID()
        (random_id, pair)
      }.cache()
      val contextMap = candidatePairsWithId.collect().toMap

      // Construct the point labeling context with a unique id for each point
      val crowdData = candidatePairsWithId.map { case (id, (row1, row2)) =>
        val entity1Data = List(row1.getString(0), row1.getLong(1))
        val entity2Data = List(row2.getString(0), row2.getLong(1))
        val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data))
        (id, context)
      }

      if (params.exist("sync")) {
        val crowdResult = crowdsourcingStrategy.run(crowdData, groupContext)
        onNewCrowdResult(crowdResult.collect())
      }
      else {
        //it is async by default
        crowdsourcingStrategy.asyncRun(crowdData, groupContext, onNewCrowdResult)
      }
      def onNewCrowdResult(results: Seq[(String, Double)]) {
        val candidatePairsArray = results.withFilter(_._2 > 0.5).map{ answer =>
          assert(contextMap.contains(answer._1))
          contextMap.apply(answer._1)
        }.toArray

        onReceiveCandidatePairs(candidatePairsArray,
          sampleTableRDD,
          sampleTableName,
          attr,
          mergeStrategy,
          hashCol,
          attrCol)
      }

    }
    else if(params.exist("activeLearningStrategy") && candidatePairs.count() != 0){
      
      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]
      activeLearningStrategy.asyncRun(emptyLabeledRDD, 
                                      candidatePairs, 
                                      colMapper, 
                                      colMapper, 
                                      onReceiveCandidatePairs(_, sampleTableRDD,
                                                                sampleTableName,
                                                                attr,
                                                                mergeStrategy,
                                                                hashCol, 
                                                                attrCol))

    }
    else{

      onReceiveCandidatePairs(candidatePairs, 
                              sampleTableRDD,
                              sampleTableName,
                              attr,
                              mergeStrategy,
                              hashCol, 
                              attrCol)
    
      }
    }
  }

  def onReceiveCandidatePairs(candidatePairs: Array[(Row, Row)], 
                              sampleTableRDD:RDD[Row], 
                              sampleTableName:String,
                              attr:String, 
                              mergeStrategy:String, 
                              hashCol:Int, 
                              attrCol:Int):Unit = {

      println("[SampleClean] Crowd identified %d dup pairs".format(candidatePairs.size))

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

 def onReceiveCandidatePairs(candidatePairs: RDD[(Row, Row)], 
                              sampleTableRDD:RDD[Row], 
                              sampleTableName:String,
                              attr:String, 
                              mergeStrategy:String, 
                              hashCol:Int, 
                              attrCol:Int):Unit = {

      onReceiveCandidatePairs(candidatePairs.collect(), 
                              sampleTableRDD, 
                              sampleTableName,
                              attr, 
                              mergeStrategy, 
                              hashCol, 
                              attrCol)
 }

  def defer(sampleTableName:String):RDD[(String,Int)] = {
    return null
  }

}


