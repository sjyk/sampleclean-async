package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.graphx._
import sampleclean.clean.deduplication.join.BlockerMatcherSelfJoinSequence

import sampleclean.clean.deduplication.join._
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import sampleclean.clean.featurize.Tokenizer._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.featurize._
import sampleclean.eval._

/**
 * This is the base class for attribute deduplication.
 * It implements a basic structure and error handling
 * for the class.
 *
 * Companion object provides a few common scenarios.
 * @param params algorithm parameters including "attr" and "mergeStrategy".
 *               "attr" refers to the attribute name that will be considered for deduplication.
 *               "mergeStrategy" refers to a strategy used to resolve a set of duplicated attributes.
 *               i.e. pick one between {USA, U.S.A, United States, United States of America, ...}
 *
 *               Allowed strategies are "mostConcise" and "mostFrequent"
 *               mostConcise will pick the shortest String
 *               mostFrequent will pick the most common String
 * @param scc SampleClean Context
 * @param sampleTableName
 * @param components blocker + matcher routine.
 */
class EntityResolution(params:AlgorithmParameters, 
							scc: SampleCleanContext,
              sampleTableName:String,
              private [sampleclean] val components: BlockerMatcherSelfJoinSequence
              ) extends SampleCleanAlgorithm(params, scc, sampleTableName) {

    //validate params before starting
    validateParameters()

    //there are the read-only class variables that subclasses can use (validated at execution time)
    val attr = params.get("attr").asInstanceOf[String]
    var mergeStrategy = params.get("mergeStrategy").asInstanceOf[String]
    
    //these are dynamic class variables
    private [sampleclean] var attrCol = 1
    private [sampleclean] var hashCol = 0

    private [sampleclean] var graphXGraph:Graph[(String, Set[String]), Double] = null

    /*
      Sets the dynamic variables at exec time
     */
    private [sampleclean] def setTableParameters(sampleTableName: String) = {
        attrCol = scc.getColAsIndex(sampleTableName,attr)
        hashCol = scc.getColAsIndex(sampleTableName,"hash")
        //curSampleTableName = sampleTableName
    }

    /*
      This function validates the parameters of the class
     */
    private [sampleclean] def validateParameters() = {
        if(!params.exists("attr"))
          throw new RuntimeException("Attribute deduplication is specified on a single attribute, you need to provide this as a parameter: attr")

        if(!params.exists("mergeStrategy"))
          throw new RuntimeException("You need to specify a strategy to resolve a set of duplicated attributes to a canonical value: mergeStrategy")
    }

    def exec() = {
        validateParameters()
        setTableParameters(sampleTableName)

        val sampleTableRDD = scc.getCleanSample(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
        val attrCountGroup = sampleTableRDD.map(x => 
                                          (x(attrCol).asInstanceOf[String],
                                           x(hashCol).asInstanceOf[String])).
                                          groupByKey()
        val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))
        val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
                               (x._1, x._2.toSet)))

        components.updateContext(List(attr,"count"))

        val edgeRDD: RDD[(Long, Long, Double)] = scc.getSparkContext().parallelize(List())
        graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

        components.printPipeline()

        components.setOnReceiveNewMatches(apply)

        apply(components.blockAndMatch(attrCountRdd))
    }

    /*
      Apply function implementation for the AbstractDedup Class
     */
	  private [sampleclean] def apply(candidatePairs: RDD[(Row, Row)]):Unit = {
       val sampleTableRDD = scc.getCleanSample(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
       //candidatePairs.collect().foreach(println)
       apply(candidatePairs, sampleTableRDD.rdd)
	  }

     /* TODO fix!
      Apply function implementation for the AbstractDedup Class
     */	
  	private [sampleclean] def apply(candidatePairs: RDD[(Row, Row)],
                sampleTableRDD:RDD[Row]):Unit = {

      var resultRDD = sampleTableRDD.map(x =>
        (x(hashCol).asInstanceOf[String], x(attrCol).asInstanceOf[String]))

      // Add new edges to the graph
      val edges = candidatePairs.map( x => (x._1(0).asInstanceOf[String].hashCode.toLong,
        x._2(0).asInstanceOf[String].hashCode.toLong, 1.0) )

      graphXGraph = GraphXInterface.addEdges(graphXGraph, edges)

      // Run the connected components algorithm
      def merge_vertices(v1: (String, Set[String]), v2: (String, Set[String])): (String, Set[String]) = {

        var b1 = v1
        var b2 = v2

        if(v1._1.hashCode.toLong < v2._1.hashCode.toLong) //handles tiebreaks
        {
            b1 = v2
            b2 = v1
        }

        val winner:String = mergeStrategy.toLowerCase.trim match {
          case "mostconcise" => if (b1._1.length < b2._1.length) b1._1 else b2._1
          case "mostfrequent" => if (b1._2.size > b2._2.size) b1._1 else b2._1
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
            //TODO if (tuple._1 != newAttr) log println(tuple._1 + " => " + newAttr)
            newAttr
          }
          case None => tuple._1
        }
      })

      scc.updateTableAttrValue(sampleTableName, attr, joined)
      this.onUpdateNotify()

  	}

    /**
     * Sets the canonicalization strategy
     */
    def setCanonicalizationStrategy(strategy:String):EntityResolution = {
      mergeStrategy = strategy
      return this
    }

    def changeSimilarity(newSimilarity: String):EntityResolution = {
      components.changeSimilarity(newSimilarity)
      return this
    }

    def changeTokenization(newTokenization: String):EntityResolution = {
      components.changeTokenization(newTokenization)
      return this
    }

    def changeThreshold(newThreshold:Double):EntityResolution = {
      components.join.simfeature.threshold = newThreshold
      return this
    }

    def tune(eval:Evaluator):EntityResolution = {
      val m = new MonotonicSimilarityThresholdTuner(scc,eval,components.join.simfeature)
      val tunedThreshold = m.tuneThreshold(sampleTableName);
      changeThreshold(tunedThreshold)
      println("Changing Threshold to " + tunedThreshold + " based on ground truth.")
      return this
    }

    def tunePersistent():EntityResolution = {
      val m = new PersistentHomologyThresholdTuner(scc,components.join.simfeature)
      val tunedThreshold = m.tuneThreshold(sampleTableName)
      changeThreshold(tunedThreshold)
      println("Changing Threshold to " + tunedThreshold + " based on persistence.")
      return this
    }

    def auto(eval:Evaluator):EntityResolution = {
      val m = new SimilarityMetricChooser(scc, eval)
      val tunedMetric = m.tuneThresholdAndMetric(sampleTableName, List(attr))
      components.join.simfeature = tunedMetric
      println("Changing Metric to " + tunedMetric.getClass.getName)
      return this
    }

}

object EntityResolution {

  /**
   * This method builds an Entity Resolution algorithm that will
   * resolve automatically. It uses several default values and is designed
   * for simple Entity Resolution tasks. For more flexibility in
   * parameters (such as setting a Similarity Featurizer and Tokenizer),
   * refer to the [[EntityResolution]] class.
   *
   * This algorithm uses the Edit Distance Similarity for pairwise comparisons
   * and a word tokenizer.
   *
   * @param scc SampleClean Context
   * @param sampleName
   * @param attribute name of attribute to resolve
   * @param threshold threshold used in the algorithm. Must be
   *                  between 0.0 and 1.0
   * @param weighting If set to true, the algorithm will automatically calculate
   *                 token weights. Default token weights are defined based on
   *                 token idf values.
   *
   *                 Adding weights into the join might lead to more reliable
   *                 pair comparisons and speed up the algorithm if there is
   *                 an abundance of common words in the dataset.
   */
    def shortAttributeCanonicalize(scc:SampleCleanContext,
                               sampleName:String, 
                               attribute: String, 
                               threshold:Double=0.9,
                               weighting:Boolean =true):EntityResolution = {

        val algoPara = new AlgorithmParameters()
        algoPara.put("attr", attribute)
        algoPara.put("mergeStrategy", "mostFrequent")

        val similarity = new EditFeaturizer(List(attribute), 
                                                   scc.getTableContext(sampleName),
                                                   WordTokenizer(), 
                                                   threshold)

        val join = new PassJoin(scc.getSparkContext(), similarity)
        val matcher = new AllMatcher(scc, sampleName)
        val blockerMatcher = new BlockerMatcherSelfJoinSequence(scc,sampleName, join, List(matcher))
        return new EntityResolution(algoPara, scc, sampleName, blockerMatcher)
    }

      /**
   * This method builds an Entity Resolution algorithm that will
   * resolve automatically. It uses several default values and is designed
   * for simple Entity Resolution tasks. For more flexibility in
   * parameters (such as setting a Similarity Featurizer and Tokenizer),
   * refer to the [[EntityResolution]] class.
   *
   * This algorithm uses the Weighted Jaccard Similarity for pairwise comparisons
   * and a word tokenizer.
   *
   * @param scc SampleClean Context
   * @param sampleName
   * @param attribute name of attribute to resolve
   * @param threshold threshold used in the algorithm. Must be
   *                  between 0.0 and 1.0
   * @param weighting If set to true, the algorithm will automatically calculate
   *                 token weights. Default token weights are defined based on
   *                 token idf values.
   *
   *                 Adding weights into the join might lead to more reliable
   *                 pair comparisons and speed up the algorithm if there is
   *                 an abundance of common words in the dataset.
   */
    def longAttributeCanonicalize(scc:SampleCleanContext,
                               sampleName:String, 
                               attribute: String, 
                               threshold:Double=0.9,
                               weighting:Boolean =true):EntityResolution = {

        val algoPara = new AlgorithmParameters()
        algoPara.put("attr", attribute)
        algoPara.put("mergeStrategy", "mostFrequent")

        val similarity = new WeightedJaccardSimilarity(List(attribute), 
                                                   scc.getTableContext(sampleName),
                                                   WordTokenizer(), 
                                                   threshold)

        val join = new BroadcastJoin(scc.getSparkContext(), similarity, weighting)
        val matcher = new AllMatcher(scc, sampleName)
        val blockerMatcher = new BlockerMatcherSelfJoinSequence(scc,sampleName, join, List(matcher))
        return new EntityResolution(algoPara, scc, sampleName, blockerMatcher)
    }

  /**
   * This method builds an Entity Resolution algorithm that will
   * resolve asynchronously. It uses several default values and is designed
   * for simple Entity Resolution tasks. For more flexibility in
   * parameters (such as setting a Similarity Featurize, Tokenizer and
   * Active Learning Strategy), refer to the [[EntityResolution]] class.
   *
   * This algorithm uses the Jaccard Similarity for pairwise filtering
   * and sim measures Levenshtein and JaroWinkler for featurization.
   *
   * The algorithm also uses a word tokenizer.
   *
   * @param scc SampleClean Context
   * @param sampleName
   * @param attribute name of attribute to resolve
   * @param threshold threshold used in the algorithm. Must be
   *                  between 0.0 and 1.0
   * @param weighting If set to true, the algorithm will automatically calculate
   *                 token weights. Default token weights are defined based on
   *                 token idf values.
   *
   *                 Adding weights into the join might lead to more reliable
   *                 pair comparisons and speed up the algorithm if there is
   *                 an abundance of common words in the dataset.
   */
    def textAttributeActiveLearning(scc:SampleCleanContext,
                               sampleName:String, 
                               attribute: String, 
                               threshold:Double=0.9,
                               weighting:Boolean =true):EntityResolution = {

        val algoPara = new AlgorithmParameters()
        algoPara.put("attr", attribute)
        algoPara.put("mergeStrategy", "mostFrequent")

        val cols = List(attribute)
        val baseFeaturizer = new SimilarityFeaturizer(cols, 
                                                      scc.getTableContext(sampleName), 
                                                      List("Levenshtein", "JaroWinkler"))

        val alStrategy = new ActiveLearningStrategy(cols, baseFeaturizer)
        val matcher = new ActiveLearningMatcher(scc, sampleName, alStrategy)
        val similarity = new WeightedJaccardSimilarity(List(attribute), 
                                                   scc.getTableContext(sampleName),
                                                   WordTokenizer(), 
                                                   threshold)

        val join = new BroadcastJoin(scc.getSparkContext(), similarity, weighting)
        val blockerMatcher = new BlockerMatcherSelfJoinSequence(scc,sampleName, join, List(matcher))
        return new EntityResolution(algoPara, scc, sampleName, blockerMatcher)
    }

    def createCrowdMatcher(scc:SampleCleanContext,
                           attr: String,
                           sampleName: String):ActiveLearningMatcher = {

        val baseFeaturizer = new SimilarityFeaturizer(List(attr), 
                                                      scc.getTableContext(sampleName), 
                                                      List("Levenshtein", "JaroWinkler"))

        val alStrategy = new ActiveLearningStrategy(List(attr), baseFeaturizer)
        return new ActiveLearningMatcher(scc, sampleName, alStrategy)
    }

}
