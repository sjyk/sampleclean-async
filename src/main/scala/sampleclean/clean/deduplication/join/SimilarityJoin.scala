package sampleclean.clean.deduplication.join
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * A class that contains an algorithm called a Similarity Join,
 * which (in this context) is comprised of both a blocking and a matching step.
 * A Similarity Join will compare two datasets and produce a single dataset
 * that will contain unique records from the union of both datasets.
 *
 * The uniqueness criteria is defined by a Similarity Function
 * of the class [[AnnotatedSimilarityFeaturizer]].
 *
 * @param sc Spark Context
 * @param simfeature Similarity Featurizer used to decide whether a pair of records is
 *                   similar or not. Featurizing a pair of rows in this context
 *                   will return 1.0 if the pair is similar or 0.0 otherwise.
 * @param weighted If set to true, the algorithm will automatically calculate
 *                 token weights. Default token weights are defined based on
 *                 token idf values.
 *
 *                 Adding weights into the join might lead to more reliable
 *                 pair comparisons but could add overhead to the algorithm.
 *                 However, smart optimizations such as Prefix Filtering used in
 *                 some implementations of [[AnnotatedSimilarityFeaturizer]]
 *                 might actually reduce overhead if there is
 *                 an abundance of common tokens in the dataset.
 */
class SimilarityJoin(@transient sc: SparkContext,
					 var simfeature: AnnotatedSimilarityFeaturizer,  
					 weighted:Boolean = false) extends Serializable {

  /**
   * Join two RDDs. The resulting RDD will contain pairs of rows that
   * are considered similar by the [[AnnotatedSimilarityFeaturizer]].
   * If rddA is the same as rddB, then the algorithm will trigger a
   * self-join.
   *
   * The default implementation is naive but subclasses should override
   * and optimize.
   *
   * @param rddA First RDD of rows
   * @param rddB Second RDD of rows
   * @param sampleA True if rddA is a sample of rddB
   * @return an RDD with pairs of similar rows.
   */
	def join(rddA: RDD[Row], 
			 rddB: RDD[Row], 
			 sampleA:Boolean = false): RDD[(Row,Row)] = {

    println("[SampleClean] Executing SimilarityJoin")

		var tokenWeights = collection.immutable.Map[String,Double]()
		var tokenCounts = collection.immutable.Map[String,Int]()


    var largeTableSize = rddB.count()
    var smallTableSize = rddA.count()


		if(weighted){
			tokenCounts = computeTokenCount(rddA.map(simfeature.tokenizer.tokenize(_, simfeature.getCols())))
			tokenWeights = tokenCounts.map(x => (x._1, math.log10(largeTableSize.toDouble / x._2)))
		}
    val selfJoin = !sampleA

    var featurized: RDD[(Set[Row], Array[Double])] = null
    if (selfJoin) {
      val aWithId: RDD[(Long, Row)] = rddA.zipWithUniqueId().map(x => (x._2,x._1))
      val bWithId: RDD[(Long, Row)] = rddA.zipWithUniqueId().map(x => (x._2,x._1))
      val filtered = aWithId.cartesian(bWithId).filter(x => x._1._1 < x._2._1).map(x => (x._1._2,x._2._2))
      featurized = filtered.map(x => simfeature.featurize(Set(x._1,x._2),tokenWeights))
    }
    else{
      featurized = rddA.cartesian(rddB).filter(x => x._1(0).asInstanceOf[String] != x._2(0).asInstanceOf[String])
                                       .map(x => simfeature.featurize(Set(x._1,x._2),tokenWeights))
    }

		featurized.filter(x => x._2(0) == 1.0).map(x => (x._1.head, x._1.last))

	}

	  def setSimilarityFeaturizer(newSimilarity:String) = {

	  	val curContext = simfeature.context
      	newSimilarity match {
              case "WeightedJaccard" => simfeature = new WeightedJaccardSimilarity(simfeature)
              case "WeightedDice" => simfeature = new WeightedDiceSimilarity(simfeature)
              case "WeightedOverlap"=> simfeature = new WeightedOverlapSimilarity(simfeature)
              case "WeightedCosine"=> simfeature = new WeightedCosineSimilarity(simfeature)
              //case "EditDistance"=> simfeature = new EditFeaturizer(simfeature)
              case _ => throw new RuntimeException("Invalid Similarity: " + newSimilarity)
      	}
      	updateContext(curContext)

    }

	/**
   	* Counts the number of times that each token shows up in the data
   	* @param data  RDD with tokenized records.
   	*/
  	private def computeTokenCount(data: RDD[(Seq[String])]): collection.immutable.Map[String, Int] = {
    	val m = data.flatMap{
      	case tokens =>
        	for (x <- tokens.distinct)
        	yield (x, 1)
    	}.reduceByKeyLocally(_ + _)

    	collection.immutable.Map(m.toList: _*)
  	}

  	private [sampleclean] def updateContext(newContext:List[String]) ={
  		simfeature.setContext(newContext)
  	}
}