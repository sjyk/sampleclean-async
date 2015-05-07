package sampleclean.clean.deduplication.join
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

/**
 * A Broadcast join is an implementation of a Similarity Join that uses
 * an optimization called Prefix Filtering. In a distributed environment,
 * this optimization involves broadcasting a series of maps to each node.
 *
 * '''Note:''' because the algorithm may collect large RDDs into maps by using
 * driver memory, java heap problems could arise. In this case, it is
 * recommended to increase allocated driver memory through Spark configuration
 * spark.driver.memory
 *
 * @param sc Spark Context
 * @param featurizer Similarity Featurizer optimized for Prefix Filtering
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
class BroadcastJoin( @transient sc: SparkContext,
					 featurizer: AnnotatedSimilarityFeaturizer,
					 weighted:Boolean = false) extends
					 SimilarityJoin(sc,featurizer,weighted) {

  /**
   * Perform a Broadcast Join
   * @param rddA First RDD of rows
   * @param rddB Second RDD of rows
   * @param sampleA true if rddA is a sample of rddB
   * @return an RDD with pairs of similar rows.
   */
  @Override
	override def join(rddA: RDD[Row],
			 rddB:RDD[Row], 
			 sampleA:Boolean = false): RDD[(Row,Row)] = {

    println("[SampleClean] Executing BroadcastJoin")

    if (!featurizer.usesTokenPrefixFiltering) {
      super.join(rddA, rddB, sampleA)
    }

    else {
      var tokenWeights = collection.immutable.Map[String, Double]()
      var tokenCounts = collection.immutable.Map[String, Int]()

      var largeTableSize = rddB.count()
      var smallTableSize = rddA.count()
      var smallTable = rddA
      var largeTable = rddB

      tokenCounts = computeTokenCount(rddA.map(simfeature.tokenizer.tokenize(_: Row, simfeature.getCols())))

      if (weighted) {
        tokenWeights = tokenCounts.map(x => (x._1, math.log10(smallTableSize.toDouble / x._2)))
        println("[SampleClean] Calculated Token Weights")
      }

      //Add a record ID into sampleTable. Id is a unique id assigned to each row.
      var smallTableWithId: RDD[(String, (Seq[String], Row))] = null

      if(!sampleA)
      {  
        smallTableWithId = smallTable.zipWithUniqueId
          .map(x => (x._2.toString, (simfeature.tokenizer.tokenize(x._1, 
            simfeature.getCols(false)), x._1))).cache()
      }
      else
      {
        smallTableWithId = smallTable.map(x => (x(0).asInstanceOf[String], 
          (simfeature.tokenizer.tokenize(x, simfeature.getCols(false)), x))).cache()
      }

      // Set a global order to all tokens based on their frequencies
      val tokenRankMap: Map[String, Int] = tokenCounts
        .toSeq.sortBy(_._2).map(_._1).zipWithIndex.toMap

      // Broadcast rank map to all nodes
      val broadcastRank = sc.broadcast(tokenRankMap)


      // Build an inverted index for the prefixes of sample data
      val invertedIndex: RDD[(String, Seq[String])] = smallTableWithId.flatMap {
        case (id, (tokens, value)) =>
          if (tokens.size < featurizer.minSize) Seq()
          else {
            val sorted = sortTokenSet(tokens, tokenRankMap)
            for (x <- sorted)
            yield (x, id)
          }
      }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))


      //Broadcast sample data to all nodes
      val broadcastIndex = sc.broadcast(invertedIndex.collectAsMap())
      val broadcastData = sc.broadcast(smallTableWithId.collectAsMap())
      val broadcastWeights = sc.broadcast(tokenWeights)

      val selfJoin = !sampleA

      val scanTable = {
        if (selfJoin) smallTableWithId
        else {
          largeTable.map(row => (row(0).asInstanceOf[String], (featurizer.tokenizer.tokenize(row,simfeature.getCols(false)), row)))
        }
      }

      //Generate the candidates whose prefixes have overlap, and then verify their overlap similarity
      scanTable.flatMap({
        case (id1, (key1, row1)) =>
          if (key1.length >= featurizer.minSize) {
            val weightsValue = broadcastWeights.value
            val broadcastDataValue = broadcastData.value
            val broadcastIndexValue = broadcastIndex.value

            val sorted: Seq[String] = sortTokenSet(key1, broadcastRank.value)
            val removedSize = simfeature.getRemovedSize(sorted, simfeature.threshold, weightsValue)
            val filtered = sorted.dropRight(removedSize)

            filtered.foldLeft(List[String]()) {
              case (a, b) =>
                a ++ broadcastIndexValue.getOrElse(b, List())
            }.distinct.map {
              case id2 =>
                // Avoid double checking in self-join
                if ((id2.toString >= id1.toString) && selfJoin) (null, null, false)
                  // doesn't return diagonal in sample join
                else if ((id2.toString == id1.toString) && !selfJoin) (null, null, false)
                else {
                  val (key2, row2) = broadcastDataValue(id2)

                  val similar: Boolean = simfeature.optimizedSimilarity(key1, key2, simfeature.threshold, weightsValue)._1

                  (key2, row2, similar)
                }
            }.withFilter(_._3).map {
              case (key2, row2, similar) => (row1, row2)
            }
          }
          else List()
      })

    }

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

  /**
   * Sorts a token list based on token's frequency
   * @param tokens  list to be sorted.
   * @param tokenRanks Key-Value map of tokens and global ranks in ascending order (i.e. token with smallest value is rarest)
   */
  private def sortTokenSet(tokens: Seq[String], tokenRanks: Map[String, Int])
  : Seq[String] = {
    tokens.map(token => (token, tokenRanks.getOrElse(token, 0))).toSeq.sortBy(_._2).map(_._1)
  }

}