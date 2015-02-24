package sampleclean.clean.deduplication.join
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

//fix smallerA bug


class BroadcastJoin( @transient sc: SparkContext,
					 blocker: AnnotatedSimilarityFeaturizer, 
					 weighted:Boolean = false) extends
					 SimilarityJoin(sc,blocker,weighted) {

  @Override
	override def join(rddA: RDD[Row],
			 rddB:RDD[Row], 
			 smallerA:Boolean = true, 
			 containment:Boolean = true): RDD[(Row,Row)] = {

    println("[SampleClean] Executing BroadcastJoin")

    if (!blocker.canPrefixFilter) {
      super.join(rddA, rddB, smallerA, containment)
    }

    else {
      var tokenWeights = collection.immutable.Map[String, Double]()
      var tokenCounts = collection.immutable.Map[String, Int]()

      var largeTableSize = rddB.count()
      var smallTableSize = rddA.count()
      var smallTable = rddA
      var largeTable = rddB

      if (smallerA && containment) {
        tokenCounts = computeTokenCount(rddA.map(blocker.tokenizer.tokenize(_, blocker.getCols())))
      }
      else if (containment) {
        tokenCounts = computeTokenCount(rddB.map(blocker.tokenizer.tokenize(_, blocker.getCols(false))))
        val n = smallTableSize
        smallTableSize = largeTableSize
        largeTableSize = n
        smallTable = rddB
        largeTable = rddA
      }
      else {
        tokenCounts = computeTokenCount(rddA.map(blocker.tokenizer.tokenize(_, blocker.getCols())).
                                        union(rddB.map(blocker.tokenizer.tokenize(_, blocker.getCols(false)))))

        largeTableSize = largeTableSize + smallTableSize
      }

      if (weighted) {
        tokenWeights = tokenCounts.map(x => (x._1, math.log10(largeTableSize.toDouble / x._2)))
      }

      println("[SampleClean] Calculated Token Weights: " + tokenWeights)
      //Add a record ID into sampleTable. Id is a unique id assigned to each row.
      val smallTableWithId: RDD[(Long, (Seq[String], Row))] = smallTable.zipWithUniqueId
        .map(x => (x._2, (blocker.tokenizer.tokenize(x._1, blocker.getCols(false)), x._1))).cache()


      // Set a global order to all tokens based on their frequencies
      val tokenRankMap: Map[String, Int] = tokenCounts //computeTokenCount(smallTableWithId.map(_._2._1)) TODO
        .toSeq.sortBy(_._2).map(_._1).zipWithIndex.toMap

      // Broadcast rank map to all nodes
      val broadcastRank = sc.broadcast(tokenRankMap)


      // Build an inverted index for the prefixes of sample data
      val invertedIndex: RDD[(String, Seq[Long])] = smallTableWithId.flatMap {
        case (id, (tokens, value)) =>
          if (tokens.size < blocker.minSize) Seq()
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

      val selfJoin = (largeTableSize == smallTableSize) && containment

      val scanTable = {
        if (selfJoin) smallTableWithId
        else {
          largeTable.map(row => (0L, (blocker.tokenizer.tokenize(row,blocker.getCols(false)), row)))
        }
      }


      //Generate the candidates whose prefixes have overlap, and then verify their overlap similarity
      scanTable.flatMap({
        case (id1, (key1, row1)) =>
          if (key1.length >= blocker.minSize) {
            val weightsValue = broadcastWeights.value
            val broadcastDataValue = broadcastData.value
            val broadcastIndexValue = broadcastIndex.value

            val sorted: Seq[String] = sortTokenSet(key1, broadcastRank.value)
            val removedSize = blocker.getRemovedSize(sorted, blocker.threshold, weightsValue)
            val filtered = sorted.dropRight(removedSize)

            filtered.foldLeft(List[Long]()) {
              case (a, b) =>
                a ++ broadcastIndexValue.getOrElse(b, List())
            }.distinct.map {
              case id2 =>
                // Avoid double checking in self-join
                if ((id2 >= id1) && selfJoin) (null, null, false)
                else {
                  val (key2, row2) = broadcastDataValue(id2)

                  val similar: Boolean = blocker.similar(key1, key2, blocker.threshold, weightsValue)
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