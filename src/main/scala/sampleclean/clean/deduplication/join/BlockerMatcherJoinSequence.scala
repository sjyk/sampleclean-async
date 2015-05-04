package sampleclean.clean.deduplication.join

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.clean.deduplication.matcher.Matcher
import sampleclean.clean.featurize.Tokenizer._
import sampleclean.clean.featurize.Tokenizer

/**
 * This class acts as a wrapper for blocker + matcher routines:
 * simjoin + List[Matchers]. We treat a similarity join
 * as a combination blocking and matching sequence.
 *
 * We call this the "BlockerMatcherJoinSequence" because
 * in this class we join two different datasets (with the same
 * schema).
 * 
 * @type {[type]}
 */
private [sampleclean] class BlockerMatcherJoinSequence(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   simjoin:SimilarityJoin,
					   matchers: List[Matcher]) extends Serializable {

  /**
   * Execute the algorithm.
   */
	def blockAndMatch(data1:RDD[Row], data2:RDD[Row]):RDD[(Row,Row)] = {

		var matchedData = simjoin.join(data1,data2,true)

		for (m <- matchers)
		{
			matchedData = m.matchPairs(matchedData)
		}

		return matchedData
	}


	def updateContext(newContext:List[String]) = {

		if (simjoin != null)
			simjoin.updateContext(newContext)

		for (m <- matchers)
			m.updateContext(newContext)
		
	}

	/**
	 * This function changes the similarity metric used in the Entity Resolution algorithm
	 * @type {[type]}
	 */
	def changeSimilarity(newSimilarity: String) = {
		if (newSimilarity == "EditDistance")
			throw new RuntimeException("You should use shortAttributeCanonicalize() instead")
		else if (simjoin.isInstanceOf[PassJoin])
			throw new RuntimeException("Error!")

		simjoin.setSimilarityFeaturizer(newSimilarity)
	}

	def changeTokenization(newTokenization: String) = {
		newTokenization match {
              case "WhiteSpace" => simjoin.simfeature.tokenizer = new WhiteSpaceTokenizer()
              case "WhiteSpaceAndPunc" => simjoin.simfeature.tokenizer = new WhiteSpacePunctuationTokenizer()
              case _ => throw new RuntimeException("Invalid Tokenizer: " + newTokenization)
      	}
	}

}

