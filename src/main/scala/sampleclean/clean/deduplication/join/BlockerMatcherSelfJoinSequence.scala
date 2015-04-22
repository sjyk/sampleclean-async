package sampleclean.clean.deduplication.join

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.clean.deduplication.matcher.Matcher
import sampleclean.clean.deduplication.blocker.Blocker

/**
 * This class acts as a wrapper for blocker + matcher routines.
 * This class has two constructors, requiring a blocker + List[matchers]
 * or a similarity join + List[Matchers]. We treat a similarity join
 * as a combination blocking and matching sequence.
 *
 * We call this the "BlockerMatcherSelfJoinSequence" because
 * in this class we apply the operation to the same sample.
 *
 * @param scc SampleClean Context
 * @param sampleTableName
 * @param blocker
 * @param matchers
 */
class BlockerMatcherSelfJoinSequence(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   blocker: Blocker,
					   var matchers: List[Matcher]) extends Serializable {
	
	private [sampleclean] var join:SimilarityJoin = null

  /**
   * Create a BlockerMatcherSelfJoinSequence based on a similarity join
   * and a list of matchers.
   * @param scc SampleClean Context
   * @param sampleTableName
   * @param simjoin Similarity Join
   * @param matchers Because the Similarity Join should contain a matching
   *                 step, this parameter commonly refers to a matcher that
   *                 matches all pairs such as:
   *                 [[sampleclean.clean.deduplication.matcher.AllMatcher]]
   *                 or to an asynchronous matcher such as
   *                 [[sampleclean.clean.deduplication.matcher.ActiveLearningMatcher]]
   */
	def this(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   simjoin: SimilarityJoin,
					   matchers: List[Matcher]) = {
		this(scc,sampleTableName,null:Blocker,matchers)
		join = simjoin
	}

  /**
   * Executes the algorithm.
   */
	def blockAndMatch(data:RDD[Row]):RDD[(Row,Row)] = {

		var blocks:RDD[Set[Row]] = null
		var matchedData:RDD[(Row,Row)] = null

		if (blocker != null)
			blocks = blocker.block(data)
		else
			{ 
			  matchedData = join.join(data,data,true,true)
			  println("Candidate Pairs Size: " + matchedData.count)
			}	

		for (m <- matchers)
		{
			if (matchedData == null)
				matchedData = m.matchPairs(blocks)
			else
				matchedData = m.matchPairs(matchedData)
		}

		return matchedData
	}

  /**
   * Adds a new matcher to the matcher list
   * @param matcher
   */
	def addMatcher(matcher: Matcher) = {
		matchers = matcher :: matchers
		matchers = matchers.reverse  
	}

	def updateContext(newContext:List[String]) = {

		if(blocker != null)
			blocker.updateContext(newContext)

		if (join != null)
			join.updateContext(newContext)

		for (m <- matchers)
			m.updateContext(newContext)
		
		println("Context Updated to: " + newContext)
	}

  /**
   * Set a function that takes some action based on new results. This
   * needs to be done if there is an asynchronous matcher
   * at the end of the sequence.
   */
	def setOnReceiveNewMatches(func: RDD[(Row,Row)] => Unit) ={
		if(matchers.last.asynchronous)
			matchers.last.onReceiveNewMatches = func
		else
			println("[SampleClean] Asychrony has no effect in this pipeline")
	}

	def printPipeline()={
			print("RDD[Row] --> ")
			if (blocker != null)
				print(blocker.getClass.getSimpleName + " --> ")
			else
				print("join(" + join.simfeature.getClass.getSimpleName + ") --> ")

			for(m <- matchers)
				print(m.getClass.getSimpleName + " --> ")

			println(" RDD[(Row,Row)]")
	}

}

