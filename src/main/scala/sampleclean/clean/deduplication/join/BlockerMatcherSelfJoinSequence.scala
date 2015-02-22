package sampleclean.clean.deduplication.join

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.clean.deduplication.matcher.Matcher
import sampleclean.clean.deduplication.blocker.Blocker

class BlockerMatcherSelfJoinSequence(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   blocker: Blocker,
					   matchers: List[Matcher]) extends Serializable {
	
	var join:SimilarityJoin = null

	def this(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   simjoin: SimilarityJoin,
					   matchers: List[Matcher]) = {
		this(scc,sampleTableName,null:Blocker,matchers)
		join = simjoin
	}

	def blockAndMatch(data:RDD[Row]):RDD[(Row,Row)] = {

		var blocks:RDD[Set[Row]] = null
		var matchedData:RDD[(Row,Row)] = null

		if (blocker != null)
			blocks = blocker.block(data)
		else
			matchedData = join.join(data,data,true,true)

		for (m <- matchers)
		{
			if (matchedData == null)
				matchedData = m.matchPairs(blocks)
			else
				matchedData = m.matchPairs(matchedData)
		}

		return matchedData
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


	def setOnReceiveNewMatches(func: RDD[(Row,Row)] => Unit) ={
		if(matchers.last.asynchronous)
			matchers.last.onReceiveNewMatches = func
		else
			println("[SampleClean] Asychrony has no effect in this pipeline")
	}

}

