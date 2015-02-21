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
              		   blocker: Blocker = null,
					   matchers: List[Matcher] = List()) {

	//def this(scc: SampleCleanContext,
    //          		   sampleTableName:String,
    //          		   simjoin: SimilarityJoin = null,
	//				   matchers: List[Matcher] = List())

	def blockAndMatch(data:RDD[Row]):RDD[(Row,Row)] = {

		var blocks:RDD[Set[Row]] = null
		var matchedData:RDD[(Row,Row)] = null

		if (blocker != null)
			blocks = blocker.block(data)
		//else
		//	matchedData = simjoin.join(data,data,true,true)

		for (m <- matchers)
		{
			if (matchedData == null)
				matchedData = m.matchPairs(blocks)
			else
				matchedData = m.matchPairs(matchedData)
		}

		return matchedData
	}

}

