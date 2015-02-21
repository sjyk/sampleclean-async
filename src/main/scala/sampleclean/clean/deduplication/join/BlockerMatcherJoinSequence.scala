package sampleclean.clean.deduplication.join

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.clean.deduplication.matcher.Matcher
import sampleclean.clean.deduplication.blocker.Blocker

class BlockerMatcherJoinSequence(scc: SampleCleanContext,
              		   sampleTableName:String,
              		   simjoin:SimilarityJoin,
					   matchers: List[Matcher]) extends Serializable {

	//def this(scc: SampleCleanContext,
    //          		   sampleTableName:String,
    //          		   simjoin: SimilarityJoin = null,
	//				   matchers: List[Matcher] = List())

	def blockAndMatch(data1:RDD[Row], data2:RDD[Row]):RDD[(Row,Row)] = {

		var matchedData = simjoin.join(data1,data2,true,true)

		for (m <- matchers)
		{
			matchedData = m.matchPairs(matchedData)
		}

		return matchedData
	}

}

