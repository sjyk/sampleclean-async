package sampleclean.clean

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite
import sampleclean.clean.deduplication.ActiveLearningStrategy
import sampleclean.clean.deduplication.join.{BlockerMatcherJoinSequence, BlockerMatcherSelfJoinSequence, BroadcastJoin}
import sampleclean.clean.deduplication.matcher.{ActiveLearningMatcher, AllMatcher, Matcher}
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.SimilarityFeaturizer
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer


class BlockerMatcherSuite extends FunSuite with LocalSCContext {

  val colNames = (0 until 20).toList.map("col" + _.toString)
  val tok = new DelimiterTokenizer(" ")
  val sampleTableName = "test_sample"

  test("sync matchers") {
    withSampleCleanContext { scc =>
      val row1 = Row("a", "b", "c")
      val row2 = Row("d", "e", "f")
      var m: Matcher = null

      m = new AllMatcher(scc, sampleTableName)

      // does not include equal rows and returns only (a,b) not (b,a)
      assert(m.selfCartesianProduct(Set(row1)) == List())
      assert(m.selfCartesianProduct(Set(row1, row2)) == List((row1,row2)))


      val candidates1: RDD[Set[Row]] = scc.getSparkContext().parallelize(Seq(Set(row1, row2)))
      val candidates2: RDD[(Row, Row)] = scc.getSparkContext().parallelize(Seq((row1, row2)))
      assert(m.matchPairs(candidates1).collect().toSeq == m.matchPairs(candidates2).collect().toSeq)

      }
    }

  test("async matchers"){
    /*withSampleCleanContext {scc =>
      val row1 = Row("a", "b", "c")
      val row2 = Row("a", "b", "f")
      val cols = List("col0","col1","col2")
      var candidatesReceived = false

      // Active Learning
      val featurizer = new SimilarityFeaturizer(cols, cols, List("JaccardSimilarity","JaccardSimilarity"))

      val alStrategy = new ActiveLearningStrategy(cols, featurizer)
      val m = new ActiveLearningMatcher(scc,"uselessName", alStrategy)

      val candidates1: RDD[Set[Row]] = scc.getSparkContext().parallelize(Seq(Set(row1, row2)))
      val candidates2: RDD[(Row, Row)] = scc.getSparkContext().parallelize(Seq((row1, row2)))

      m.context = cols
      m.onReceiveNewMatches = {_ => candidatesReceived = true}

      m.matchPairs(candidates1)
      assert(candidatesReceived)

      candidatesReceived = false
      m.matchPairs(candidates2)
      assert(candidatesReceived)
    }*/
  }



  test("self join sequence") {
    withFullRecords (1,{ scc =>

      val blocker = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.5)
      val bJoin = new BroadcastJoin(scc.getSparkContext(), blocker, false)
      val rdd = scc.getFullTable(sampleTableName)
      val matcher = new AllMatcher(scc, sampleTableName)

      val blockMatch = new BlockerMatcherSelfJoinSequence(scc, sampleTableName, bJoin, List(matcher))
      assert(blockMatch.blockAndMatch(rdd).count() == 100)

      val baseFeaturizer = new SimilarityFeaturizer(colNames, scc.getTableContext(sampleTableName),
                                                    List("Levenshtein", "JaroWinkler"))
      val alstrategy = ActiveLearningStrategy(colNames,baseFeaturizer)
      blockMatch.addMatcher(new ActiveLearningMatcher(scc,sampleTableName,alstrategy))
      blockMatch.setOnReceiveNewMatches(_ => println("do nothing"))

      // TODO async run

      blockMatch.updateContext(scc.getTableContext(sampleTableName).map(x => x + x))
      assert(blocker.context == scc.getTableContext(sampleTableName).map(x => x + x))
      assert(matcher.context == scc.getTableContext(sampleTableName).map(x => x + x))
    })

}

  test("sample join sequence") {
    withFullRecords(0.5, { scc =>

      val blocker = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.465)
      val bJoin = new BroadcastJoin(scc.getSparkContext(), blocker, false)
      val full = scc.getFullTable(sampleTableName)
      val sample = scc.getCleanSample(sampleTableName)
      val matcher = new AllMatcher(scc, sampleTableName)

      val blockMatch = new BlockerMatcherJoinSequence(scc, sampleTableName, bJoin, List(matcher))
      assert(blockMatch.blockAndMatch(sample, full).count() >= 40 * 2)
      blockMatch.updateContext(scc.getTableContext(sampleTableName).map(x => x + x))
      assert(blocker.context == scc.getTableContext(sampleTableName).map(x => x + x))
    })
  }

  test("clear tables"){
    withSampleCleanContext { scc =>
      // clear temp tables
      scc.closeHiveSession()

      // clear other tables
      scc.hql("DROP TABLE test")
      assert(scc.hql("SHOW TABLES").collect().forall(!_.getString(0).contains("test")))
    }
  }

}
