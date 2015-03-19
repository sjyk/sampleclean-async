package dedupTesting

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite
import sampleclean.api.{SampleCleanAQP, SampleCleanContext}
import sampleclean.clean.deduplication.EntityResolution
import sampleclean.clean.deduplication.join.{BlockerMatcherJoinSequence, BlockerMatcherSelfJoinSequence, BroadcastJoin}
import sampleclean.clean.deduplication.matcher.{AllMatcher, Matcher}
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer
import sampleclean.parse.SampleCleanParser


class blockerMatcherTest extends FunSuite with Serializable {
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SCUnitTest")
  val sc = new SparkContext(conf)
  val scc = new SampleCleanContext(sc)


  test("matcher"){

    val row1 = Row("a","b","c")
    val row2 = Row("d","e","f")
    var m:Matcher = null


    m = new AllMatcher(scc, "test_sample")

    // does not include equal rows
    val cartesian = Set((row2,row1),(row1,row2))
    assert(m.selfCartesianProduct(Set(row1,row1)) == List())
    assert(m.selfCartesianProduct(Set(row1,row2)).toSet == cartesian)


    val candidates1: RDD[Set[Row]] = sc.parallelize(Seq(Set(row1,row2)))
    val candidates2: RDD[(Row,Row)] = sc.parallelize(Seq((row1,row2)))
    // TODO should they be the same?
    //assert(m.matchPairs(candidates1).collect() == m.matchPairs(candidates2).collect())

    // TODO asynchronous matchers

    }

  val context = (0 until 20).toList.map("col" + _.toString)
  val colNames = context
  val colNamesString = context.mkString(" String,") + " String"
  val hiveContext = scc.getHiveContext()
  scc.closeHiveSession()
  hiveContext.hql("DROP TABLE IF EXISTS test")
  hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(colNamesString))
  hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dups' OVERWRITE INTO TABLE test")
  val tok = new DelimiterTokenizer(" ")

  test("self join sequence"){


    //val path = "./src/test/resources"
    //val rdd = sc.textFile(path + "/dirtyJaccard100dups").map(Row(_))
    scc.initializeConsistent("test", "test_sample", "col0", 1)

    val blocker = new WeightedJaccardSimilarity(colNames,context,tok,0.5)
    val bJoin = new BroadcastJoin(sc,blocker,false)
    val rdd = scc.getFullTable("test_sample")
    val matcher = new AllMatcher(scc, "test_sample")

    val blockMatch = new BlockerMatcherSelfJoinSequence(scc, "test_sample",bJoin,List(matcher))
    assert(blockMatch.blockAndMatch(rdd).count() == 100)
    //blockMatch.updateContext(context.map(x => x + x))
    //assert(blocker.context == context.map(x => x + x))

  }

  test("join sequence"){
    scc.closeHiveSession()
    val (clean,dirty) = scc.initializeConsistent("test", "test_sample2", "col0", 2)

    val blocker = new WeightedJaccardSimilarity(colNames,context,tok,0.465)
    val bJoin = new BroadcastJoin(sc,blocker,false)
    val rdd1 = scc.getFullTable("test_sample2")
    val rdd2 = dirty
    val matcher = new AllMatcher(scc, "test_sample2")

    val blockMatch = new BlockerMatcherJoinSequence(scc, "test_sample2",bJoin,List(matcher))
    assert(blockMatch.blockAndMatch(rdd2,rdd1).count() >= 40 * 2 + rdd2.count())
    blockMatch.updateContext(context.map(x => x + x))
    assert(blocker.context == context.map(x => x + x))
  }
}
