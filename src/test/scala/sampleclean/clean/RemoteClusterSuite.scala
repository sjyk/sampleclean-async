package sampleclean.clean

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite
import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.AlgorithmParameters
import sampleclean.clean.deduplication.RecordDeduplication
import sampleclean.clean.deduplication.join.{BroadcastJoin, BlockerMatcherJoinSequence}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer


class RemoteClusterSuite extends FunSuite with RemoteSCContext{

  val colNames = (0 until 20).toList.map("col" + _.toString)
  val sampleTableName = "test_sample"
  val tok = new DelimiterTokenizer(" ")

  def defaultBM(scc: SampleCleanContext,sim: AnnotatedSimilarityFeaturizer): BlockerMatcherJoinSequence = {
    val bJoin = new BroadcastJoin(scc.getSparkContext(),sim,false)
    val matcher = new AllMatcher(scc, sampleTableName)
    new BlockerMatcherJoinSequence(scc, sampleTableName,bJoin,List(matcher))
  }

  test("hdfs") {
    withSampleCleanContext { scc =>
      val master = scc.getSparkContext().master.replace("spark","hdfs").replace("7077","9000")
      val rowRDDLarge = scc.getSparkContext().textFile("%s/csvJaccard100000dups".format(master)).map(x => Row.fromSeq(x.split(",", -1).toSeq))
    println(rowRDDLarge.count())
    }
  }

  /*test("api"){
    withFullRecords (1,{ scc =>
      // Initialize algorithm
      scc.resetSample(sampleTableName)
      val params = new AlgorithmParameters()

      var similarity = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.5)
      var RD = new RecordDeduplication(params, scc, sampleTableName, defaultBM(scc, similarity))
      RD.setTableParameters(sampleTableName)
      assert(scc.getCleanSampleAttr(sampleTableName, "dup").filter(x => x.getInt(1) > 1).count() == 0)
      RD.synchronousExecAndRead()
      assert(scc.getCleanSampleAttr(sampleTableName, "dup").filter(x => x.getInt(1) > 1).count() == 100)

      scc.resetSample(sampleTableName)
      similarity = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.51)
      RD = new RecordDeduplication(params, scc, sampleTableName, defaultBM(scc, similarity))
      RD.setTableParameters(sampleTableName)
      assert(scc.getCleanSampleAttr(sampleTableName, "dup").filter(x => x.getInt(1) > 1).count() == 0)
      RD.synchronousExecAndRead()
      assert(scc.getCleanSampleAttr(sampleTableName, "dup").filter(x => x.getInt(1) > 1).count() == 0)
    })

  }*/
}
