package dedupTesting

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite
import sampleclean.clean.deduplication.RecordDeduplication
import sampleclean.clean.deduplication.join.{BroadcastJoin}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.AlgorithmParameters

import sampleclean.clean.deduplication.join.BlockerMatcherJoinSequence


class recordDedupTest extends FunSuite with Serializable {
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SCUnitTest")
  val sc = new SparkContext(conf)
  val scc = new SampleCleanContext(sc)
  val context = List("id") ++ (0 until 20).toList.map("col" + _.toString)
  val colNames = context.drop(1)
  val contextString = context.mkString(" String,") + " String"
  val hiveContext = scc.getHiveContext()
  scc.closeHiveSession()
  hiveContext.hql("DROP TABLE IF EXISTS test")
  hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
  hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dups' OVERWRITE INTO TABLE test")
  val sampleTableName = "test_sample"
  scc.initializeConsistent("test", sampleTableName, "id", 1)
  val tok = new DelimiterTokenizer(" ")

  def defaultBM(sim: AnnotatedSimilarityFeaturizer): BlockerMatcherJoinSequence = {
    val bJoin = new BroadcastJoin(sc,sim,false)
    val matcher = new AllMatcher(scc, sampleTableName)
    new BlockerMatcherJoinSequence(scc, sampleTableName,bJoin,List(matcher))
  }

  test("function validations"){
    // Initialize algorithm
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    val blockerMatcher = defaultBM(similarity)
    val params = new AlgorithmParameters()

    val RD = new RecordDeduplication(params,scc,sampleTableName,blockerMatcher)
    RD.setTableParameters(sampleTableName)
    assert(RD.hashCol == 0)

    // Within exec() function
    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val fullTableRDD = scc.getFullTable(sampleTableName)
    assert(sampleTableRDD.count() == 201 && fullTableRDD.count() == 201)

    val filteredPairs = blockerMatcher.blockAndMatch(sampleTableRDD,fullTableRDD)
    assert(filteredPairs.count() == 100)

    val dupCounts = filteredPairs.map{case (fullRow, sampleRow) =>
      (sampleRow(RD.hashCol).asInstanceOf[String],1)}
      .reduceByKey(_ + _)
      .map(x => (x._1,x._2+1))

    scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 100)
  }

  test("execution validation"){
    // Initialize algorithm
    scc.resetSample(sampleTableName)
    val params = new AlgorithmParameters()

    var similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    var RD = new RecordDeduplication(params,scc,sampleTableName,defaultBM(similarity))
    RD.setTableParameters(sampleTableName)
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)
    RD.synchronousExecAndRead()
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 100)

    scc.resetSample(sampleTableName)
    similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.51)
    RD = new RecordDeduplication(params,scc,sampleTableName,defaultBM(similarity))
    RD.setTableParameters(sampleTableName)
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)
    RD.synchronousExecAndRead()
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)

  }

  test("overhead"){
    scc.resetSample(sampleTableName)
    val params = new AlgorithmParameters()

    var t0 = System.nanoTime()
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    val RD = new RecordDeduplication(params,scc,sampleTableName,defaultBM(similarity))
    RD.setTableParameters(sampleTableName)
    val t01 = System.nanoTime()
    RD.synchronousExecAndRead()
    val t02 = System.nanoTime()
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 100)
    var t1 = System.nanoTime()

    println("Exec() in algorithm lasted " + (t02-t01).toDouble/1000000000 + " seconds.")
    println("Whole cleaning algorithm lasted " + (t1-t0).toDouble/1000000000 + " seconds.")

    val rowRDDLarge = sc.textFile("./src/test/resources/csvJaccard100dups").map(x => Row.fromSeq(x.split(",", -1).toSeq))

    t0 = System.nanoTime()
    val blocker = new WeightedJaccardSimilarity(colNames,context,tok,0.5)
    val bJoin = new BroadcastJoin(sc,blocker,false)
    assert(bJoin.join(rowRDDLarge,rowRDDLarge).count() == 100)
    t1 = System.nanoTime()

    println("Join lasted " + (t1-t0).toDouble/1000000000 + " seconds.")



  }
}
