package dedupTesting

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite
import sampleclean.clean.deduplication.{GraphXInterface, EntityResolution}
import sampleclean.clean.deduplication.join.{BlockerMatcherSelfJoinSequence, BroadcastJoin}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}


class entityResolutionTest extends FunSuite with Serializable{
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SCUnitTest")
  val sc = new SparkContext(conf)
  val scc = new SampleCleanContext(sc)
  val context = (0 until 20).toList.map("col" + _.toString)
  val attr = "col1"
  val colNames = List(attr)
  val colNamesString = context.mkString(" String,") + " String"
  val hiveContext = scc.getHiveContext()
  scc.closeHiveSession()
  hiveContext.hql("DROP TABLE IF EXISTS test")
  hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(colNamesString))
  hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dups' OVERWRITE INTO TABLE test")
  scc.initializeConsistent("test", "test_sample", "col0", 2)
  println(scc.getCleanSample("test_sample").count())
  println(scc.getFullTable("test_sample").count())
  val tok = new DelimiterTokenizer(" ")

  test("function validations"){
    val sampleTableName = "test_sample"

    // Initialize algorithm
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    val bJoin = new BroadcastJoin(sc,similarity,false)
    val matcher = new AllMatcher(scc, sampleTableName)
    val blockerMatcher = new BlockerMatcherSelfJoinSequence(scc, sampleTableName,bJoin,List(matcher))
    val params = new AlgorithmParameters()
    params.put("attr",attr)
    params.put("mergeStrategy","mostFrequent")

    val ER = new EntityResolution(params,scc,sampleTableName,blockerMatcher)
    ER.setTableParameters(sampleTableName)
    assert(ER.attrCol == 3 && ER.hashCol == 0)

    // Within exec() function
    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val attrCountGroup = sampleTableRDD.map(x =>
      (x(ER.attrCol).asInstanceOf[String],
        x(ER.hashCol).asInstanceOf[String])).
      groupByKey()
    val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))
    val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
      (x._1, x._2.toSet)))

    blockerMatcher.updateContext(List(attr,"count"))

    val edgeRDD: RDD[(Long, Long, Double)] = scc.getSparkContext().parallelize(List())
    ER.graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

    println(sampleTableRDD.count())
    println(attrCountGroup.count())
    println(attrCountRdd.count())
    println(vertexRDD.count())
    println(edgeRDD.count())

    blockerMatcher.printPipeline()

    blockerMatcher.setOnReceiveNewMatches(ER.apply)

    val candidates = blockerMatcher.blockAndMatch(attrCountRdd)
    assert(candidates.count() == 101)

    //val clean = scc.getCleanSample("test_sample")
    //assert(clean.count() == 101)



  }

}
