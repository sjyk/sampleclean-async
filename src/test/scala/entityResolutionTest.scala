package dedupTesting

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite
import sampleclean.clean.deduplication.{RecordDeduplication, GraphXInterface, EntityResolution}
import sampleclean.clean.deduplication.join.{BlockerMatcherJoinSequence, BlockerMatcherSelfJoinSequence, BroadcastJoin}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD


class entityResolutionTest extends FunSuite with Serializable{
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SCUnitTest")
    .set("spark.driver.allowMultipleContexts","true")
  val sc = new SparkContext(conf)
  val scc = new SampleCleanContext(sc)
  val context = List("id","col0")
  val attr = "col0"
  val colNames = List(attr)
  val contextString = context.mkString(" String,") + " String"
  val hiveContext = scc.getHiveContext()
  scc.closeHiveSession()
  hiveContext.hql("DROP TABLE IF EXISTS test")
  hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
  hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dupsAttr' OVERWRITE INTO TABLE test")
  val sampleTableName = "test_sample"
  scc.initializeConsistent("test", sampleTableName, "id", 1)
  val tok = new DelimiterTokenizer(" ")

  def defaultBM(sim: AnnotatedSimilarityFeaturizer): BlockerMatcherSelfJoinSequence = {
    val bJoin = new BroadcastJoin(sc,sim,false)
    val matcher = new AllMatcher(scc, sampleTableName)
    new BlockerMatcherSelfJoinSequence(scc, sampleTableName,bJoin,List(matcher))

  }


  test("function validations"){
    // Initialize algorithm
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    val params = new AlgorithmParameters()
    params.put("attr",attr)
    params.put("mergeStrategy","mostFrequent")

    val ER = new EntityResolution(params,scc,sampleTableName,defaultBM(similarity))

    // Within exec() function
    ER.setTableParameters(sampleTableName)
    assert(ER.attrCol == 3 && ER.hashCol == 0)
    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val attrCountGroup = sampleTableRDD.map(x =>
      (x(ER.attrCol).asInstanceOf[String],
        x(ER.hashCol).asInstanceOf[String])).
      groupByKey()
    val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))
    val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
      (x._1, x._2.toSet)))

    ER.components.updateContext(List(attr,"count"))

    val edgeRDD: RDD[(Long, Long, Double)] = scc.getSparkContext().parallelize(List())
    ER.graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

    assert(sampleTableRDD.count() == 201)
    assert(attrCountGroup.count() == 201)
    assert(attrCountRdd.count() == 201)
    assert(vertexRDD.count() == 201)
    assert(edgeRDD.count() == 0)

    ER.components.printPipeline()
    ER.components.setOnReceiveNewMatches(ER.apply)

    val candidates = ER.components.blockAndMatch(attrCountRdd)
    assert(candidates.count() == 100)
    ER.apply(candidates)

    assert(scc.getCleanSampleAttr(sampleTableName,"col0").map(x => (x.getString(1),x.getString(0))).groupByKey().count() == 101)

    
  }

  test("execution validation"){
    // Initialize algorithm
    scc.resetSample(sampleTableName)
    val params = new AlgorithmParameters()
    params.put("attr",attr)
    params.put("mergeStrategy","mostFrequent")

    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)

    val ER = new EntityResolution(params,scc,sampleTableName,defaultBM(similarity))

    assert(scc.getCleanSampleAttr(sampleTableName,"col0").map(x => (x.getString(1),x.getString(0))).groupByKey().count() == 201)
    ER.exec()
    assert(scc.getCleanSampleAttr(sampleTableName,"col0").map(x => (x.getString(1),x.getString(0))).groupByKey().count() == 101)

  }

  test("overhead"){
    scc.resetSample(sampleTableName)
    val params = new AlgorithmParameters()

    var t0 = System.nanoTime()
    params.put("attr",attr)
    params.put("mergeStrategy","mostFrequent")
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.5)
    val ER = new EntityResolution(params,scc,sampleTableName,defaultBM(similarity))
    val t01 = System.nanoTime()
    ER.exec()
    var t1 = System.nanoTime()

    println("Exec() in algorithm lasted " + (t1-t01).toDouble/1000000000 + " seconds.")
    println("Whole cleaning algorithm lasted " + (t1-t0).toDouble/1000000000 + " seconds.")

    val rowRDDLarge = sc.textFile("./src/test/resources/csvJaccard100dupsAttr").map(x => Row.fromSeq(x.split(",", -1).toSeq))

    t0 = System.nanoTime()
    val blocker = new WeightedJaccardSimilarity(colNames,context,tok,0.5)
    val bJoin = new BroadcastJoin(sc,blocker,false)
    assert(bJoin.join(rowRDDLarge,rowRDDLarge).count() == 100)
    t1 = System.nanoTime()

    println("Join lasted " + (t1-t0).toDouble/1000000000 + " seconds.")



  }

}
