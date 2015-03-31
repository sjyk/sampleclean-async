package dedupTesting

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
import org.apache.spark.sql.{SchemaRDD, Row}


class entityResolutionTest extends FunSuite with Serializable{
  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SCUnitTest")
  val sc = new SparkContext(conf)
  val scc = new SampleCleanContext(sc)
  val context = List("id") ++ (0 until 20).toList.map("col" + _.toString)
  val attr = "col0"
  val colNames = List(attr)
  val contextString = context.mkString(" String,") + " String"
  val hiveContext = scc.getHiveContext()
  scc.closeHiveSession()
  hiveContext.hql("DROP TABLE IF EXISTS test")
  hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
  hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dups' OVERWRITE INTO TABLE test")
  val sampleTableName = "test_sample"
  scc.initializeConsistent("test", sampleTableName, "id", 1)
  println(scc.getCleanSample(sampleTableName).count())
  println(scc.getFullTable(sampleTableName).count())
  val tok = new DelimiterTokenizer(" ")

  def defaultBM(sim: AnnotatedSimilarityFeaturizer): BlockerMatcherSelfJoinSequence = {
    val bJoin = new BroadcastJoin(sc,sim,false)
    val matcher = new AllMatcher(scc, sampleTableName)
    // TODO why self join?
    new BlockerMatcherSelfJoinSequence(scc, sampleTableName,bJoin,List(matcher))

  }


  test("function validations"){
    // Initialize algorithm
    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.9)
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
    assert(attrCountGroup.count() == 101)
    assert(attrCountRdd.count() == 101)

    // TODO I don't understand
    assert(vertexRDD.count() == 101)
    assert(edgeRDD.count() == 0)

    ER.components.printPipeline()
    ER.components.setOnReceiveNewMatches(ER.apply)

    val candidates = ER.components.blockAndMatch(attrCountRdd)
    assert(candidates.count() == 0)
    ER.apply(candidates)
    println(scc.getCleanSampleAttr(sampleTableName,"dup").collect().toSeq)
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 100)

    
  }

  test("execution validation"){
    /*// Initialize algorithm
    scc.resetSample(sampleTableName)
    val params = new AlgorithmParameters()
    params.put("attr",attr)
    params.put("mergeStrategy","mostFrequent")

    val similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.9)
    val ER = new EntityResolution(params,scc,sampleTableName,defaultBM(similarity))

    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)

    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val attrCountGroup = sampleTableRDD.map(x =>
      (x(ER.attrCol).asInstanceOf[String],
        x(ER.hashCol).asInstanceOf[String])).
      groupByKey()
    val attrCountRdd  = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))

    assert(sampleTableRDD.count() == 201)
    assert(attrCountGroup.count() == 101)
    assert(attrCountRdd.count() == 101)

    ER.components.updateContext(List(attr,"count"))
    val candidates = ER.components.blockAndMatch(attrCountRdd)
    assert(candidates.count() == 0)
    ER.apply(candidates)



    //ER.exec()
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 100)*/

/*    scc.resetSample(sampleTableName)
    similarity = new WeightedJaccardSimilarity(colNames,scc.getTableContext(sampleTableName),tok,0.51)
    ER = new EntityResolution(params,scc,sampleTableName,defaultBM(similarity))
    ER.setTableParameters(sampleTableName)
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)
    ER.synchronousExecAndRead()
    assert(scc.getCleanSampleAttr(sampleTableName,"dup").filter(x => x.getInt(1) > 1).count() == 0)*/

  }

}
