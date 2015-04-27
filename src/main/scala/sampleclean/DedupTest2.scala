package sampleclean

import org.apache.spark.{SparkContext, SparkConf}
import sampleclean.api.{SampleCleanQuery, SampleCleanAQP, SampleCleanContext}
import sampleclean.clean.algorithm.{SampleCleanPipeline, AlgorithmParameters}
import sampleclean.clean.deduplication.RecordDeduplication
import org.apache.spark.SparkContext._
import sampleclean.clean.deduplication.EntityResolution
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.WordTokenizer
import sys.process._


object DedupTest2 {
  /**
   * Main function
   */
  def main(args: Array[String]): Unit = {

    //val master = ("cat /root/ephemeral-hdfs/conf/masters" !!).trim()
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    //conf.setMaster("spark://" + master + ":7077")
    conf.set("spark.executor.memory", "4g")

    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)
    val saqp = new SampleCleanAQP()
    val sampleName = "test_sample"

    val context = List("id", "col0")
    val contextString = context.mkString(" String,") + " String"

    val hiveContext = scc.getHiveContext()
    scc.closeHiveSession()
    hiveContext.hql("DROP TABLE IF EXISTS test")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
    //val master = ("cat /root/ephemeral-hdfs/conf/masters" !!).trim()
    //hiveContext.hql("LOAD DATA INPATH 'hdfs://%s:9000/csvJaccard100dupsAttr' OVERWRITE INTO TABLE test".format(master))
    hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dupsAttr' OVERWRITE INTO TABLE test")
    scc.initializeConsistent("test", sampleName, "id", 1)

    // query to get duplicates
    val aqp = new SampleCleanAQP()
    val query = new SampleCleanQuery(scc,aqp,sampleName,"*","COUNT","dup > 1","")

    val algorithm1 = RecordDeduplication.textAutomatic(scc, sampleName, context.drop(1),0.5, false)
    val algorithm2 = EntityResolution.textAttributeActiveLearning(scc, sampleName,"col0",0.3,false)

    // automatic ER
    query.execute()
    println("0 duplicates initially")
    algorithm1.exec()
    query.execute()
    println("100 duplicates after automatic deduplication")

    // Active Learning ER
    scc.resetSample(sampleName)
    query.execute()
    println("0 duplicates initially")

    val watchedQueries = Set(query)
    val pp = new SampleCleanPipeline(saqp, List(algorithm2), watchedQueries)
    pp.exec()


  }

}
