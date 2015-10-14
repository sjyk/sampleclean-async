package sampleclean.clean

import org.apache.spark.{SparkContext, SparkConf}
import sampleclean.api.SampleCleanContext
import sys.process._

trait RemoteSCContext extends Serializable {

  def withSampleCleanContext[T](f: SampleCleanContext => T): T = {
    val master = ("cat /root/ephemeral-hdfs/conf/masters" !!).trim()
    val conf = new SparkConf()
      .setMaster("spark://" + master + ":7077")
      .setAppName("test")
      //.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)
    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }

  def withFullRecords[T](sample:Int, f: SampleCleanContext => T): T = {
    val master = ("cat /root/ephemeral-hdfs/conf/masters" !!).trim()
    val conf = new SparkConf()
      .setMaster("spark://" + master + ":7077")
      .setAppName("test")
      //.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)
    val context = List("id") ++ (0 until 20).toList.map("col" + _.toString)

    val contextString = context.mkString(" String,") + " String"
    val hiveContext = scc.getHiveContext()
    scc.closeHiveSession()
    scc.hql("DROP TABLE IF EXISTS test")
    scc.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
    scc.hql("LOAD DATA INPATH 'hdfs://%s:9000/csvJaccard100000dups' OVERWRITE INTO TABLE test".format(master))
    scc.initializeConsistent("test", "test_sample", "id", sample)

    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }
}
