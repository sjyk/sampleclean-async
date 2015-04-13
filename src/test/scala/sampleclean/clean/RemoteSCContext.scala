package sampleclean.clean

import org.apache.spark.{SparkContext, SparkConf}
import sampleclean.api.SampleCleanContext
import sys.process._

trait RemoteSCContext extends Serializable {

  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
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
    hiveContext.hql("DROP TABLE IF EXISTS test")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
    hiveContext.hql("LOAD DATA INPATH '%s/csvJaccard100000dups' OVERWRITE INTO TABLE test".format(sc.master.replace("spark","hdfs").replace("7077","9000")))
    scc.initializeConsistent("test", "test_sample", "id", sample)

    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }
}
