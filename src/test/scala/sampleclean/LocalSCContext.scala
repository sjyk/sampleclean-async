package sampleclean

import org.apache.spark.{SparkConf, SparkContext}
import sampleclean.api.SampleCleanContext

/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
 */
trait LocalSCContext extends Serializable{
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSampleCleanContext[T](f: SampleCleanContext => T): T = {
    val conf = new SparkConf()
      .set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext("local", "test", conf)
    val scc = new SampleCleanContext(sc)
    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }

  def withSingleAttribute[T](sample:Int,f: SampleCleanContext => T): T = {
    val conf = new SparkConf()
      .set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext("local", "test", conf)
    val scc = new SampleCleanContext(sc)
    val context = List("id", "col0")
    val contextString = context.mkString(" String,") + " String"

    val hiveContext = scc.getHiveContext()
    scc.closeHiveSession()
    hiveContext.hql("DROP TABLE IF EXISTS test")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
    hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dupsAttr' OVERWRITE INTO TABLE test")
    scc.initializeConsistent("test", "test_sample", "id", sample)

    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }

  def withFullRecords[T](sample:Int, f: SampleCleanContext => T): T = {
    val conf = new SparkConf()
      .set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext("local", "test", conf)
    val scc = new SampleCleanContext(sc)
    val context = List("id") ++ (0 until 20).toList.map("col" + _.toString)

    val contextString = context.mkString(" String,") + " String"
    val hiveContext = scc.getHiveContext()
    scc.closeHiveSession()
    hiveContext.hql("DROP TABLE IF EXISTS test")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS test(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'".format(contextString))
    hiveContext.hql("LOAD DATA LOCAL INPATH './src/test/resources/csvJaccard100dups' OVERWRITE INTO TABLE test")
    scc.initializeConsistent("test", "test_sample", "id", sample)

    try {
      f(scc)
    } finally {
      sc.stop()
    }
  }
}


