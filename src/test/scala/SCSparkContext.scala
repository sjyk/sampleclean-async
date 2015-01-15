/**
 * Created by juanmanuelsanchez on 12/11/14.
 */

package dedupTesting

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}

trait SCSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SCUnitTest")
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}


