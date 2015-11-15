package sampleclean.clean.outlierremoval

import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import sampleclean.clean.LocalSCContext

/**
 * @author Viraj Mahesh
 */
class StdDeviationFilterTest extends FunSuite with LocalSCContext with Serializable {

  // Schema of the students table
  val SCHEMA = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType , true),
      StructField("gpa", DoubleType, true)))

  // Properties passed into the databricks CSV loader
  val PROPERTIES = Map("path" -> "./src/test/resources/students.csv")

  test("outlier removal integer column") {
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val sqlContext = new SQLContext(sc)

      val data = sqlContext.load("com.databricks.spark.csv", schema = SCHEMA, PROPERTIES)
      val filteredData = StdDeviationFilter.removeOutliers(scc, 1.0, "age", data)

      // Find the index of the age column and only retain the age column
      val colIdx = filteredData.columns.indexOf("age")
      val remainingValues = filteredData.map(x => x.getInt(colIdx)).collect()

      assert(remainingValues.length == 4)

      assert(remainingValues.contains(20))
      assert(remainingValues.contains(22))
      assert(remainingValues.contains(25))
      assert(remainingValues.contains(30))
    }
  }

  test("outlier removal double column") {
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val sqlContext = new SQLContext(sc)

      val data = sqlContext.load("com.databricks.spark.csv", schema = SCHEMA, PROPERTIES)
      val filteredData = StdDeviationFilter.removeOutliers(scc, 1.0, "gpa", data)

      // Find the index of the GPA column and only retain the gpa column
      val colIdx = filteredData.columns.indexOf("gpa")
      val remainingValues = filteredData.map(x => x.getDouble(colIdx)).collect()

      assert(remainingValues.length == 3)

      assert(remainingValues.contains(3.23))
      assert(remainingValues.contains(4.00))
      assert(remainingValues.contains(3.10))
    }
  }
}
