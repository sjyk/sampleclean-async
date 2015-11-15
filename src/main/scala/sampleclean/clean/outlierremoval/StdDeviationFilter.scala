package sampleclean.clean.outlierremoval

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import sampleclean.api.SampleCleanContext

/**
 * Detects outliers based on deviation from the mean
 *
 * @author Viraj Mahesh
 *
 * @param scc The sample clean context
 * @param maxDev An observation x is an outlier iff: abs(x - mean) > maxDev * stdDev
 * @param colName The index of the column that will be used to classify a row
 * as an outlier
 */
class StdDeviationFilter(scc: SampleCleanContext,
                         maxDev: Double,
                         colName: String) extends OutlierRemovalAlgorithm with Serializable {

  /**
   * Convert x to a Double
   */
  def toDouble(x: Any): Double = {
    x match {
      case n: Number => n.doubleValue()
    }
  }

  override def removeOutliers(dataFrame: DataFrame): DataFrame = {
    val colIdx = dataFrame.columns.indexOf(colName) // Get the column index of this column
    val univariateData: RDD[Double] = dataFrame.map({case x: Row => toDouble(x(colIdx))})

    // Calculate the mean and standard deviation of the column
    val mean: Double = univariateData.mean
    val stdDev: Double = univariateData.stdev

    // Only keep those columns that are less than maxDev standard deviations from the mean
    dataFrame.filter(dataFrame.col(colName) > mean - (maxDev * stdDev))
             .filter(dataFrame.col(colName) < mean + (maxDev * stdDev))
  }
}

object StdDeviationFilter {
  /**
   * Creates a new StdDeviationFilter and applies it on the dataset
   */
  def removeOutliers(scc: SampleCleanContext, maxDev: Double,
                     columnName: String, dataFrame: DataFrame) = {
    new StdDeviationFilter(scc, maxDev, columnName).removeOutliers(dataFrame)
  }
}
