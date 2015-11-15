package sampleclean.clean.outlierremoval

import org.apache.spark.sql.DataFrame
import sampleclean.api.WorkingSet

/**
 * @author Viraj Mahesh
 *
 */
trait OutlierRemovalAlgorithm {

  /**
   * Removes outliers from a dataset. The algorithm for identifying outliers
   * depends on the implementing class.
   *
   * @param dataFrame The dataset that we are cleaning
   * @return A new DataFrame with outliers removed
   */
  def removeOutliers(dataFrame: DataFrame): DataFrame
}
