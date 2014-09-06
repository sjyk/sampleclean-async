package sampleclean.activeml

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/*
 * Class to hold active learning training state.
 * M is the type of model being trained.
 * Training code should call addModel and addLabeledData whenever new models or data are available.
 */
class ActiveLearningTrainingState[M](trainingFuture: ActiveLearningTrainingFuture[M]) {
  var models = new ArrayBuffer[(M, Long)]()
  var labeledData: RDD[(String, Double)] = null

  /*
   * Register a new model with the training state.
   */
  def addModel(model: M, trainN: Long) = {
    models += model -> trainN
    trainingFuture.newModel(model, trainN)
  }

  /*
   * Register new labeled data with the training state.
   */
  def addLabeledData(data: RDD[(String, Double)]) = {
    if (labeledData != null) {
      labeledData = labeledData union data
    } else {
      labeledData = data
    }
    trainingFuture.newLabeledData(data)
  }

}
