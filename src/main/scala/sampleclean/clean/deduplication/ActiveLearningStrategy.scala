package sampleclean.clean.deduplication


import sampleclean.activeml._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import scala.concurrent.Await
import scala.concurrent.duration.Duration



case class ActiveLearningStrategy(featureVector: FeatureVector,
                             toPointLabelingContext: (Row, Row) => PointLabelingContext,
                             groupLabelingContext: GroupLabelingContext) {

  def asyncRun(labeledInput: RDD[(String, LabeledPoint)], candidatePairs: RDD[(Row, Row)], onUpdateDupCounts: RDD[(Row, Row)] => Unit) = {

    val candidatePairsWithId = candidatePairs.map((utils.randomUUID(), _)).cache()

    val unlabeledInput = candidatePairsWithId.map(p => (p._1, Vectors.dense(featureVector.toFeatureVector(p._2._1, p._2._2)), toPointLabelingContext(p._2._1, p._2._2)))
    val trainingFuture = ActiveSVMWithSGD.train(
      labeledInput,
      unlabeledInput,
      groupLabelingContext,
      SVMParameters(),
      ActiveLearningParameters(budget = 60, batchSize = 10, bootstrapSize = 10),
      new CrowdLabelGetter(CrowdLabelGetterParameters(maxPointsPerHIT = 100)),
      SVMMarginDistanceFilter)

    def processNewModel(model:SVMModel, modelN: Long) {
      val modelLabeledData: RDD[(String, Double)] = unlabeledInput.map(p => (p._1, model.predict(p._2)))
      var mergedLabeledData: RDD[(String, Double)] = modelLabeledData

      val crowdLabeledData = trainingFuture.getLabeledData
      println("crowdLabeledData")
      crowdLabeledData match {
        case None => // do nothing
        case Some(crowdData) =>
            //crowdData.collect().foreach(println)
            //println("modelLabeledData")
            //mergedLabeledData.collect().foreach(println)
          mergedLabeledData = modelLabeledData.leftOuterJoin(crowdData).map{
            case (pid, (modelLabel, None)) => (pid, modelLabel)
            case (pid, (modelLabel, Some(crowdLabel))) => (pid, crowdLabel)
        }
      }
      //println("mergedLabeledData")
      //mergedLabeledData.collect().foreach(println)
      assert(mergedLabeledData.count() == modelLabeledData.count())
      assert(mergedLabeledData.count() == candidatePairsWithId.count())

      val duplicatePairs = mergedLabeledData.filter(_._2 > 0.5).join(candidatePairsWithId).map(_._2._2) // 1: duplicate; 0: non-duplicate
      //println("DuplicatePairs")
      //duplicatePairs.collect().foreach(println)
      onUpdateDupCounts(duplicatePairs)
    }

    trainingFuture.onNewModel(processNewModel)

    // wait for training to complete
    Await.ready(trainingFuture, Duration.Inf)
  }



}
