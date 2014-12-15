package sampleclean.activeml

/* SimpleApp.scala */

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DedupDemo {
  // set up a spark context
  val conf = new SparkConf()
    .setAppName("Active ML Driver")
    .setMaster("local[4]")
    .set("spark.executor.memory", "4g")
  val sc = new SparkContext(conf)

  // set up our parameters
  val svmParameters = SVMParameters() // Use defaults
  val frameworkParameters = ActiveLearningParameters(budget=60, batchSize=10, bootstrapSize=10)
  val labelGetterParameters = CrowdLabelGetterParameters() // Use defaults

  // Render Context for a deduplication task!
  val dedupGroupContext : GroupLabelingContext = DeduplicationGroupLabelingContext(
    taskType="er", data=Map("fields" -> List("id", "entity_id", "name", "address", "city", "type"))).asInstanceOf[GroupLabelingContext]

  // Render Context for a sentiment analysis task!
//  val throwawayContext : PointLabelingContext = new SentimentPointLabelingContext("This is a simple tweet")
//  val throwawayGroupContext : GroupLabelingContext = new SentimentGroupLabelingContext("sa", Map())

  // global state for our program
  val emptyLabeledRDD = sc.parallelize(new Array[(String, LabeledPoint)](0))
  var trainingData: RDD[(String, LabeledPoint, PointLabelingContext)] = null
  var testData: RDD[(String, LabeledPoint)]= null
  var trainN: Long = 0
  var testN: Long = 0

  /* Run an example program that does trains an active learning model to recognize duplicate restaurants. */
  def main(args: Array[String]) {

    // load some sample data
    // this file is a transformed version of restaurant.fv with everything for one pair of records on the same line.
    val data = sc.textFile("dedup_sample.txt")
    val parsedData = data.map { line =>
      val random_id = utils.randomUUID()
      val parts = line.split(',')
      val label = if (parts(0) == "dup") 1.0 else 0.0
      val entity1Data = parts.slice(1, 7).toList
      val entity2Data = parts.slice(7, 13).toList
      val fv = parts(13)
      val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data)).asInstanceOf[PointLabelingContext]
      (random_id, LabeledPoint(label, Vectors.dense(fv.split(' ').map(_.toDouble))), context)
    }
    //val parsedData = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 1000, 10, 0.1, 4)
    println(parsedData.count() + " data points")

    // split into train and test sets (80% / 20% respectively)
    val dataSplit = parsedData.randomSplit(Array(0.8, 0.2))
    trainingData = dataSplit(0).cache()
    testData = dataSplit(1).map {p=> p._1 -> p._2}.cache()
    trainN = trainingData.count()
    testN = testData.count()
    println("split data: " + trainN + " training points, " + testN + " test points")

    // hold out our labels
    val unlabeledTrainingData = trainingData.map(p => (p._1, p._2.features, p._3))

    // Train the active learning model.
    // This happens asynchronously, but a future-like object is returned immediately.
    // Use default parameters for the SVM.
    val trainingFuture = ActiveSVMWithSGD.train(
      emptyLabeledRDD, // empty RDD since we have no already-existing labeled data
      unlabeledTrainingData, // unlabeled data for training (has enough context to get labels)
      dedupGroupContext, // group context for getting labels, contains the schema
      svmParameters,
      frameworkParameters,
      new CrowdLabelGetter(labelGetterParameters), // label getter to get labels from the crowd.
      SVMMarginDistanceFilter)

    // add callbacks for new models and new data.
    trainingFuture.onNewModel(processNewModel)
    trainingFuture.onNewLabeledData(processNewLabeledData)
    trainingFuture.onComplete(() => println("All done! " + trainingFuture.getModels.size + " models trained."))

    // wait for training to complete
    Await.ready(trainingFuture, Duration.Inf)

    // see if we exited too early
    println("Got to here!")
    //System.exit(0)
  }

  /*
   * Callback for when new data is labeled by the crowd. This function just prints an acknowledgment.
   */
  def processNewLabeledData(newData: RDD[(String, Double)]) {
    println("Received " + newData.count() + " new labeled data points.")
  }

  /*
   * Callback for when a new model is trained during active learning.
   * This function trains a passive-learning model on the same number of training points, evaluates both the active and
   * passive models on our test set, and reports test error for the two models.
   */
  def processNewModel(model:SVMModel, modelN: Long) {
    // train an SVM w/out active learning for comparison
    val passive_model = SVMWithSGD.train(trainingData.sample(withReplacement=false, fraction=modelN.toDouble/trainN).map(p => p._2),
      svmParameters.numIterations, svmParameters.stepSize, svmParameters.regParam, svmParameters.miniBatchFraction)

    // run the models on the test data and build the confusion matrix
    val activeConfMat = (sc.accumulator(0.0), sc.accumulator(0.0), sc.accumulator(0.0), sc.accumulator(0.0))
    val passiveConfMat = (sc.accumulator(0.0), sc.accumulator(0.0), sc.accumulator(0.0), sc.accumulator(0.0))
    testData foreach { point =>
      val active_pred = model.predict(point._2.features)
      val passive_pred = passive_model.predict(point._2.features)
      active_pred -> point._2.label match {
        case (1.0, 1.0) => activeConfMat._1 += 1.0
        case (0.0, 0.0) => activeConfMat._2 += 1.0
        case (1.0, 0.0) => activeConfMat._3 += 1.0
        case (0.0, 1.0) => activeConfMat._4 += 1.0
        case _ => println("DIDN'T MATCH!!")
      }
      passive_pred -> point._2.label match {
        case (1.0, 1.0) => passiveConfMat._1 += 1.0
        case (0.0, 0.0) => passiveConfMat._2 += 1.0
        case (1.0, 0.0) => passiveConfMat._3 += 1.0
        case (0.0, 1.0) => passiveConfMat._4 += 1.0
      }
    }
    val activeConfusionMatrix = (activeConfMat._1.value, activeConfMat._2.value, activeConfMat._3.value, activeConfMat._4.value)
    val passiveConfusionMatrix = (passiveConfMat._1.value, passiveConfMat._2.value, passiveConfMat._3.value, passiveConfMat._4.value)
    println(activeConfusionMatrix)
    println(testData.first()._2.label)
    println(model.predict(testData.first()._2.features))
    println("New model trained on " + modelN + " points.")
    println("Active model:")
    println("\t error rate: " + error(activeConfusionMatrix))
    println("\t precision: " + precision(activeConfusionMatrix))
    println("\t recall: " + recall(activeConfusionMatrix))
    println("\t F-measure: " + f1(activeConfusionMatrix))
    println("Passive model:")
    println("\t error rate: " + error(passiveConfusionMatrix))
    println("\t precision: " + precision(passiveConfusionMatrix))
    println("\t recall: " + recall(passiveConfusionMatrix))
    println("\t F-measure: " + f1(passiveConfusionMatrix))
  }

  /**
   * Compute error rate (1 - accuracy), which is (TP + TN) / (P + N)
   * @param confusionMatrix the confusion matrix, a tuple of doubles for (TP, TN, FP, FN)
   * @return the error rate as a double.
   */
  def error(confusionMatrix: (Double, Double, Double, Double)): Double = {
    1.0 - (confusionMatrix._1 + confusionMatrix._2) /
      (confusionMatrix._1 + confusionMatrix._2 + confusionMatrix._3 + confusionMatrix._4)
  }

  /**
   * Compute precision, which is TP / (TP + FP)
   * @param confusionMatrix the confusion matrix, a tuple of doubles for (TP, TN, FP, FN)
   * @return the precision as a Double.
   */
  def precision(confusionMatrix: (Double, Double, Double, Double)): Double = {
    confusionMatrix._1 / (confusionMatrix._1 + confusionMatrix._3)
  }

  /**
   * Compute recall, which is TP / (TP + FN)
   * @param confusionMatrix the confusion matrix, a tuple of doubles for (TP, TN, FP, FN)
   * @return the recall as a Double.
   */
  def recall(confusionMatrix: (Double, Double, Double, Double)): Double = {
    confusionMatrix._1 / (confusionMatrix._1 + confusionMatrix._4)
  }

  /**
   * Compute F-measure, which is 2TP / (2TP + FP + FN)
   * @param confusionMatrix the confusion matrix, a tuple of doubles for (TP, TN, FP, FN)
   * @return the F-measure as a Double.
   */
  def f1(confusionMatrix: (Double, Double, Double, Double)): Double = {
    (2.0 * confusionMatrix._1) / (2.0 * confusionMatrix._1 + confusionMatrix._3 + confusionMatrix._4)
  }
}
