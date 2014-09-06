package sampleclean.activeml

/* SimpleApp.scala */

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write => swrite}

object ActiveDriver {
  // set up a spark context
  val conf = new SparkConf().setAppName("Active ML Driver")
  val sc = new SparkContext(conf)

  // set up our parameters
  val svmParameters = SVMParameters() // Use defaults
  val labelGetterParameters = CrowdLabelGetterParameters() // Use defaults

  // Render Context for a deduplication task!
  val throwawayContext : PointLabelingContext = new DeduplicationPointLabelingContext(List(List("80.0", "London"), List("80", "london")))
  val throwawayGroupContext : GroupLabelingContext = new DeduplicationGroupLabelingContext("er", Map("fields" -> List("Price", "Location")))

  // Render Context for a sentiment analysis task!
//  val throwawayContext : PointLabelingContext = new SentimentPointLabelingContext("This is a simple tweet")
//  val throwawayGroupContext : GroupLabelingContext = new SentimentGroupLabelingContext("sa", Map())

  // global state for our program
  val emptyLabeledRDD = sc.parallelize(new Array[(String, LabeledPoint)](0))
  var trainingData: RDD[(String, LabeledPoint)] = emptyLabeledRDD
  var testData: RDD[(String, LabeledPoint)]= emptyLabeledRDD
  var trainN: Long = 0
  var testN: Long = 0

  /*
   * Run an example program that does 'active learning' on generated svm data.
   * Right now it doesn't actually get good labels for the data, it just shows the crowd a dummy tweet for sentiment
   * analysis.
   */
  def main(args: Array[String]) {

    // load some sample data
    val data = sc.textFile("/home/ubuntu/ActiveML/active_learn/data/svm-generated-1000x10.txt")
    val parsedData = data.map { line =>
      val random_id = utils.randomUUID()
      val parts = line.split(',')
      (random_id, LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble))))
    }
    //val parsedData = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 1000, 10, 0.1, 4)
    println(parsedData.count() + " data points")

    // split into train and test sets (80% / 20% respectively)
    val dataSplit = parsedData.randomSplit(Array(0.8, 0.2))
    trainingData = dataSplit(0)
    testData = dataSplit(1)
    trainN = trainingData.count()
    testN = testData.count()
    println("split data: " + trainN + " training points, " + testN + " test points")

    // hold out our labels
    val unlabeledTrainingData = trainingData.map(p => (p._1, p._2.features, throwawayContext))

    // Train the active learning model.
    // This happens asynchronously, but a future-like object is returned immediately.
    // Use default parameters for the SVM.
    val trainingFuture = ActiveSVMWithSGD.train(
      emptyLabeledRDD, // empty RDD since we have no already-existing labeled data
      unlabeledTrainingData, // unlabeled data for training (has enough context to get labels)
      throwawayGroupContext, // group context for getting labels, not used for sentiment analysis
      svmParameters,
      new CrowdLabelGetter(labelGetterParameters), // label getter to get labels from the crowd.
      SVMMarginDistanceFilter)

    // add callbacks for new models and new data.
    trainingFuture.onNewModel(processNewModel)
    trainingFuture.onNewLabeledData(processNewLabeledData)
    trainingFuture.onComplete(() => println("All done! " + trainingFuture.getModels.size + " models trained."))

    // wait for training to complete
    Await.ready(trainingFuture, Duration.Inf)
  }

  /*
   * Callback for when new data is labeled by the crowd. This function just prints an acknowledgment.
   */
  def processNewLabeledData(newData: RDD[(String, Double)]) {
    println("Callback called for " + newData.count() + " new labeled data points.")
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

    // run the models on the test data
    val labelAndPreds = testData.map { point =>
      val active_pred = model.predict(point._2.features)
      val passive_pred = passive_model.predict(point._2.features)
      (point._2.label, active_pred, passive_pred)
    }

    // report model accuracy
    val activeTestErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testN
    val passiveTestErr = labelAndPreds.filter(r => r._1 != r._3).count().toDouble / testN
    println("New model trained on " + modelN + " points. Test Error = " + activeTestErr + " (active), " + passiveTestErr + " (passive).")
  }
}
