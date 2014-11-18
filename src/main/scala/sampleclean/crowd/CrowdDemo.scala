package sampleclean.crowd

/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}
import sampleclean.activeml._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CrowdDemo {

  /* Run an example program that uses the crowd to recognize duplicate restaurants. */
  def main(args: Array[String]) {

    // set up a spark context
    val conf = new SparkConf()
      .setAppName("Crowd Demo")
      .setMaster("local[4]")
      .set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    // load some sample data
    // this file is a transformed version of restaurant.fv with everything for one pair of records on the same line.
    val data = sc.textFile("dedup_sample.txt")
    val parsedData = data.map { line =>
      val random_id = utils.randomUUID()
      val parts = line.split(',')
      val entity1Data = parts.slice(1, 7).toList
      val entity2Data = parts.slice(7, 13).toList
      val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data)).asInstanceOf[PointLabelingContext]
      (random_id, context)
    }

    // take just the first few points for labeling, and store the context so we can print it out later.
    val crowdData = parsedData.take(20)
    val contextMap = crowdData toMap

    // set up the crowd parameters
    val labelGetterParameters = CrowdLabelGetterParameters(crowdName = "amt", 
							   maxPointsPerHIT = 20, 
							   crowdConfig = Map("sandbox" -> true,
								 	     "title" -> "SampleClean",
									     "reward" -> 0.08,
									     "description" -> "This is an entity resolution task."))

    // Group Context for a deduplication task with the restaurant schema.
    val groupContext : GroupLabelingContext = DeduplicationGroupLabelingContext(
      taskType="er", 
      data=Map("fields" -> List("id", "entity_id", "name", "address", "city", "type"),
               "instruction" -> "Decide whether two records in each group refer to the <b>same entity</b>.")).asInstanceOf[GroupLabelingContext]

    // Make the request, which returns a future
    val groupId = utils.randomUUID() // random group id for the request.
    val resultFuture = CrowdHTTPServer.makeRequest(groupId, crowdData, groupContext, labelGetterParameters)

    // wait for the crowd to finish (in real usage, better to use future callbacks than Await)
    val result = Await.result(resultFuture, Duration.Inf)

    // print out our results
    result.answers foreach { answer =>
      val context = contextMap.getOrElse(answer.identifier, null).asInstanceOf[DeduplicationPointLabelingContext]
      val label = if (answer.value == 1.0) "DUPLICATE" else "NOT DUPLICATE"
      println("Entity1: " + context.content(0) + ", Entity2: " + context.content(1) + ", Result: " + label)
    }

    // quit.
    //System.exit(0)
  }
}
