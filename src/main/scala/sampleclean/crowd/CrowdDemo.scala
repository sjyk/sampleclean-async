package sampleclean.crowd

/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}
import sampleclean.activeml._
import sampleclean.crowd.context.{GroupLabelingContext, PointLabelingContext, DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

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
      val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data))
      (random_id, context)
    }

    // sample a few points for labeling, and store the context so we can print it out later
    val crowdData = parsedData.sample(false, 0.01).cache()
    println(crowdData.count + " points in data set.")
    val contextMap = crowdData.collect() toMap

    // Group Context for a deduplication task with the restaurant schema.
    val groupContext = DeduplicationGroupLabelingContext(
      data=Map(
        "fields" -> List("id", "entity_id", "name", "address", "city", "type"),
        "instruction" -> "Decide whether two records in each group refer to the <b>same entity</b>.")
    )

    // set up the crowd parameters, using the default ports/hosts
    val crowdConfig = CrowdConfiguration(
      crowdName="internal"
    )
    val taskConfig = CrowdTaskConfiguration(
      maxPointsPerTask = 5,
      votesPerPoint = 1,
      crowdTaskOptions = Map(
        "sandbox" -> true,
        "title" -> "SampleClean Demo Crowd Task",
        "reward" -> 0.08,
        "description" -> "Decide whether or not two restaurants are the same.")
    )


    // Make the request, which returns asynchronously, and set up callbacks as the data processes
    val task = new DeduplicationTask()
    task.configureCrowd(crowdConfig)
    val resultObject = task.processStreaming(crowdData, groupContext, taskConfig)
    resultObject onTupleProcessed { tuple =>
      val label = if (tuple._2 == 1.0) "DUPLICATE" else "NOT DUPLICATE"
      contextMap.get(tuple._1) match {
        case Some(c) => println("Entity1: " + c.content(0) + ", Entity2: " + c.content(1) + ", Result: " + label)
        case _ => throw new RuntimeException("Unexpected tuple id: " + tuple._1)
      }
    }
    resultObject onBatchProcessed({tupleBatch => println("New batch processed!")}, 10)
    resultObject onComplete { () => println("All done!") }
  }
}
