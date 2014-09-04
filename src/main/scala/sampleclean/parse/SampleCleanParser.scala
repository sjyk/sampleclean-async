package sampleclean.parse

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanContext._

/* This class parses a command 
*  it takes an SC command argument
*  and tokenizes it.
*/
class SampleCleanParser(scc: SampleCleanContext) {

  /* This command returns a result and a code tuple.
   */
  def parseAndExecute(command:String):(String, Int) = {

  	System.out.println(command);

  	return ("", 0)

  }

}