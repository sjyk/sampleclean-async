package sampleclean

import sampleclean.activeml._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SchemaRDD, Row}

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanContext._;
import sampleclean.api.SampleCleanAQP;
import sampleclean.parse.SampleCleanParser;
//import sampleclean.parse.SampleCleanParser._;
import sampleclean.clean.outlier.ParametricOutlier;
import sampleclean.clean.misc._;
import sampleclean.clean.deduplication._


import org.apache.spark.sql.hive.HiveContext
import sampleclean.clean.algorithm.AlgorithmParameters
import sampleclean.clean.deduplication.WordTokenizer
import sampleclean.clean.deduplication.BlockingStrategy
import sampleclean.clean.deduplication.BlockingKey
import sampleclean.clean.deduplication.ActiveLearningStrategy

import sampleclean.clean.algorithm.SampleCleanPipeline
import sampleclean.clean.algorithm.SampleCleanPipeline._

/*This class provides the main driver for the SampleClean
* application. We execute commands read from the command 
* line
*/
object SCDriver {

  //Configure the prompt for the sampleclean system
  val PROMPT:String = "sampleclean> ";

  //Driver Key Words
  var QUIT_COMMAND:String = "quit";

  //Exit Codes
  val SUCCESS:Int = 0;
  val QUIT:Int = 1;
  val ERROR:Int = 2;
  
  /* This command executes input from the command line
  *  prompt. The execution returns an exit code which
  * determines the next action. 
  */
  def run(command:String, parser:SampleCleanParser):Int = {

  	//force the string to lower case
  	val commandL = command.toLowerCase(); 
  	
  	if (commandL == QUIT_COMMAND)
  		return 2;

  	//execute the command
  	val result = parser.parseAndExecute(commandL);
    println(result._2 + " (ms)")

  	return 0;
  }


  def main(args: Array[String]) {

    val conf = new SparkConf();
    conf.setAppName("SampleClean Materialized View Experiments");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);
    val saqp = new SampleCleanAQP();
    val hiveContext = new HiveContext(sc);

    val parser:SampleCleanParser = new SampleCleanParser(scc,saqp);

    //The REPL of the program
    var input = readLine(PROMPT);
    var exit_status = run(input,parser);
    while (exit_status == 0){
      input = readLine(PROMPT);
      exit_status = run(input,parser);
    }

  }
}
