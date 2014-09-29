package sampleclean

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import sampleclean.parse.SampleCleanParser;
import org.apache.spark.sql.hive.HiveContext

/**
* This object provides the main driver for the SampleClean
* application. We execute commands read from the command 
* line.
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
  
  /**
   * run executes a command and prints the execution time in milliseconds
   * @param command a string whitch what command the user entered
   * @param parser a reference to the SampleCleanParser context
   * @return A status code
   */
  def run(command:String, parser:SampleCleanParser):Int = {

  	//force the string to lower case
  	val commandL = command.toLowerCase(); 
  	
    //exit
  	if (commandL == QUIT_COMMAND)
  		return 2;

  	//execute the command
  	val result = parser.parseAndExecute(commandL);
    println(result._2 + " (ms)")

  	return 0;
  }


  /**
   * Main function
   */
  def main(args: Array[String]) {

    val conf = new SparkConf();
    conf.setAppName("SampleClean Spark Driver");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);
    val saqp = new SampleCleanAQP();

    val parser:SampleCleanParser = new SampleCleanParser(scc,saqp);

    //println(scc.getHiveTableFullSchema("paper"))

    //The REPL of the program
    var input = readLine(PROMPT);
    var exit_status = run(input,parser);
    while (exit_status == 0){
      input = readLine(PROMPT);
      exit_status = run(input,parser);
    }

  }
  
}
