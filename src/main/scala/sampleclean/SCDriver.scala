package sampleclean

import sampleclean.activeml._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SchemaRDD, Row}

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import sampleclean.parse.SampleCleanParser;
//import sampleclean.parse.SampleCleanParser._;
import sampleclean.clean.ParametricOutlier;
import sampleclean.clean.dedup._

import org.apache.spark.sql.hive.HiveContext
import sampleclean.clean.dedup.WordTokenizer
import sampleclean.clean.dedup.BlockingStrategy
import sampleclean.clean.dedup.BlockingKey
import sampleclean.clean.dedup.ActiveLearningStrategy

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
  	parser.parseAndExecute(commandL);

  	return 0;
  }

  
  /*This is the main of the program which starts the application
  */
  def mainsc(args: Array[String]) {

  	val conf = new SparkConf();
    conf.setAppName("SampleClean Materialized View Experiments");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);
    val saqp = new SampleCleanAQP();
    val hiveContext = new HiveContext(sc);
    val p = new ParametricOutlier(scc);
    scc.closeHiveSession()
    scc.initializeHive("src","src_sample",0.01)
    /*p.clean("src_sample", "key", 1.8)
    println(saqp.normalizedSCQuery(scc, "src_sample", "key", "sum","true", 0.01))
    println(saqp.rawSCQuery(scc, "src_sample", "key", "sum","true", 0.01))
    p.clean("src_sample", "key", 1.0)
    println(saqp.normalizedSCQuery(scc, "src_sample", "key", "sum","true", 0.01))
     println(saqp.rawSCQuery(scc, "src_sample", "key", "sum","true", 0.01))*/
    println(saqp.normalizedSCQuery(scc, "src_sample", "key", "avg","true", 0.01))
    //hiveContext.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    //hiveContext.hql("LOAD DATA LOCAL INPATH 'kv1.txt' OVERWRITE INTO TABLE src")
    //hiveContext.hql("CREATE TABLE IF NOT EXISTS dblp (value STRING)")
    //hiveContext.hql("LOAD DATA LOCAL INPATH 'dblp.txt' OVERWRITE INTO TABLE dblp")
    /*scc.closeHiveSession("dblp_sample")
    scc.initializeHive("dblp","dblp_sample",0.001)
    hiveContext.hql("select count(*) from dblp").collect().foreach(println)
    hiveContext.hql("select count(*) from dblp_sample_clean").collect().foreach(println)*/

    //println(saqp.rawSCQuery(scc, "src_sample", "key", "count","key > 300", 0.1))
    //println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001)) // To Sanjay: predicate cannot be empty
    //p.clean("src_sample", "key", 1)


    val d = new Deduplication(scc);
    val sampleKey = new BlockingKey(Seq(2),WordTokenizer())
    val fullKey = new BlockingKey(Seq(0),WordTokenizer())

    //val f1 = new Feature(Seq(2), Seq(0), Seq("Jaro", "JaccardSimilarity"))
    //val f2 = new Feature(Seq(2), Seq(0), Seq("Levenshtein"))

    //d.clean("dblp_sample", BlockingStrategy("Jaccard", 0.8, sampleKey, fullKey), ActiveLearningStrategy(Seq(f1,f2)))
    println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001))
    
    //println(saqp.rawSCQuery(scc, scc.updateTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 2 as dup from src_sample_clean limit 10")), "key", "count","key > 300", 0.01))
    /*hiveContext.hql("select * from src_sample_clean").collect().foreach(println)*/
    //scc.filterWriteTable("src_sample", hiveContext.hql("select * from src_sample_clean limit 10"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)
    //scc.updateHiveTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 1 as dup from src_sample_clean"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)

// Queries are expressed in HiveQL
  //println(saqp.approxQuery(hiveContext, "src_sample_clean", "key", "count","key > 200", 0.1))

   /* val parser:SCParse = new SCParse(sc);

  	//The REPL of the program
	  var input = readLine(PROMPT);
	  var exit_status = run(input,parser);
	  while (exit_status == 0){
		  input = readLine(PROMPT);
		  exit_status = run(input,parser);
	  }*/

  }

  def maindup(args: Array[String]) {

    val conf = new SparkConf();
    conf.setAppName("SampleClean Materialized View Experiments");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);
    val saqp = new SampleCleanAQP();
    val hiveContext = new HiveContext(sc);

    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant (id STRING,entity_id STRING,name STRING,address STRING,city STRING,type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'restaurant.csv' OVERWRITE INTO TABLE restaurant")
    scc.closeHiveSession()
    scc.initializeHive("restaurant","restaurant_sample",1)
    hiveContext.hql("select count(*) from restaurant").collect().foreach(println)

    hiveContext.hql("select * from restaurant_sample_clean").collect().foreach(println)

    //println(saqp.rawSCQuery(scc, "src_sample", "key", "count","key > 300", 0.1))
    //println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001)) // To Sanjay: predicate cannot be empty
    //p.clean("src_sample", "key", 1)


    val d = new Deduplication(scc);
    val sampleKey = new BlockingKey(Seq(4,5,6,7),WordTokenizer())
    val fullKey = new BlockingKey(Seq(2,3,4,5),WordTokenizer())

    val sampleTable = scc.getCleanSample("restaurant_sample")
    val fullTable = scc.getFullTable("restaurant_sample")





    def toPointLabelingContext(sampleRow1: Row, fullRow2: Row): PointLabelingContext = {
      DeduplicationPointLabelingContext(List(List(sampleRow1.getString(2)), List(sampleRow1.getString(0))))
    }

    val throwawayGroupContext = new DeduplicationGroupLabelingContext("er", Map("fields" -> List("paper")))


    val f1 = new Feature(Seq(4), Seq(2), Seq("Levenshtein", "CosineSimilarity"))
    val f2 = new Feature(Seq(5), Seq(3), Seq("Levenshtein", "CosineSimilarity"))
    val f3 = new Feature(Seq(6), Seq(4), Seq("Levenshtein", "CosineSimilarity"))
    val f4 = new Feature(Seq(7), Seq(5), Seq("Levenshtein", "CosineSimilarity"))

    val featureVector = new FeatureVector(Seq(f1,f2,f3,f4))



    val similarPairs = d.clean("restaurant_sample", BlockingStrategy("Jaccard", 0.35, sampleKey, fullKey)).filter(x => x._1.getString(2).toInt < x._2.getString(0).toInt)

    println(similarPairs.filter(x => x._1.getString(3) == x._2.getString(1)).count)
    println(similarPairs.count)
    val results = similarPairs.map{case (row1, row2) =>

        val featurestr = featureVector.toFeatureVector(row1, row2).foldLeft("")((x,y) => x+" "+y).trim
        val rowstr1 = row1.getString(2)+","+row1.getString(3)+","+row1.getString(4)+","+row1.getString(5)+","+row1.getString(6)+","+row1.getString(7)
        val rowstr2 = row2.getString(0)+","+row2.getString(1)+","+row2.getString(2)+","+row2.getString(3)+","+row2.getString(4)+","+row2.getString(5)
        var flag = "dup"
        if (row1.getString(3) != row2.getString(1))
          flag = "nodup"
        (flag, rowstr1, rowstr2, featurestr)

    }.foreach{ case x=>
      println(x._1)
      println(x._2)
      println(x._3)
      println(x._4)
      println
    }





    //println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001))

    //println(saqp.rawSCQuery(scc, scc.updateTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 2 as dup from src_sample_clean limit 10")), "key", "count","key > 300", 0.01))
    /*hiveContext.hql("select * from src_sample_clean").collect().foreach(println)*/
    //scc.filterWriteTable("src_sample", hiveContext.hql("select * from src_sample_clean limit 10"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)
    //scc.updateHiveTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 1 as dup from src_sample_clean"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)

    // Queries are expressed in HiveQL
    //println(saqp.approxQuery(hiveContext, "src_sample_clean", "key", "count","key > 200", 0.1))

    /* val parser:SCParse = new SCParse(sc);

     //The REPL of the program
     var input = readLine(PROMPT);
     var exit_status = run(input,parser);
     while (exit_status == 0){
       input = readLine(PROMPT);
       exit_status = run(input,parser);
     }*/

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

    hiveContext.hql("CREATE TABLE IF NOT EXISTS dblp (value STRING)")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'dblp.txt' OVERWRITE INTO TABLE dblp")
    scc.closeHiveSession()
    scc.initializeHive("dblp","dblp_sample",0.001)
    hiveContext.hql("select count(*) from dblp").collect().foreach(println)
    hiveContext.hql("select count(*) from dblp_sample_clean").collect().foreach(println)

    //println(saqp.rawSCQuery(scc, "src_sample", "key", "count","key > 300", 0.1))
    //println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001)) // To Sanjay: predicate cannot be empty
    //p.clean("src_sample", "key", 1)


    val d = new Deduplication(scc);
    val sampleKey = new BlockingKey(Seq(2),WordTokenizer())
    val fullKey = new BlockingKey(Seq(0),WordTokenizer())


    def toPointLabelingContext(sampleRow1: Row, fullRow2: Row): PointLabelingContext = {
      DeduplicationPointLabelingContext(List(List(sampleRow1.getString(2)), List(sampleRow1.getString(0))))
    }

    val throwawayGroupContext = new DeduplicationGroupLabelingContext("er", Map("fields" -> List("paper")))


    val f1 = new Feature(Seq(2), Seq(0), Seq("Jaro", "JaccardSimilarity"))
    val f2 = new Feature(Seq(2), Seq(0), Seq("Levenshtein"))


    d.clean("dblp_sample",
      BlockingStrategy("Jaccard", 0.8, sampleKey, fullKey),
      ActiveLearningStrategy(new FeatureVector(Seq(f1,f2)), toPointLabelingContext, throwawayGroupContext))

    //println(saqp.normalizedSCQuery(scc, "dblp_sample", "value", "count","true", 0.001))

    //println(saqp.rawSCQuery(scc, scc.updateTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 2 as dup from src_sample_clean limit 10")), "key", "count","key > 300", 0.01))
    /*hiveContext.hql("select * from src_sample_clean").collect().foreach(println)*/
    //scc.filterWriteTable("src_sample", hiveContext.hql("select * from src_sample_clean limit 10"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)
    //scc.updateHiveTableDuplicateCounts("src_sample", hiveContext.hql("select src_sample_clean.hash as hash, 1 as dup from src_sample_clean"))
    //println(saqp.rawSCQuery(scc, "src_sample_clean", "key", "count","key > 200", 0.01))
    //hiveContext.hql("select * from src_sample_clean").collect().foreach(println)

    // Queries are expressed in HiveQL
    //println(saqp.approxQuery(hiveContext, "src_sample_clean", "key", "count","key > 200", 0.1))

    /* val parser:SCParse = new SCParse(sc);

     //The REPL of the program
     var input = readLine(PROMPT);
     var exit_status = run(input,parser);
     while (exit_status == 0){
       input = readLine(PROMPT);
       exit_status = run(input,parser);
     }*/

  }






}
