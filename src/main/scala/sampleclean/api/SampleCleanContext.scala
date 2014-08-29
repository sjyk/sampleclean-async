package sampleclean.api

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.util.Random

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import SampleCleanContext._

/** As an analog to the SparkContext, the SampleCleanContext
*gives a handle to the current session. This class provides
*the basic API to manipulate the data structures. We assume
*that the data is initially in a HIVE store.
*  
*In its current implementation, the SampleCleanContext
*supports both persistent data in HIVE or keeping the data
*in memory as an RDD. 
*/
@serializable
class SampleCleanContext(sc: SparkContext) {

	//Use these functions to access the Spark
	//and Hive contexts in API calls.

	/**Returns the HiveContext
	*/
	def getHiveContext():HiveContext = {
		return new HiveContext(sc)
	}

	/**Returns the SparkContext
	*/
	def getSparkContext():SparkContext = {
		return sc
	}

	//There are three initialize functions: initializeHive, initializeAnon, and initialize
	//Initialize functions create samples with the following schema
	//col1: Hash <String>
	//col2: Duplicate Count <Int>
	//col3....colN other attributes

	/**This function initializes the base samples in Hive
	 * by convention we denote a clean sample as a new table
	 * _clean, and the dirty sample as a new table _dirty.
	 */
	def initializeHive(baseTable:String, tableName: String, samplingRatio: Double) = {

		val hiveContext = new HiveContext(sc)
		//creates the clean sample using table sampling procedure
		//when databricks gives us a better implementation of sampling
		//we can use that. 
		var query = "CREATE TABLE " + tableName + 
		            "_clean as SELECT reflect(\"java.util.UUID\", \"randomUUID\")" + 
		            " as hash,1 as dup, * FROM " +  
		            baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 

		hiveContext.hql(query)

		query = "CREATE TABLE " + tableName + 
		        "_dirty as SELECT reflect(\"java.util.UUID\", \"randomUUID\")"+
		        " as hash,1 as dup, * FROM " + 
		        baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 
		
		hiveContext.hql(query)
	}

    /*This function initializes the clean and dirty samples as
    * Schema RDD's in a tuple (Clean, Dirty). As opposed to initializeHive 
    * this is a synchronous functional approach
    * that does not change any SparkContext state.
    */
	def initializeAnon(baseTable:String, samplingRatio: Double): (SchemaRDD, SchemaRDD) = {

		val hiveContext = new HiveContext(sc)
	   
		val query1 = "SELECT reflect(\"java.util.UUID\", \"randomUUID\")" + 
		            " as hash,1 as dup, * FROM " +  
		            baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 

		val query2 = "SELECT reflect(\"java.util.UUID\", \"randomUUID\")"+
		        " as hash,1 as dup, * FROM " + 
		        baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 
		
		return (hiveContext.hql(query1), hiveContext.hql(query2))
	}

    /* This approach is comparable to initializeHive, but instead of
     * writing a HIVE table it registers the SchemaRDD with SparkSQL
     * (In development)
     */
	def initialize(baseTable:String, tableName: String, samplingRatio: Double) = {

		val hiveContext = new HiveContext(sc)
	   
		val query1 = "SELECT reflect(\"java.util.UUID\", \"randomUUID\")" + 
		            " as hash,1 as dup, * FROM " +  
		            baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 

		val query2 = "SELECT reflect(\"java.util.UUID\", \"randomUUID\")"+
		        " as hash,1 as dup, * FROM " + 
		        baseTable + " TABLESAMPLE("+samplingRatio+"PERCENT)" 
		
		hiveContext.registerRDDAsTable(hiveContext.hql(query1),tableName+"_clean")
		hiveContext.registerRDDAsTable(hiveContext.hql(query2),tableName+"_dirty")
	}

	/* This function cleans up after using initializeHive by dropping
	 * the tables. If you use hive and don't execute this command, 
	 * the samples can persist between sessions as it will be written
	 * to disk.
	 */
	def closeHiveSession(tableName: String) = {
		val hiveContext = new HiveContext(sc)
	    
	    hiveContext.hql("DROP TABLE IF EXISTS " + tableName + "_clean")

		hiveContext.hql("DROP TABLE IF EXISTS " + tableName +"_dirty")
	}

	/* Returns an RDD which points to a full table
	*/
	def getFullTable(tableName: String):SchemaRDD = {

		val hiveContext = new HiveContext(sc)

		return hiveContext.hql("SELECT * FROM " + tableName);
	}

	/* Returns an RDD which points to a sample 
	 * of a full table.
	 */
	def getCleanSampleRow(tableName: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.hql("SELECT * FROM " + tableName +"_clean");
	}

	/* Returns an RDD which points to a sample 
	 * of a full table. This method returns a tuple
	 * of the hash and the requested attribute
	 */
	def getCleanSampleAttr(tableName: String, attr: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.hql("SELECT hash, "+attr+" FROM " + tableName +"_clean");
	}

	/**This function given a schemardd of rows (col1 = hash, col2 = dup_count)
	 * it updates the corresponding Hive table. This only works if initializeHive
	 * has been called first.
	 */
	def updateHiveTableDuplicateCounts(tableName: String, rdd:SchemaRDD)= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = tableName + "_clean"
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceDupSchema(rdd)),"tmp")
		hiveContext.hql("CREATE TABLE " + tmpTableName +" as SELECT * FROM tmp")

		//Uses the hive API to get the schema of the table.
		var selectionString = ""

		for (field <- getHiveTableSchema(tableName+"_clean"))
		{
			if (field.equals("dup"))
				selectionString = selectionString + " , " + "coalesce(" + tmpTableName + ".dup,1)"
			else if (selectionString == "")
				selectionString = tableNameClean +"."+field
			else
				selectionString = selectionString + " , " + tableNameClean +"."+field
		}

   		//applies hive query to update the data
   		hiveContext.hql("INSERT OVERWRITE TABLE "+ 
   			            tableNameClean +" SELECT "+ 
   			            selectionString+" FROM "+
   			            tableNameClean+" LEFT OUTER JOIN " + 
   			            tmpTableName+" ON ("+
   			            tmpTableName+".hash = "+tableNameClean+".hash)")

   		//drop temporary table
   		hiveContext.hql("DROP TABLE " + tmpTableName)
	}

	/**This function given a schemardd of rows (col1 = hash)
	 * keeps only rows in the schemardd. It updates the HIVE
	 * table with the new sample.
	 */
	def filterHiveTable(tableName: String, rdd:SchemaRDD)= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = tableName + "_clean"
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceFilterSchema(rdd)),"tmp")

		hiveContext.hql("CREATE TABLE " + tmpTableName +" as SELECT hash FROM tmp")
		
		hiveContext.hql("INSERT OVERWRITE TABLE "+ tableNameClean +
			            " SELECT "+tableNameClean+".* FROM " +
			            tableNameClean+
			            " JOIN " + tmpTableName +
			            " ON ("+tmpTableName+".hash = "+
			            tableNameClean+".hash)")

		hiveContext.hql("DROP TABLE " + tmpTableName)
	}

	/**This function given a schemardd of rows (col1 = hash)
	* keeps only rows in the schemardd. Unlike before this
	* returns a SchemaRDD instead of updating the table
	*/
	def filterTable(tableName: String, rdd:SchemaRDD):SchemaRDD= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = tableName + "_clean"
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceFilterSchema(rdd)),"tmp")

		hiveContext.hql("CREATE TABLE " + tmpTableName +" as SELECT * FROM tmp")
		
		val result = hiveContext.hql("SELECT "+tableNameClean+ ".* FROM "
			         +tableNameClean+" JOIN " 
			         + tmpTableName+
			         " ON ("+tmpTableName+".hash = "+
			         tableNameClean+".hash)")

		return result
	}

	/**This function given a schemardd of rows (col1 = hash, col2 = dup_count)
	 * returns an updated RDD. 
	 */
	def updateTableDuplicateCounts(tableName: String, rdd:SchemaRDD):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = tableName + "_clean"
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceDupSchema(rdd)),"tmp")
		hiveContext.hql("CREATE TABLE " + tmpTableName +" as SELECT * FROM tmp")

		var selectionString = ""

		for (field <- getHiveTableSchema(tableName+"_clean"))
		{
			if (field.equals("dup")) {
					selectionString = selectionString + 
					                  " , " + "coalesce("+					                  
					                  	tmpTableName + ".dup,1) as dup"
			}
			else if (selectionString == ""){
				selectionString = tableNameClean + 
				                  "."+field + 
				                  " as " + field
			}
			else{
				selectionString = selectionString + 
				                  " , " + tableNameClean + 
				                  "."+ field + " AS " + field
			}
		}

   		val result = hiveContext.hql("SELECT " + selectionString + 
   			                         " FROM "+tableNameClean +
   			                         " LEFT OUTER JOIN " + tmpTableName +
   			                         " ON ("+tmpTableName+".hash = "
   			                         +tableNameClean+".hash)")

   		return result
	}

}

@serializable
object SampleCleanContext {

	def getHiveTableSchema(tableName:String):List[String] = {
		var schemaList = List[String]()
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			val sd:StorageDescriptor = msc.getTable(tableName).getSd();
			val fieldSchema = sd.getCols();
			for (field <- fieldSchema)
				schemaList = field.getName() :: schemaList 
		}
		catch {
     		case e: Exception => 0
   		}

		return schemaList.reverse
	}


	case class FilterTuple(hash: String)

	def enforceFilterSchema(rdd:SchemaRDD): RDD[FilterTuple] = {
		return rdd.map( x => FilterTuple(x(0).asInstanceOf[String]))
	}

	case class DupTuple(hash: String, dup: Int)

	def enforceDupSchema(rdd:SchemaRDD): RDD[DupTuple] = {
		return rdd.map( x => DupTuple(x(0).asInstanceOf[String],x(1).asInstanceOf[Int]))
	}

}
