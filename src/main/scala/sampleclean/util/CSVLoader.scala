package sampleclean.util
import scala.util.Random
import sampleclean.api.SampleCleanContext

/**
 * A CSV loader loads a CSV file into a Hive Table.
 * 
 * @type {[type]}
 */
class CSVLoader(scc:SampleCleanContext, 
				schema:List[(String,String)],
				filename:String) 
				extends Loader(scc,schema)
{
	//val TABLEDEF = "CREATE TABLE IF NOT EXISTS %s ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
	val TABLEDEF = "CREATE TABLE IF NOT EXISTS %s ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'"
	val TABLELOAD = "LOAD DATA LOCAL INPATH '%t' OVERWRITE INTO TABLE %w"

	def load2Hive(namedSet:String=""):String = {
		val tmpTableName = if (namedSet == "") "tmp"+Math.abs((new Random().nextLong())) else namedSet
		scc.hql("add jar ./lib/csv-serde-1.1.2-0.11.0-all.jar")
		scc.hql(TABLEDEF.replace("%s", tmpTableName+schemaToSchemaList(schema)))
		scc.hql(TABLELOAD.replace("%t",filename).replace("%w",tmpTableName))
		return tmpTableName
	}

	def schemaToSchemaList(schema:List[(String,String)]):String = {
		var result = List[String]()
		for(s <- schema)
			result = s._1 + " " + s._2 :: result 

		return "("+result.reverse.mkString(",")+")"
	}
}