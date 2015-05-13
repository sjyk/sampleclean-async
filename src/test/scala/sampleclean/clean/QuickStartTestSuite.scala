package sampleclean.clean

import org.scalatest.FunSuite
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions.Row

class QuickStartTestSuite extends FunSuite with LocalSCContext{

    test("load tables") {
      withSampleCleanContext {scc =>
        scc.hql("DROP TABLE IF EXISTS restaurant")

        scc.hql("CREATE TABLE restaurant(id String, entity String,name String,category String,city String) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")

        assert(scc.hql("SHOW TABLES").collect().contains(Row("restaurant")))

        scc.hql("LOAD DATA LOCAL INPATH './src/test/resources/restaurant.csv' OVERWRITE INTO TABLE restaurant")
        
        assert(scc.hql("SELECT COUNT(*) FROM restaurant").collect()(0).getLong(0) == 858L)
      }
    }

    test("initialize"){
      withSampleCleanContext { scc =>
        scc.initialize("restaurant", "restaurant_working")
        assert(scc.hql("select count(distinct name) from restaurant").collect()(0).getLong(0) == 776L)
      }
    }

    test("Entity Resolution"){
        withSampleCleanContext { scc =>
          //scc.initialize("restaurant", "restaurant_working")
          import sampleclean.clean.deduplication.EntityResolution

          val algorithm = EntityResolution.longAttributeCanonicalize(scc, "restaurant_working", "name", 0.7)

          algorithm.exec()
          assert(scc.getCleanSampleAttr("restaurant_working", "name").map(x => (x.getString(1), x.getString(0))).groupByKey().count() == 761)

          assert(scc.hql("select count(distinct name) from restaurant").collect()(0).getLong(0) == 776L)
          scc.writeToParent("restaurant_working")
          assert(scc.hql("select count(distinct name) from restaurant").collect()(0).getLong(0) == 761L)
        }

    }

    test("clear tables"){
      withSampleCleanContext { scc =>
        // clear temp tables
        scc.closeHiveSession()

        // clear other tables
        scc.hql("DROP TABLE restaurant")
        assert(scc.hql("SHOW TABLES").collect().forall(!_.getString(0).contains("restaurant")))
      }
    }

  }












