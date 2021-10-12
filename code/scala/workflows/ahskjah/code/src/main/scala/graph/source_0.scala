package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object source_0 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "default" =>
        import spark.implicits._
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        spark.read
          .format("jdbc")
          .option("url",
                  "jdbc:postgresql://test-database.cqu6jg6pmsfo.us-east-1.rds.amazonaws.com/asp"
          )
          .option("user",
                  dbutils.secrets.get(scope = "testGCred", key = "username")
          )
          .option("password",
                  dbutils.secrets.get(scope = "testGCred", key = "password")
          )
          .option("dbtable", "aspect")
          .option("driver",  "org.postgresql.Driver")
          .load()
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
