package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object target_0 {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "default" =>
        import spark.implicits._
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        in.write
          .format("jdbc")
          .option("url",     "fasfsa")
          .option("dbtable", "faf")
          .option("user",    dbutils.secrets.get(scope = "fasf", key = "username"))
          .option("password",
                  dbutils.secrets.get(scope = "fasf", key = "password")
          )
          .option("driver", "akjsdhkja")
          .save()
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
