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
        var writer = in.write.format("jdbc")
        "fasfsa".foreach { url =>
          writer = writer.option("url", url)
        }
        writer = writer.option("dbtable", "faf")
        writer = writer.option(
          "user",
          dbutils.secrets.get(scope = "fasf", key = "username")
        )
        writer = writer.option(
          "password",
          dbutils.secrets.get(scope = "fasf", key = "password")
        )
        writer = writer.option("driver", "akjsdhkja")
        writer.save()
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
