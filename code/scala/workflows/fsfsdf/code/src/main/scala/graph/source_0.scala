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
        spark.read
          .format("parquet")
          .load(
            "dbfs:/FileStore/Users/MaciejV/ptr-data/tcmls_edwau_b1auth_dtl_1.parquet/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
