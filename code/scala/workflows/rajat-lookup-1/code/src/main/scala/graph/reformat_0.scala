package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object reformat_0 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(lookup("MyLookup1", col("customer_id"))
                .getField("first_name")
                .as("lookedup"),
              col("last_name")
    )

}
