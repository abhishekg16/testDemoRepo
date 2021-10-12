package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object lookup_0 {

  def apply(spark: SparkSession, in0: DataFrame): Unit =
    createLookup("Lookup_0", in0, spark, List("customer_id"), "first_name")

}
