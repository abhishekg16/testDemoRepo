package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object my_lookup_1 {

  def apply(spark: SparkSession, in0: DataFrame): Unit =
    createLookup("MyLookup1", in0, spark, List("customer_id"), "first_name")

}
