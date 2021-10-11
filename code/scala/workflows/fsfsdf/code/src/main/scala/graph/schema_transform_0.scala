package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object schema_transform_0 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.drop("cmls_3ds_authntn_mthd")
      .withColumn("new_col", col("cmls_aa_score_thrshld_excd"))

}
