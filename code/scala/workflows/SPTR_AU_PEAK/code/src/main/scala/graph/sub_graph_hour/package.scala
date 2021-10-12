package graph

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object sub_graph_hour {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_reformat_0_out  = reformat_0(spark, in0)
    val df_aggregate_0_out = aggregate_0(spark)
    df_reformat_0_out
  }

}
