package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object set_operation_0 {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame = {
    def unionWithSchema(
      dataFrame:      DataFrame,
      otherDataFrame: DataFrame
    ): DataFrame =
      dataFrame.union(
        otherDataFrame
          .select(dataFrame.columns.head, dataFrame.columns.tail: _*)
      )
    def unionAll(df: DataFrame*): DataFrame =
      df.reduce((df1, df2) => unionWithSchema(df1, df2))
    unionAll(in0, in1)
  }

}
