package com.databeans.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object HistoryBuilder {

  def historyBuilder (historyLedMovedInAndLedAddress: DataFrame): DataFrame = {

    val hasSameAddressFilter= historyLedMovedInAndLedAddress.filter(col("address") ===  col("led_address") and  col("moved_out")=== col("led_moved_in"))
    val historyLeftAntiResult = historyLedMovedInAndLedAddress.join(hasSameAddressFilter, Seq("id"), "left_anti")
    val unionSameAddressFilterWithHistoryLeftAntiResult = hasSameAddressFilter.union(historyLeftAntiResult)
    val dfWithNewCurrentValue = unionSameAddressFilterWithHistoryLeftAntiResult.withColumn("update_value_current", when(isnull(col("moved_out")), true).otherwise(false))
    val buildResult=dfWithNewCurrentValue.select(
         col("id"),
         col("firstName"),
         col("lastName"),
         col("address"),
         col("moved_in"),
         col("led_moved_in").as("moved_out"),
        col("update_value_current").as("current"),
       )

    buildResult
  }

}
