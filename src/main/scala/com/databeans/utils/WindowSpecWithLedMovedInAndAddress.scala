package com.databeans.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

object WindowSpecWithLedMovedInAndAddress {

  def windowSpecWithLedMovedInAndAddress (unionHistoryAndUpdate: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("id").orderBy(col("moved_in").asc)
    val ledMovedIn= unionHistoryAndUpdate
      .withColumn("led_moved_in", lead("moved_in", 1).over(windowSpec))
      .withColumn("led_address", lead("address", 1).over(windowSpec))
    ledMovedIn
  }

}
