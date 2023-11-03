package com.databeans.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object UnionHistoryAndUpdate {

  def unionHistoryAndUpdate(history: DataFrame, update: DataFrame): DataFrame = {

    val takeHistory = update.as("update")
      .join(history.as("history"), history("id") === update("id"), "left")
      .select(
        col("history.id"),
        col("history.firstName"),
        col("history.lastName"),
        col("history.address"),
        col("history.moved_in"),
        col("history.moved_out"),
        col("history.current"),
      )

    val adjustedUpdate = update
      .withColumn("moved_out", lit(null))
      .withColumn("current", lit(false))

    takeHistory.union(adjustedUpdate)
  }

}
