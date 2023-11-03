package com.databeans

import org.apache.spark.sql.DataFrame
import com.databeans.utils.UnionHistoryAndUpdate.unionHistoryAndUpdate
import com.databeans.utils.WindowSpecWithLedMovedInAndAddress.windowSpecWithLedMovedInAndAddress
import com.databeans.utils.HistoryBuilder.historyBuilder
import org.apache.spark.sql.functions.col


object CSDTypeTwoBuilder {

  def cSDTypeTwoBuilder(history: DataFrame, update: DataFrame): DataFrame={
    val resultUnionHistoryAndUpdate = unionHistoryAndUpdate(history, update)
    val resultWindowSpecWithLedMovedInAndAddress = windowSpecWithLedMovedInAndAddress(resultUnionHistoryAndUpdate)
    val resultHistoryBuilder=historyBuilder(resultWindowSpecWithLedMovedInAndAddress)
    val resultFromPickedHistory = history.as("history").join(resultHistoryBuilder.as("resultHistoryBuilder"), Seq("id"),"left_anti")
      .select(
        col("history.id"),
        col("history.firstName"),
        col("history.lastName"),
        col("history.address").as("address"),
        col("history.moved_in").as("moved_in"),
        col("history.moved_out").as("moved_out"),
        col("history.current").as("current")
      )
    resultHistoryBuilder.union(resultFromPickedHistory)
  }

}
