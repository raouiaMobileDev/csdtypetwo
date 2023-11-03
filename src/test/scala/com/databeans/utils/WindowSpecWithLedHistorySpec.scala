package com.databeans.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.databeans.utils.UnionHistoryAndUpdate.unionHistoryAndUpdate
import com.databeans.utils.WindowSpecWithLedMovedInAndAddress.windowSpecWithLedMovedInAndAddress
import com.databeans.models._

class WindowSpecWithLedHistorySpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("HistoryBuilderDataFrame_Test")
    .getOrCreate()

  import spark.implicits._

  "UnionHistoryAndUpdateResult" should "Trait data within the DataFrame" in {
    Given("The input dataframes")
    val inputHistoryData = Seq(
      History(5, "Bilel", "Ben amor", "Sfax", "2020-12-01", null, true),
      History(1, "Iheb", "Ben amor", "Sfax", "2010-05-01", "2012-07-01", false)
    ).toDF()
    val inputUpdateData = Seq(Update(5, "Bilel", "Ben amor", "Sfax", "2015-12-12")
    ).toDF()
    When("unionHistoryAndUpdate function is invoked")
    val resultUnionHistoryAndUpdateData = unionHistoryAndUpdate(inputHistoryData,inputUpdateData)
    val resultWindowSpecWithLedMovedInData= windowSpecWithLedMovedInAndAddress(resultUnionHistoryAndUpdateData)
    Then("The dataframe should be returned")
    val expectedResultData= Seq(
      LedMovedInAndLedAddress(5, "Bilel", "Ben amor", "Sfax", "2015-12-12",  null, false,"2020-12-01","Sfax"),
      LedMovedInAndLedAddress(5, "Bilel", "Ben amor", "Sfax", "2020-12-01",  null, true, null,null)
    ).toDF()
    expectedResultData.collect() should contain theSameElementsAs (resultWindowSpecWithLedMovedInData.collect())
  }
  "WindowSpecWithLedMovedInResult" should "Trait data within the DataFrame" in {
    Given("The input dataframes")
    val inputHistoryData = Seq(
      History(1, "Mohsen", "Abidi", "Sousse", "1995-12-23", "2020-12-12", false),
      History(10, "Iheb", "Ben amor", "Sfax", "2010-05-01", "2012-07-01", false)
    ).toDF()
    val inputUpdateData = Seq(Update(1, "Mohsen", "Abidi", "Tunis", "2021-01-01")
    ).toDF()
    When("windowSpecWithLedMovedIn function is invoked")
    val resultUnionHistoryAndUpdateData = unionHistoryAndUpdate(inputHistoryData,inputUpdateData)
    val resultWindowSpecWithLedMovedInData= windowSpecWithLedMovedInAndAddress(resultUnionHistoryAndUpdateData)
   Then("The dataframe should be returned")
    val expectedResultData= Seq(
      LedMovedInAndLedAddress(1, "Mohsen", "Abidi", "Sousse", "1995-12-23",  "2020-12-12", false, "2021-01-01", "Tunis"),
      LedMovedInAndLedAddress(1, "Mohsen", "Abidi", "Tunis", "2021-01-01", null, false, null, null),
    ).toDF()
    expectedResultData.collect() should contain theSameElementsAs (resultWindowSpecWithLedMovedInData.collect())
  }

}
