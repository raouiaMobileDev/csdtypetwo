package com.databeans.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.databeans.utils.UnionHistoryAndUpdate.unionHistoryAndUpdate
import com.databeans.models._

class UnionHistoryAndUpdateSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
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
    Then("The dataframe should be returned")
        val expectedResultData= Seq(
          History(5, "Bilel", "Ben amor", "Sfax", "2020-12-01",  null, true),
          History(5, "Bilel", "Ben amor", "Sfax", "2015-12-12",  null, false)
        ).toDF()
    expectedResultData.collect() should contain theSameElementsAs (resultUnionHistoryAndUpdateData.collect())
  }

}
