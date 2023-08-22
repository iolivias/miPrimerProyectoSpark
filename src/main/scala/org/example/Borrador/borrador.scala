package org.example.Borrador

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object borrador {

  def funcion1()(implicit sparkSession: SparkSession): Unit = {

    val mnmFile = "src/resources/mnm_dataset.csv"

    val mnmDF = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(mnmFile)

    // mnmDF.printSchema()

    // mnmDF.show(30, false)

    val groupDF = mnmDF.groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))

   // val miudf = udf

    groupDF.show()

  }

  def funcion2()(implicit sparkSession: SparkSession): Unit = {

    val mnmFile = "src/resources/mnm_dataset.csv"

    val mnmDF = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(mnmFile)

    val uniqueStates = mnmDF.select("State").distinct().collect().map(
      row => row.getString(0)
    )

      //_.getString(0))

    for (state <- uniqueStates) {
      val caCountMnMDF = mnmDF
        .select("State", "Color", "Count")
        .where(col("State") === state)
        .groupBy("State", "Color")
        .agg(sum("Count").alias("Total"))
        .orderBy(desc("Total"))

      println(s"Results for State: $state")
      caCountMnMDF.show()
    }
  }

}

