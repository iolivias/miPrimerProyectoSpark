package org.example.Libro

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

object ejercicioCap3 {

  def ejercicioCap3()(implicit spark: SparkSession): Unit = {

    // creates a DataFrame of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

    // groups the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    avgDF.show()

  }
}