package org.example.Libro

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
object Ejercicio_Cap3 {

  def Ejercicio_Cap3()(implicit spark: SparkSession): Unit = {

    /* Create a DataFrame using SparkSession
  val spark = SparkSession
    .builder
    .appName("AuthorsAges")
    .getOrCreate() */

    // Create a DataFrame of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    // Show the results of the final execution
    avgDF.show()

  }
}