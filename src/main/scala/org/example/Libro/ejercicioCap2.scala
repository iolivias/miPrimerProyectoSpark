package org.example.Libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejercicioCap2 {

  // ejercicio M&M del capítulo 2 del Libro
  def ejercicioCap2()(implicit spark: SparkSession) {

    // Get the M&M data set filename
    val mnmFile = "src/resources/mnm_dataset.csv"

    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mnmFile)
    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()
    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }


}
