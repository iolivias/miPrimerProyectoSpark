package org.example.Libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejercicioCap4 {

  // working with SQL queries on a temporary view

  def ejercicioCap4()(implicit spark: SparkSession): Unit = {

    val csvFile = "src/resources/departuredelays.csv"

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val df = spark.read.format("csv")
      .option(schema, "true")
      .option("header", "true")
      .load(csvFile)

    // print schema to check data types are ok

    df.printSchema()

    // create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // query the temporary view and display the results
    /* val query = "SELECT * FROM us_delay_flights_tbl"
    val resultDF = spark.sql(query)
    resultDF.show(100)*/

    // query 1 with spark methods and functions

    val resultDF = spark.read.table("us_delay_flights_tbl")
    resultDF.show(100)

    // use the limit function
    val limitedResultDF = resultDF.limit(10)
    limitedResultDF.show()

    // find 50 flights whose distance is greater than 1000 miles
    /* val query2 = "SELECT * FROM us_delay_flights_tbl" +
      " WHERE distance > 1000"
    val query2DF = spark.sql(query2)
    query2DF.show(50) */

    // query 2 with spark methods and functions

    import spark.implicits._
    val query2DF = resultDF.where($"distance" > 1000)
    query2DF.show(50)

    // get to know how many resulting rows-flights there are
    val rowCount = query2DF.count()

    // print the resulting number of rows with an interpolated string
    println(s"Number of resulting rows: $rowCount")

    // find 10 flights with longest distance - check origin and destination airports
    /* val query3 = "SELECT * FROM us_delay_flights_tbl" +
      " WHERE distance > 1000 ORDER BY distance DESC"
    val query3DF = spark.sql(query3)
    query3DF.show(10) */

    // query 3 with spark methods and functions

    val query3DF = resultDF
      .filter($"distance" > 1000)
      .orderBy($"distance".desc)
      .limit(10)
    query3DF.show()

    // find all flights between SFO and ORD with at least 2 hour delay
    /* val query4 = "SELECT * FROM us_delay_flights_tbl" +
      " WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'"
    val query4DF = spark.sql(query4)
    query4DF.show(50)*/

    // query 4 with spark methods and functions

    val query4DF = resultDF
      .filter($"delay" > 120 && $"origin" === "SFO" && $"destination" === "ORD")
    query4DF.show(50)

    // convert the date column into a readable format and find the days or months when
    // these delays were most common - where they related to winter or holidays?

   /* val query5 = "SELECT *, TO_DATE(LEFT(date, 4), 'MMdd') as readable_date" +
      " FROM us_delay_flights_tbl"
    val query5DF = spark.sql(query5)
    query5DF.show(50)*/

    // query 5 with spark methods and functions

    val query5DF = resultDF
      .withColumn("readable_date", to_date(substring($"date", 1, 4), "MMdd"))
    query5DF.show(50)

    // label all US flights with an indication of the delays they experienced:
    // Very Long Delays > 6 hours, Long Delays 2-6 hours, Short Delays 1-2 hours, Tolerable Delays < 1 hour
    // Add these to a new column called Flight_Delays

    /* val query6 = "SELECT *, " +
    "CASE " +
      "WHEN delay >= 360 THEN 'Very Long Delays' " +
      "WHEN delay < 360 AND delay >= 120 THEN 'Long Delays' " +
      "WHEN delay < 120 AND delay >= 60 THEN 'Short Delays' " +
      "WHEN delay < 60 AND delay > 0 THEN 'Tolerable Delays' " +
      "WHEN delay = 0 THEN 'No Delay' " +
      "WHEN delay < 0 THEN 'Early' " +
      "END AS Flight_Delays " +
      "FROM us_delay_flights_tbl"
    val query6DF = spark.sql(query6)
    query6DF.show(50) */

    // query6 with spark methods and functions

    val query6DF = resultDF.withColumn("Flight_Delays",
      when(col("delay") >= 360, "Very Long Delays")
        .when(col("delay") < 360 && col("delay") >= 120, "Long Delays")
        .when(col("delay") < 120 && col("delay") >= 60, "Short Delays")
        .when(col("delay") < 60 && col("delay") > 0, "Tolerable Delays")
        .when(col("delay") === 0, "No Delay")
        .when(col("delay") < 0, "Early")
    )

    query6DF.show(50)


  }
}