package org.example.Libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object ejercicioCap3c {

  // working with DF columns

  def ejercicioCap3c()(implicit spark: SparkSession): Unit = {

    val blogsFile = "src/resources/blogs.json"

    val schema = StructType(Array(
      StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaign", ArrayType(StringType), false)
    ))

    val blogsDF = spark.read.schema(schema).json(blogsFile)

    // no result
    blogsDF.columns

    // Access a particular column with col and it returns a Column type - no result
    blogsDF.col("Id")

    // Use an expression to compute a value
    // select + expr
    blogsDF.select(expr("Hits * 2")).show(2)

    // or use col to compute value
    // select + col
    blogsDF.select(col("Hits") * 2).show(2)

    // Use an expression to compute big hitters for blogs
    // This adds a new column, Big Hitters, based on the conditional expression
    // withColumn + expr
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    // Concatenate three columns, create a new column, and show it
    // withColumn + concat  + expr
    // select + col
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)

    // These statements return the same value, showing that
    // expr is the same as a col method call
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)

    // Sort by column "Id" in descending order
    // sort + col
    blogsDF.sort(col("Id").desc).show()

  }
}