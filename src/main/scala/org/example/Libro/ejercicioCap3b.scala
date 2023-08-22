package org.example.Libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ejercicioCap3b {

  /* this function defines a schema programmatically for a DF with 7 columns: Id, First, Last, Url,
  Published, Hits and Campaign. Sets their data types and specifies that the columns can't have null values.
  Uses the Spark DataFrame API */
  def ejercicioCap3b()(implicit spark: SparkSession): Unit = {

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

    blogsDF.show()

    println(blogsDF.printSchema)
    println(blogsDF.schema)

  }

}



