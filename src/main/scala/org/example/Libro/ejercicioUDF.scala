package org.example.Libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejercicioUDF {

  def ejercicioLower()(implicit spark: SparkSession): Unit = {

    import spark.implicits._
    // esto hace falta? leí que hay que importarlo para poder utilizar to DF o withColumn después

    // Crear datos
    val data = Seq(("MARIA", 32), ("MARTA", 24), ("ANA", 25))

    // Crear DF
    val df = data.toDF("names", "ages")

    // Definir la función udf
    val firstLetterToLower = udf((s: String) => s.head.toLower + s.tail)

    // Aplicar la función udf a la columna names y mostrar el df resultante
    val modifiedDF = df.withColumn("names", firstLetterToLower(col("names")))
    modifiedDF.show()

  }
}

