package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.example.Borrador.borrador
import org.example.Libro.{ejercicioCap2, ejercicioUDF, ejercicioCap3, ejercicioCap3b, ejercicioCap3c, ejercicioCap4}

object App {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("MiPrimerProyectoSpark")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //ejercicioCap2.ejercicioCap2()

    //ejercicioUDF.ejercicioLower()

    //ejercicioCap3.ejercicioCap3()

    //ejercicioCap3b.ejercicioCap3b()

    //ejercicioCap3c.ejercicioCap3c()

    ejercicioCap4.ejercicioCap4()

  }

}


