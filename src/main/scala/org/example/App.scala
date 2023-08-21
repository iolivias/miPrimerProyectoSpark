package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.example.Libro.{Ejercicio2, Ejercicio_1, Ejercicio2Libro, Ejercicio_Cap3}

object App {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("MiPrimerProyectoSpark")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Ejercicio_1.ejercicio_1()

    //Ejercicio2Libro.EjercicioLower()

    Ejercicio_Cap3.Ejercicio_Cap3()

  }

}


