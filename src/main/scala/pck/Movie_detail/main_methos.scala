package pck.Movie_detail

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object main_methos {

  def setup_dataframe(path1: String):DataFrame={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val movie = spark.read.option("header", "true")
      .csv(path = path1) //"C:\\Users\\surajnayak\\Hashedin\\Scala_assignment\\movies_dataset.csv")
    import spark.implicits._
    movie

  }
}