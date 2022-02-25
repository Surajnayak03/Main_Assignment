package pck.Movie_detail
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import pck.Movie_detail.main_methos.setup_dataframe

object movie_details {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org") .setLevel(Level.ERROR)

   val spark = SparkSession
     .builder()
     .appName("Spark SQL basic example")
     .config("spark.master", "local")
     .getOrCreate()

   val movie=setup_dataframe("C:\\Users\\surajnayak\\Hashedin\\Scala_assignment\\movies_dataset.csv")
   import spark.implicits._
   println("take  top 10k items")
   val movie_tenk=movie.limit(10000)

   def Title_byDirector_YearRange(Director:String,From_year:Int,To_year:Int):Unit= {
     val To_year1= (To_year-1).toString
     val From_year1=(From_year+1).toString
     val movie_titles = movie_tenk.filter(movie_tenk("director") === Director &&
      movie_tenk("year").lt(To_year1) && movie_tenk("year").gt(From_year1))
     println(" director year range movie title")
     movie_titles.show(7000, false)
   }


     //english movie sorted desc

   def English_Title_revbyUser_Desc(Language:String):Unit= {
     val eng_title_usr_rvw_desc = movie_tenk.filter(movie_tenk("language") === Language).
      sort(movie_tenk("reviews_from_users").cast(IntegerType).desc)
     println("englist title in desc user review")
     eng_title_usr_rvw_desc.show(50, false)
   }

  def highest_budget_title( year_value : String, Country_value:String): Unit= {
        var movie_ten1_int_budget=movie_tenk.withColumn("budget_new",
     regexp_replace($"budget","[^A-Z0-9_]","")).drop("budget")

    //changing the budget_new datatype as int
    movie_ten1_int_budget=movie_ten1_int_budget.withColumn("budget_new",
     movie_ten1_int_budget("budget_new").cast(IntegerType))
     movie_ten1_int_budget.createOrReplaceTempView ("GETBYID")
      val query=s"""SELECT s.country,s.year,s.budget_new,s.title FROM GETBYID s
        inner JOIN (SELECT year,country,MAX(budget_new) AS id FROM GETBYID
          where year= $year_value and country= '$Country_value' GROUP BY year,country) max ON
          s.budget_new = max.id and s.year=max.year and s.country=max.country"""

    val max_budget= spark.sql(query)
     println ("max budget")
     max_budget.show (5000,false)
    }

   val movie_ten_votes_int = movie_tenk.withColumn("votes_new", movie_tenk("votes").
     cast(IntegerType)).drop("votes")



   Title_byDirector_YearRange("D.W. Griffith",1913,1919)
    English_Title_revbyUser_Desc("English")
    highest_budget_title("1915","USA")

  }
}
