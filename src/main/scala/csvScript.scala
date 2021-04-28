import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object loadCsvScript {
  def main(args: Array[String]): Unit = {

    println("-=-=-=-Apache Spark Application Started...-=-=-=-\n")

    val spark = SparkSession.builder()
      .appName(name = "Create DataFrame from CSV file")
      .master(master = "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val csv_path = "books.csv"
    val df = spark.read.option("header", true).csv(csv_path)

    println("Схема полученного DataFrame:")
    df.printSchema()

    println("Количество записей в DataFrame: " + df.count())

    df.filter("average_rating > 4.50").show(400, truncate = false)

    // Средний рейтинг без учета округлления
    df.select(avg("average_rating")
      .alias("Средний рейтинг для всех книг"))
      .show(truncate = false)

    // Средний рейтинг с учетом округления
    df.select(round(avg("average_rating"))
      .alias("Средний рейтинг для всех книг"))
      .show(truncate = false)

    println("Количество книг в диапазоне 0 - 1: " + df.select(df.col("*"))
      .filter("average_rating >= 0 and average_rating < 1").count())
    println("Количество книг в диапазоне 1 - 2: " + df.select(df.col("*"))
      .filter("average_rating >= 1 and average_rating < 2").count())
    println("Количество книг в диапазоне 2 - 3: " + df.select(df.col("*"))
      .filter("average_rating >= 2 and average_rating < 3").count())
    println("Количество книг в диапазоне 3 - 4: " + df.select(df.col("*"))
      .filter("average_rating >= 3 and average_rating < 4").count())
    println("Количество книг в диапазоне 4 - 5: " +df.select(df.col("*"))
      .filter("average_rating >= 4 and average_rating <= 5").count())

    println("\n-=-=-=-Apache Spark Application Finish-=-=-=-")
  }
}
