package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)
  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demo_commune = spark.read.json("data/input/demographie_par_commune.json")
    val pop = demo_commune.select($"Population").agg(sum($"Population")).show


    val pop_dep = demo_commune.select($"Population", $"Departement")
    pop_dep.groupBy($"Departement").sum().sort(desc("sum(Population)")).show(10)

    val dep = spark.read.csv("data/input/departements.txt")
      .select($"_c0".as(alias="name"), $"_c1".as(alias="code"))
    pop_dep.join(dep, pop_dep("Departement") === dep("code"))
    .groupBy($"name").sum().sort(desc("sum(Population)")).show(10)
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07 = spark.read      .option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_07")
      .select($"_c0".as(alias="code"), $"_c1".as(alias="job"), $"_c2".as(alias="count"), $"_c3".as(alias="salary"))

    val s08 = spark.read      .option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as(alias="code"), $"_c1".as(alias="job"), $"_c2".as(alias="count"), $"_c3".as(alias="salary"))

    val top_salary07 = s07.where($"salary" > 100000).sort(desc("salary"))
    top_salary07.show(10)

    val salary_growth = s07.join(s08,s07("code") === s08("code"), "Inner")
      .select(s07("job"), s07("salary"), s08("salary"), s08("salary")-s07("salary") as "growth")
      .where(s07("salary")<s08("salary"))
      .sort(desc("growth"))
    salary_growth.show(10)

    val job_loss = s07.join(s08,s07("code") === s08("code"), "Inner")
      .select(s07("code"),s07("job"),s07("count")-s08("count") as "Job_loss")
      .where(s08("salary")>100000 and(s08("salary")>100000))
      .sort(desc("Job_loss"))
    job_loss.show(10)
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    
    toursDF.agg(countDistinct("tourDifficulty") as "number of difficulty levels").show

    val tourPrices = toursDF.columns(7)
    toursDF.agg(min(tourPrices), max(tourPrices), avg(tourPrices)).show

    val tourPricesByDifficulty = toursDF.groupBy("tourDifficulty").agg(min(tourPrices), max(tourPrices), avg(tourPrices))
    tourPricesByDifficulty.show

    val tourDuration = toursDF.columns(4)
    val TourPricesAndDurationByDifficulty = toursDF.groupBy("tourDifficulty").agg(min(tourPrices), max(tourPrices), avg(tourPrices), min(tourDuration), max(tourDuration), avg(tourDuration))
    TourPricesAndDurationByDifficulty.show

    val tourTags = toursDF.select(toursDF("tourDifficulty"), explode($"tourTags") as "tags")
      .groupBy("tourDifficulty", "tags")
      .agg(count("tags") as "number")
      .orderBy($"number".desc)
    tourTags.show(10)

    val tourTagsByDifficulty = toursDF.select(toursDF("tourDifficulty"), toursDF("tourPrice"), explode($"tourTags") as "tags")
      .groupBy("tourDifficulty", "tourPrice", "tags")
      .agg(count("tags") as "number", max(toursDF("tourPrice")) as "maxPrice", min(toursDF("tourPrice")) as "minPrice", mean(toursDF("tourPrice")) as "avgPrice")
      .orderBy($"avgPrice")
    tourTagsByDifficulty.show(10)
  }
}