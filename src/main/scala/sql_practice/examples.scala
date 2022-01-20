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
}
