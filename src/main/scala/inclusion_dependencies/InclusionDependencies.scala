package inclusion_dependencies

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InclusionDependencies extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val sparkBuilder = SparkSession
    .builder()
    .appName("InclusionDependencies")
    .master("local[4]")
  val spark = sparkBuilder.getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "8")

  val nations = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("delimiter", ";")
    .csv("src/main/resources/tpch_nation.csv")

  nations.show()
}