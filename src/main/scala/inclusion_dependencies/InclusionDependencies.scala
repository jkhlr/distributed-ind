package inclusion_dependencies

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object InclusionDependencies extends App {

  case class CommandLineArgs(path: String = "./TPCH", cores: Int = 4)

  val parser = new scopt.OptionParser[CommandLineArgs]("distributed-ind") {
    opt[String]('p', "path")
    opt[String]('c', "cores")
  }

  val config = parser.parse(args, CommandLineArgs()).get

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkBuilder = SparkSession
    .builder()
    .appName("InclusionDependencies")
    .master(s"local[${config.cores}]")
  val spark = sparkBuilder.getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", s"${config.cores * 2}")

  import spark.implicits._

  def loadFile(path: String): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
  }

  def toCells(dataFrame: DataFrame): Dataset[(String, Set[String])] = {
    dataFrame.rdd.flatMap(row => {
      val values = row.toSeq.map(_.toString)
      val attrNameSets = row.schema.map(attr => Set(attr.name))
      values.zip(attrNameSets)
    }).toDS
  }

  def aggregateWith(func: (Set[String], Set[String]) => Set[String]): (String, Iterator[(String, Set[String])]) => (String, Set[String]) = {
    (key, iterator) => (key, iterator.map(_._2).reduce(func))
  }

  def byValue(t: (String, Set[String])): String = {
    t match {case (value, _) => value}
  }

  def toInclusionLists(t: (String, Set[String])): Set[(String, Set[String])] = {
    t match {
      case (_, set) => set.map(elem => (elem, set - elem))
    }
  }

  def nonEmptyAttributeSet(t: (String, Set[String])): Boolean = {
    t match {case (_, set) => set.nonEmpty}
  }

  def toDependencyString(t: (String, Set[String])): String = {
    t match {
      case (dependent, referenced) =>
        s"$dependent < ${referenced.toList.sorted.mkString(", ")}"
    }
  }

  val time = System.currentTimeMillis
  val tpchPath = "src/main/resources/"
  val paths = new File(tpchPath).listFiles.filter(_.isFile).map(_.getAbsolutePath)
  val dependencies =
    paths
      .map(loadFile)
      .map(toCells)
      .reduce(_.union(_))
      .groupByKey(byValue)
      .mapGroups(aggregateWith(_.union(_)))
      .flatMap(toInclusionLists)
      .groupByKey(byValue)
      .mapGroups(aggregateWith(_.intersect(_)))
      .filter(_._2.nonEmpty)
      .collect()
      .map(toDependencyString)
      .sorted

  dependencies.foreach(println)
}