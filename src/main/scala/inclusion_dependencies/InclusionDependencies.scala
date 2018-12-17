package inclusion_dependencies

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object InclusionDependencies extends App {

  case class CommandLineArgs(path: String = "./TPCH", cores: Int = 4)

  def parseConfig(): CommandLineArgs = {
    val parser = new scopt.OptionParser[CommandLineArgs]("distributed-ind") {
      opt[String]('p', "path")
      opt[String]('c', "cores")
    }
    parser.parse(args, CommandLineArgs()).get
  }

  def initSparkSession(config: CommandLineArgs): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkBuilder = SparkSession
      .builder()
      .appName("InclusionDependencies")
      .master(s"local[${config.cores}]")
    val spark = sparkBuilder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", s"${config.cores * 2}")
    spark
  }

  val config = parseConfig()
  val spark = initSparkSession(config)

  import spark.implicits._

  case class Cell(value: String, attributes: Set[String])

  def loadFile(path: String): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
  }

  def toCells(dataFrame: DataFrame): Dataset[Cell] = {
    dataFrame.rdd.flatMap(row => {
      val values = row.toSeq.map(_.toString)
      val attrNameSets = row.schema.map(attr => Set(attr.name))
      values
        .zip(attrNameSets)
        .map { case (value, attributes) => Cell(value, attributes) }
    }).toDS
  }

  def toDependencyString(cell: Cell): String = {
    val sortedAttributes = cell.attributes.toList.sorted
    s"${cell.value} < ${sortedAttributes.mkString(", ")}"
  }

  val paths = new File(config.path).listFiles.filter(_.isFile).map(_.getAbsolutePath)

  val dependencies = paths
    .map(loadFile)
    .map(toCells)
    .reduce(_.union(_))
    .groupByKey(_.value)
    .mapGroups((value, cells) =>
      Cell(value, cells.map(_.attributes).reduce(_.union(_)))
    )
    .flatMap(cell =>
      cell.attributes.map(elem => Cell(elem, cell.attributes - elem))
    )
    .groupByKey(_.value)
    .mapGroups((value, cells) =>
      Cell(value, cells.map(_.attributes).reduce(_.intersect(_)))
    )
    .filter(_.attributes.nonEmpty)
    .collect()
    .map(toDependencyString)
    .sorted

  dependencies.foreach(println)
}