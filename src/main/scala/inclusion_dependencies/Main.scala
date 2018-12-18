package inclusion_dependencies

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {

  case class CommandLineArgs(path: String = "./TPCH", cores: Int = 4)

  def parseConfig(): CommandLineArgs = {
    val parser = new scopt.OptionParser[CommandLineArgs]("distributed-ind") {
      opt[String]('p', "path")
      opt[String]('c', "cores")
    }
    parser.parse(args, CommandLineArgs()).get
  }

  def initSparkSession(cores: Int): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkBuilder = SparkSession
      .builder()
      .appName("InclusionDependencies")
      .master(s"local[$cores]")
    val spark = sparkBuilder.getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", s"${cores * 2}")
    spark
  }

  def getFilePaths(dirPath: String): Seq[String] = {
    new File(config.path)
      .listFiles
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  val config = parseConfig()
  val spark = initSparkSession(config.cores)
  val paths = getFilePaths(config.path)

  val cells = new Pipeline(spark, paths).run
  println(cells.map(_.toDependencyString).mkString("\n"))
}