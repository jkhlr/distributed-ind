package inclusion_dependencies

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {

  case class CommandLineArgs(path: String = "./TPCH", cores: Int = 4)

  val config: CommandLineArgs = {
    val parser = new scopt.OptionParser[CommandLineArgs]("distributed-ind") {
      opt[String]('p', "path")
      opt[String]('c', "cores")
    }
    parser.parse(args, CommandLineArgs()).get
  }

  def initSparkSession: SparkSession = {
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

  def getFilePaths: Seq[String] = {
    new File(config.path)
      .listFiles
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  println(
    new Pipeline(initSparkSession, getFilePaths)
      .run
      .map(_.toDependencyString)
      .mkString("\n")
  )
}