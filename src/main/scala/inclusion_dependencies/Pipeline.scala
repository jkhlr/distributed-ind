package inclusion_dependencies

import inclusion_dependencies.Pipeline.Cell
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Pipeline extends Serializable {

  case class Cell(value: String, attributes: Set[String])

  def run(spark: SparkSession, paths: Seq[String]): Seq[String] = new Pipeline(spark).run(paths)
}

class Pipeline(val spark: SparkSession) extends Serializable {

  import spark.implicits._

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
        .map {
          case (value, attributes) => Cell(value, attributes)
        }
    }).toDS
  }

  def toDependencyString(cell: Cell): String = {
    val sortedAttributes = cell.attributes.toList.sorted
    s"${
      cell.value
    } < ${
      sortedAttributes.mkString(", ")
    }"
  }

  def run(paths: Seq[String]): Seq[String] = {
    paths
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
  }
}
