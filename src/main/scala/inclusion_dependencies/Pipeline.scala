package inclusion_dependencies

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Pipeline(val spark: SparkSession, val paths: Seq[String]) extends Serializable {

  import spark.implicits._

  def loadFile(path: String): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
  }

  def toCells(dataFrame: DataFrame): Dataset[Cell] = {
    val attrNameSets = dataFrame.columns.map(Set(_))
    dataFrame.rdd.flatMap(row => {
      row
        .toSeq
        .map(_.toString)
        .zip(attrNameSets)
        .map { case (value, attributes) =>
          Cell(value, attributes)
        }
    }).toDS
  }

  def run: Seq[Cell] = {
    paths
      .map(loadFile)
      .map(toCells)
      .reduce(_.union(_))
      .groupByKey(_.value)
      .mapGroups((value, cells) => {
        val attributeUnion = cells.map(_.attributes).reduce(_.union(_))
        Cell(value, attributeUnion)
      })
      .flatMap(cell =>
        cell.attributes.map(attribute =>
          Cell(attribute, cell.attributes - attribute)
        )
      )
      .groupByKey(_.value)
      .mapGroups((value, cells) => {
        val attributeIntersection = cells.map(_.attributes).reduce(_.intersect(_))
        Cell(value, attributeIntersection)
      })
      .filter(_.attributes.nonEmpty)
      .collect
      .sortWith(_.value < _.value)
  }
}
