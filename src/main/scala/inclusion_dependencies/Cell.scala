package inclusion_dependencies

case class Cell(value: String, attributes: Set[String]) {
  def toDependencyString: String = {
    val sortedAttributes = this.attributes.toList.sorted
    s"${this.value} < ${sortedAttributes.mkString(", ")}"
  }
}