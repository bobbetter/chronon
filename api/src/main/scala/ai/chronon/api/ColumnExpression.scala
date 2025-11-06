package ai.chronon.api

import ai.chronon.api.CollectionExtensions.JMapExtension

case class ColumnExpression(column: String, expression: Option[String]) {
  def render: String =
    expression match {
      case Some(value) => s"$value as $column"
      case None        => column
    }
}

object ColumnExpression {
  private def isIdentifier(s: String): Boolean = {
    // alpha numeric underscore regex match
    s.matches("^[a-zA-Z0-9_]*$")
  }

  // timeColumn = null, selects: {"ts": "expression"}          :: expression as ts
  // timeColumn = null, selects: {..} or null                  :: ts
  // timeColumn = "timeMs", selects: {"timeMs": "expression"}  :: expression as ts
  // timeColumn = "timeMs", selects: {..} or null              :: timeMs as ts
  // timeColumn = "expression", selects: {..} or null          :: expression as ts
  def getTimeExpression(query: Query): ColumnExpression = {
    if (query == null) return ColumnExpression(Constants.TimeColumn, None)

    val expressionOpt: Option[String] = if (!query.isSetTimeColumn) {
      query.getSelects.safeGet(Constants.TimeColumn)
    } else if (isIdentifier(query.getTimeColumn)) {
      query.getSelects.safeGet(query.getTimeColumn, default = query.getTimeColumn)
    } else {
      Option(query.getTimeColumn)
    }

    ColumnExpression(Constants.TimeColumn, expressionOpt)
  }
}
