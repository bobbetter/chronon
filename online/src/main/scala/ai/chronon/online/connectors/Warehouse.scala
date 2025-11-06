package ai.chronon.online.connectors

import ai.chronon.api.DataSpec

// we will impl for bigquery, snowflake etc
abstract class Warehouse(catalog: Catalog) {
  def createDatabase(databaseName: String): Unit
  protected def createTableInternal(table: Table, spec: DataSpec): Unit
  def enableIngestion(topic: Topic, table: Table): Unit

  // for staging queries
  // runs the query and appends the data to the table
  // the first time this is called, the table will be created
  def nativeQuery(query: String, table: Table): Unit

  def createTable(table: Table, spec: DataSpec): Unit = {
    createTableInternal(table, spec)
    catalog.putSpec(table, spec)
  }
}
