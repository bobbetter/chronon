// Delete all tables in a given database.
// Usage examples:
//   spark-shell -i ./scripts/data-deletion.scala --master local[*] --conf spark.chronon.database=data

// Resolve database parameter from Spark conf, defaulting to "data".
// Pass via: --conf spark.chronon.database=<db>
val targetDatabase = spark.conf.getOption("spark.chronon.database").filter(_.nonEmpty).getOrElse("data")

println(s"[data-deletion] Target database: '$targetDatabase'")

// Validate database exists using Catalog API (avoids exceptions on SHOW TABLES IN ...)
val databaseExists = spark.catalog.listDatabases().filter(_.name == targetDatabase).collect().nonEmpty

if (!databaseExists) {
  println(s"[data-deletion] Database '$targetDatabase' does not exist. Nothing to delete.")
  System.exit(0)
}

// List all non-view tables in the target database
val tables = spark.catalog
  .listTables(targetDatabase)
  .collect()
  .filter(t => t.tableType != null && !t.tableType.equalsIgnoreCase("VIEW"))

if (tables.isEmpty) {
  println(s"[data-deletion] No tables found in database '$targetDatabase'.")
  System.exit(0)
}

println(s"[data-deletion] Found ${tables.length} tables in '$targetDatabase'. Dropping...")

// Drop each table using fully-qualified, backticked identifiers to handle special characters
tables.foreach { t =>
  val qualified = s"`$targetDatabase`.`${t.name}`"
  println(s"[data-deletion] Dropping table: $qualified")
  spark.sql(s"DROP TABLE IF EXISTS $qualified")
}

println(s"[data-deletion] Completed dropping ${tables.length} tables from '$targetDatabase'.")

System.exit(0)


