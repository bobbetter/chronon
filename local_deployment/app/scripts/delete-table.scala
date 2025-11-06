// Delete a specific table in a database.
// Usage examples:
//   spark-shell -i ./scripts/delete-table.scala --master local[*] --conf spark.chronon.table=data.my_table

// Resolve table parameter from Spark conf (format: database.table_name)
// Pass via: --conf spark.chronon.table=<database>.<table>
val targetTableName = spark.conf.getOption("spark.chronon.table").filter(_.nonEmpty).getOrElse {
  println(s"[delete-table] ERROR: spark.chronon.table not specified")
  System.exit(1)
  ""
}

println(s"[delete-table] Target table: '$targetTableName'")

// Parse database and table name from fully-qualified table name
val parts = targetTableName.split("\\.", 2)
if (parts.length != 2) {
  println(s"[delete-table] ERROR: Table name must be fully qualified (database.table_name). Got: '$targetTableName'")
  System.exit(1)
}
val databaseName = parts(0)
val tableName = parts(1)

println(s"[delete-table] Database: '$databaseName', Table: '$tableName'")

// Check if database exists
val databases = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
if (!databases.contains(databaseName)) {
  println(s"[delete-table] Database '$databaseName' does not exist. Nothing to delete.")
  System.exit(0)
}

// Check if table exists in the database
val tables = spark.sql(s"SHOW TABLES IN `$databaseName`").collect()
val tableExists = tables.exists(row => row.getString(1) == tableName)

if (!tableExists) {
  println(s"[delete-table] Table '$targetTableName' does not exist. Nothing to delete.")
  System.exit(0)
}

println(s"[delete-table] Dropping table: $targetTableName")
spark.sql(s"DROP TABLE `$databaseName`.`$tableName`")

println(s"[delete-table] Completed dropping table '$targetTableName'.")

System.exit(0)


