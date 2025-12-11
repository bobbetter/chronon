// Create a database with a given name.
// Usage examples:
//   spark-shell -i /srv/chronon/scripts/create-database.scala --master local[*] --conf spark.chronon.database=data

// Resolve database parameter from Spark conf
// Pass via: --conf spark.chronon.database=<db>
val targetDatabase = spark.conf.getOption("spark.chronon.database").filter(_.nonEmpty).getOrElse {
  println(s"[create-database] ERROR: spark.chronon.database not specified")
  System.exit(1)
  ""
}

println(s"[create-database] Target database: '$targetDatabase'")

// Check if database already exists using Catalog API
val databaseExists = spark.catalog.listDatabases().filter(_.name == targetDatabase).collect().nonEmpty

if (databaseExists) {
  println(s"[create-database] Database '$targetDatabase' already exists. Nothing to create.")
  System.exit(0)
}

// Create the database
println(s"[create-database] Creating database: '$targetDatabase'")
spark.sql(s"CREATE DATABASE `$targetDatabase`")

println(s"[create-database] Successfully created database '$targetDatabase'.")

System.exit(0)



