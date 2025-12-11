// Create tables from csv and load them into spark.
// spark-shell -i data-loader.scala --master spark://spark-master:7077
import java.io.File
import org.apache.spark.sql.types._


spark.sql("CREATE DATABASE IF NOT EXISTS data;")
// Directory containing CSV files (mounted inside container at /srv/chronon/app)
val folderPath = "/srv/chronon/app/data/"

val folder = new File(folderPath)

// Basic sanity checks so it's obvious when nothing happens
if (!folder.exists || !folder.isDirectory) {
  println(s"[data-loader] Data folder does not exist or is not a directory: $folderPath")
  System.exit(1)
}

// List all files in the directory
val files = folder.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv"))

if (files.isEmpty) {
  println(s"[data-loader] No CSV files found in: $folderPath")
  System.exit(0)
} else {
  println(s"[data-loader] Found ${files.length} CSV files in: $folderPath")
}

// Process each CSV file
files.foreach { file =>
  val fileName = file.getName

  // Load CSV file into DataFrame
  val df = spark.read.option("header", "true").csv(s"$folderPath/$fileName")
  // Set schema: ts -> long, _price -> long, _amt -> long, anything else -> string
  // // Get the existing column names
  val columns = df.columns

  // Define the schema based on column names
  val customSchema = StructType(
    columns.map { columnName =>
      val dataType = columnName match {
        case "ts" => LongType
        case name if name.endsWith("_price") || name.endsWith("_amt") => LongType
        case _ => StringType
      }
      StructField(columnName, dataType, nullable = true)
    }
  )
  val dfWithSchema = spark.read.schema(customSchema).option("header", "true").csv(s"$folderPath/$fileName")
  // Create a table name based on the file name (without the extension)
  val tableName = s"data.${fileName.split('.')(0)}"

  // Save DataFrame as a Hive table
  dfWithSchema.show()
  dfWithSchema.write.partitionBy("ds").mode("overwrite").saveAsTable(tableName)
}

// Stop Spark session
System.exit(0)
