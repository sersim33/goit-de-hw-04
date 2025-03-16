from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox") \
    .getOrCreate()

# Load dataset
file_path = "./nuek-vuh3.csv"  # Ensure the file path is correct
# /Users/HP/Desktop/DATA_Eng/goit-de-hw-04/nuek-vuh3.csv
nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

# Repartition the dataset
nuek_repart = nuek_df.repartition(2)

# Process data
nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count() \
    .where("count > 2")  # Filtering step

# Show results
nuek_processed.show()

# Wait for user input before closing
input("Press Enter to exit...")

# Stop Spark session
spark.stop()


