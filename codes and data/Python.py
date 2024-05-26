import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, format_number
from pyspark import SparkContext
from pyspark.sql.types import FloatType

# Check if the correct number of arguments is provided
if len(sys.argv) != 3:
    print("Usage: spark-submit script.py input_file output_file")
    sys.exit(1)

# Extract input and output file paths from command-line arguments
input_file = sys.argv[1]
output_file = sys.argv[2]

# Create SparkContext
sc = SparkContext("local", "Average Salary Calculation")

# Create SparkSession
spark = SparkSession.builder \
    .appName("Average Salary Calculation") \
    .getOrCreate()

# Read input CSV file with comma delimiter
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .csv(input_file)

# Perform the required DataFrame operations
average_salary_df = df.filter(df["work_year"] == 2024).groupBy("job_title") \
    .agg(avg("salary").alias("average_salary"))

# Convert "average_salary" column to float type
average_salary_df = average_salary_df.withColumn("average_salary", average_salary_df["average_salary"].cast(FloatType()))

# Sort the DataFrame by "average_salary" column in descending order
average_salary_df = average_salary_df.orderBy("average_salary", ascending=False)

# Format the average_salary to 3 decimal places
average_salary_df = average_salary_df.withColumn("average_salary", format_number("average_salary", 3))

# Write the DataFrame to a CSV file
average_salary_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_file)

# Stop the SparkSession
spark.stop()

# Stop the SparkContext
sc.stop()
