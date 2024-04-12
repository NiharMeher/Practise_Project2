# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize a Spark session
spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# Load data from the CSV file
df = spark.read.csv("C:\\Users\\nranjanm\\Documents\\Practise10", header=True, inferSchema=True)

# Perform data transformation (calculate average age)
average_age = df.select(avg("age")).first()[0]

# Display the result
print(f"The average age of people is: {average_age}")

# Stop the Spark session
spark.stop()