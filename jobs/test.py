from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
print("Hola Spark")
spark.stop()
print("Fin")