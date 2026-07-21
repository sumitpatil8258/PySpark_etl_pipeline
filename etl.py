from pyspark.sql import SparkSession

from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("AQI ETL") \
    .getOrCreate()

# EXTRACT
df = spark.read.csv(
    "Dataset.csv",
    header=True,
    inferSchema=True
)

#df.show()

# TRANSFORM
df = df.dropna()
df = df.dropDuplicates()
df1 = df.select("NAME","HUMIDITY","TEMPRATURE_MAX")
df_filtered = df1.filter((col("HUMIDITY") > 20) & (col("TEMPRATURE_MAX" )> 40))
df_filtered.show()


# LOAD
df_filtered.write\
.format("jdbc")\
.option("url", "jdbc:mysql://localhost:3307/pharmacydb")\
.option("driver", "com.mysql.cj.jdbc.Driver")\
.option("dbtable", "AQI")\
.option("user", "root") \
.option("password", "YOUR_ROOT_PASSWORD") \
.mode("overwrite") \
.save()

spark.stop()
