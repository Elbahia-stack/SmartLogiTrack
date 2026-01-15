from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os


spark = SparkSession.builder \
    .appName("LoadSilverDataToPostgres") \
    .config("spark.jars", "org.postgresql:postgresql:42.7.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

print(" Spark session créée avec succès")


jdbc_url = "jdbc:postgresql://postgres:5432/silver_data"
connection_properties = {
    "user": "silver_user",
    "password": "silver_pass123",
    "driver": "org.postgresql.Driver"
}


folder_path = "data/silver/silver_dataset_single"


df = spark.read.parquet(folder_path)

print(f" Données chargées: {df.count()} lignes")
df.show(5)


table_name = "silver_table"  

df.write \
    .mode("overwrite") \
    .option("batchsize", "50000") \
    .option("numPartitions", "10") \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

print(f" Données écrites dans PostgreSQL dans la table '{table_name}'")


df_read = spark.read \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

print(f" Nombre de lignes dans PostgreSQL: {df_read.count()}")
df_read.show(5)

spark.stop()
print(" Terminé !")
