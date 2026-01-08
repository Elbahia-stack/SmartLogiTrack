from pyspark.sql import SparkSession
from sqlalchemy import create_engine


spark = SparkSession.builder \
    .appName("SaveSilverToPostgres") \
    .getOrCreate()


df_silver = spark.read.parquet("./data/silver/silver_dataset")




pdf = df_silver.sample(0.2).toPandas()


engine = create_engine(
    "postgresql://postgres:elbahia2005@172.26.112.1:5432/smartlogi"
)

pdf.to_sql(
    name="silver_taxi",
    con=engine,
    if_exists="replace",   
    index=False,
    chunksize=10_000
)


