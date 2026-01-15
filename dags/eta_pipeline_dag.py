from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


SILVER_PATH = "./data/silver/silver_dataset_single"


JDBC_URL = "jdbc:postgresql://postgres-silver:5432/silver_data"
DB_PROPERTIES = {
    "user": "silver_user",
    "password": "silver_pass123",
    "driver": "org.postgresql.Driver"
}

TABLE_NAME = "silver_table"
MODEL_PATH = "models/pyspark_model"




def get_spark():
    return SparkSession.builder \
        .appName("Airflow_PySpark_ETA") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
       .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "1g") \
        .getOrCreate()


# TASK 1 : SILVER → POSTGRESQL

def load_silver_to_postgres():
    spark = get_spark()

   
    df = spark.read.parquet(SILVER_PATH)

    print(f"Silver chargé : {df.count()} lignes")
    df.show(5)

 
    df.write \
        .mode("overwrite") \
        .option("batchsize", "50000") \
        .option("numPartitions", "10") \
        .jdbc(
            url=JDBC_URL,
            table=TABLE_NAME,
            properties=DB_PROPERTIES
        )

    spark.stop()




def train_gbt_model():
    spark = get_spark()

    # Charger Silver depuis PostgreSQL
    df = spark.read.jdbc(
        url=JDBC_URL,
        table=TABLE_NAME,
        properties=DB_PROPERTIES
    )

    columns_to_use = [
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "Airport_fee",
        "RatecodeID",
        "pickup_hour",
        "pickup_dayofweek"
    ]

    assembler = VectorAssembler(
        inputCols=columns_to_use,
        outputCol="features"
    )

    df_model = assembler.transform(df) \
        .select("features", "trip_duration_min")

    train_data, test_data = df_model.randomSplit([0.8, 0.2], seed=42)

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="trip_duration_min",
        predictionCol="gbt_prediction",
        maxIter=30,
        maxDepth=5,
        seed=42
    )
    print(f"Taille du set d'entraînement : {train_data.count()}")
    train_data.select("features", "trip_duration_min").show(5)
    gbt_model = gbt.fit(train_data)
    gbt_preds = gbt_model.transform(test_data)

    # Évaluation
    for metric in ["rmse", "mae", "r2"]:
        score = RegressionEvaluator(
            labelCol="trip_duration_min",
            predictionCol="gbt_prediction",
            metricName=metric
        ).evaluate(gbt_preds)

        print(f"{metric.upper()} du GBT : {score}")

    # Sauvegarde modèle
    gbt_model.save(MODEL_PATH)

    spark.stop()




default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 15, 1),
    "retries": 1
}

with DAG(
    dag_id="silver_to_gbt_training",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    silver_task = PythonOperator(
        task_id="silver_to_postgres",
        python_callable=load_silver_to_postgres
    )

    train_task = PythonOperator(
        task_id="train_gbt_model",
        python_callable=train_gbt_model
    )

    silver_task >> train_task
