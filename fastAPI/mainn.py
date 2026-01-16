from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy import text
from fastAPI.database import Base, engine, get_db
from . import models
from .models import Prediction
from .schemas import TripFeatures, LoginRequest
from .auth import create_access_token, verify_token

Base.metadata.create_all(bind=engine)

app = FastAPI()

# Spark
spark = SparkSession.builder.appName("api").getOrCreate()
model = GBTRegressionModel.load("models/pyspark_model")

# OAuth
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

from pyspark.ml.feature import VectorAssembler

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

assembler = VectorAssembler(inputCols=columns_to_use, outputCol="features")

@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username != "admin" or form_data.password != "admin":
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token(form_data.username)
    return {
        "access_token": token,
        "token_type": "bearer"
    }



@app.post("/predict")
def predict(data: TripFeatures,user=Depends(verify_token), db: Session = Depends(get_db)):
  
    input_df = spark.createDataFrame([data.dict()])
    input_df = assembler.transform(input_df)

    prediction_result = model.transform(input_df)
    predicted_duration = prediction_result.select("gbt_prediction").first()[0]
    new_log = Prediction(
        prediction_value=predicted_duration,
        model_version="v1"
    )
    db.add(new_log)
    db.commit()

    return {
        "estimated_duration": round(predicted_duration, 2)
    }

@app.get("/analytics/avg-duration-by-hour")
def avg_duration_by_hour(user=Depends(verify_token), db: Session = Depends(get_db)):
    query = text("""
        WITH trips_by_hour AS (
            SELECT pickup_hour, AVG(trip_duration_min) AS avgduration
            FROM silver_table
            GROUP BY pickup_hour
        )
        SELECT pickup_hour, avgduration
        FROM trips_by_hour
        ORDER BY pickup_hour
    """)
    result = db.execute(query).fetchall()
    return [
        {"pickuphour": row[0], "avgduration": float(row[1])} 
        for row in result
    ]



@app.get("/analytics/payment-analysis")
def payment_analysis(user=Depends(verify_token), db: Session = Depends(get_db)):
    query = text("""
        SELECT payment_type, COUNT(*) AS total_trips, AVG(trip_duration_min) AS avg_duration
        FROM silver_table
        GROUP BY payment_type
        ORDER BY payment_type
    """)
    result = db.execute(query).fetchall()
    return [
        {"payment_type": row[0], "total_trips": row[1], "avg_duration": float(row[2])}
        for row in result
    ]