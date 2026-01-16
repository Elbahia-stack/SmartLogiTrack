from sqlalchemy import Column, Integer, Float, String, DateTime
from fastAPI.database import Base
import datetime

class Prediction(Base):
    __tablename__ = "eta_predictions"

    id = Column(Integer, primary_key=True, index=True)
    model_version = Column(String, default="GBT_v1")
    prediction_value = Column(Float)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)