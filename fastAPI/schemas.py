from pydantic import BaseModel 

class TripFeatures(BaseModel):
    trip_distance:float
    fare_amount:float
    tip_amount:float 
    tolls_amount:float
    total_amount:float
    Airport_fee:float
    RatecodeID:int
    pickup_hour:int
    pickup_dayofweek:int

class LoginRequest(BaseModel):
    username: str
    password: str    