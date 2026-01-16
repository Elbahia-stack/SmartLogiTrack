# tests/test_minimal_predict.py
from fastapi.testclient import TestClient
from fastAPI.mainn import app
from auth import create_access_token

client = TestClient(app)

def test_predict_minimal():
  
    token = create_access_token("admin")
    headers = {"Authorization": f"Bearer {token}"}

   
    payload = {
        "trip_distance": 1,
        "fare_amount": 5,
        "tip_amount": 0,
        "tolls_amount": 0,
        "total_amount": 5,
        "Airport_fee": 0,
        "RatecodeID": 1,
        "pickup_hour": 10,
        "pickup_dayofweek": 1
    }

    response = client.post("/predict", json=payload, headers=headers)

    assert response.status_code == 200
    assert "estimated_duration" in response.json()
    print("Predicted duration:", response.json()["estimated_duration"])
