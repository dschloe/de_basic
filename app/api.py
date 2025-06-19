# FastAPI 코드
import pickle
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
model = pickle.load(open("model.pkl", "rb"))

class InputData(BaseModel):
    features: list

@app.post("/predict")
def predict(data: InputData):
    pred = model.predict([data.features])
    return {"prediction": int(pred[0])}