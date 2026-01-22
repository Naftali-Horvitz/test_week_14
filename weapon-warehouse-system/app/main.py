from fastapi import FastAPI, UploadFile, HTTPException
import uvicorn
from models import Weapons
import pandas as pd
import numpy as np
from db import get_conn, create_table, insert_data

app = FastAPI()

def add_risk_level_col(data):
        data['risk_level'] = pd.cut(
            data['range_km'],
            bins=[0, 20, 100, 300, np.inf],
            labels=["low", "mediume", "high", "extreme"]
        )


def handling_manufacturer_col(data: pd.DataFrame):
    data['manufacturer'] = data['manufacturer'].fillna("Unknown")
    valid_rows = []
    for row in data.to_dict(orient="records"):
        try:
            obj = Weapons(**row)
            valid_rows.append(obj)
        except:
            continue   
    data = valid_rows


@app.post("/upload")
def upload_file(file: UploadFile):
    if file.content_type != "text/csv":
        raise {"msg": "Invalid file type"}
    data = pd.read_csv(file.file)
    handling_manufacturer_col(data=data)
    add_risk_level_col(data=data)
    try:
        conn = get_conn()
    except HTTPException as e:
        raise {"msg": f"Connection failed {e}"}
    try:
        create_table(conn)
    except HTTPException as e:
        raise {"msg": f"create table failed {e}"}
    try:
        return insert_data(data.to_dict("records"), conn)
    except HTTPException as e:
        raise {"msg": f"insert data failed {e}"}



if __name__ =="__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8008, reload=True)

