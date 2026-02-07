from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import csv
import os
import asyncio

app = FastAPI(title="Air Quality IoT Server")

# ================= CONFIG =================
CSV_FILE = "air_quality_data.csv"
BUFFER_SIZE = 10  # عدد السجلات قبل الكتابة للملف

# ================= BUFFER =================
buffer = []

# ================= Pydantic Model =================
class ESP32Data(BaseModel):
    device_id: str
    temperature: float
    humidity: float
    co_ppm: float
    h2_ppm: float
    butane_ppm: float

# ================= ALERT THRESHOLDS =================
CO_THRESHOLD = 50.0
BUTANE_THRESHOLD = 10.0
TEMP_MIN = 15.0
TEMP_MAX = 30.0
HUMIDITY_MIN = 20.0
HUMIDITY_MAX = 70.0

# ================= CSV INIT =================
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "device_id",
            "temperature",
            "humidity",
            "co_ppm",
            "h2_ppm",
            "butane_ppm",
            "alert",
            "co_alert",
            "butane_alert",
            "temperature_alert",
            "humidity_alert"
        ])

# ================= HELPERS =================
def compute_alerts(data: ESP32Data):
    co_alert = data.co_ppm > CO_THRESHOLD
    butane_alert = data.butane_ppm > BUTANE_THRESHOLD
    temperature_alert = not (TEMP_MIN <= data.temperature <= TEMP_MAX)
    humidity_alert = not (HUMIDITY_MIN <= data.humidity <= HUMIDITY_MAX)
    alert = co_alert or butane_alert or temperature_alert or humidity_alert
    return alert, co_alert, butane_alert, temperature_alert, humidity_alert

def write_buffer_to_csv():
    global buffer
    if not buffer:
        return
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(buffer)
    buffer = []

# ================= ROUTES =================

@app.post("/api/data")
async def receive_data(data: ESP32Data):
    timestamp = datetime.utcnow().isoformat()

    alert, co_alert, butane_alert, temp_alert, hum_alert = compute_alerts(data)

    row = [
        timestamp,
        data.device_id,
        data.temperature,
        data.humidity,
        data.co_ppm,
        data.h2_ppm,
        data.butane_ppm,
        alert,
        co_alert,
        butane_alert,
        temp_alert,
        hum_alert
    ]

    buffer.append(row)

    if len(buffer) >= BUFFER_SIZE:
        write_buffer_to_csv()

    return {"status": "ok"}

@app.get("/latest")
async def latest_data():
    if buffer:
        row = buffer[-1]
    else:
        with open(CSV_FILE, "r") as f:
            lines = f.readlines()
            if len(lines) <= 1:
                return {"message": "No data yet"}
            row = lines[-1].strip().split(",")

    keys = [
        "timestamp","device_id","temperature","humidity",
        "co_ppm","h2_ppm","butane_ppm","alert",
        "co_alert","butane_alert","temperature_alert","humidity_alert"
    ]
    return dict(zip(keys, row))

@app.get("/download/csv")
async def download_csv():
    return FileResponse(
        path=CSV_FILE,
        media_type="text/csv",
        filename="air_quality_data.csv"
    )

@app.get("/health")
async def health():
    return {"status": "running"}

# ================= BACKGROUND TASK =================
async def periodic_csv_flush():
    while True:
        await asyncio.sleep(10)
        write_buffer_to_csv()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_csv_flush()) 
