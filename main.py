from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import csv
import io

from sqlalchemy import create_engine, Column, Float, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ================= CONFIG =================
DATABASE_URL = "postgres://username:password@host:port/air_quality_db"  # ضع بياناتك هنا
MAX_RECORDS_PER_DEVICE = 1000  # لا تحفظ أكثر من 1000 قراءة لكل جهاز

# ================= SQLALCHEMY SETUP =================
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class AirQuality(Base):
    __tablename__ = "air_quality"
    id = Column(String, primary_key=True, unique=True)  # device_id+timestamp
    timestamp = Column(DateTime)
    device_id = Column(String)
    temperature = Column(Float)
    humidity = Column(Float)
    co_ppm = Column(Float)
    h2_ppm = Column(Float)
    butane_ppm = Column(Float)
    alert = Column(Boolean)
    co_alert = Column(Boolean)
    butane_alert = Column(Boolean)
    temperature_alert = Column(Boolean)
    humidity_alert = Column(Boolean)

# إنشاء الجداول عند تشغيل السيرفر
Base.metadata.create_all(bind=engine)

# ================= FASTAPI =================
app = FastAPI(title="Air Quality IoT Server (Limited DB)")

# ================= ALERT THRESHOLDS =================
CO_THRESHOLD = 50.0
BUTANE_THRESHOLD = 10.0
TEMP_MIN = 15.0
TEMP_MAX = 30.0
HUMIDITY_MIN = 20.0
HUMIDITY_MAX = 70.0

# ================= Pydantic Model =================
class ESP32Data(BaseModel):
    device_id: str
    temperature: float
    humidity: float
    co_ppm: float
    h2_ppm: float
    butane_ppm: float

# ================= HELPERS =================
def compute_alerts(data: ESP32Data):
    co_alert = data.co_ppm > CO_THRESHOLD
    butane_alert = data.butane_ppm > BUTANE_THRESHOLD
    temperature_alert = not (TEMP_MIN <= data.temperature <= TEMP_MAX)
    humidity_alert = not (HUMIDITY_MIN <= data.humidity <= HUMIDITY_MAX)
    alert = co_alert or butane_alert or temperature_alert or humidity_alert
    return alert, co_alert, butane_alert, temperature_alert, humidity_alert

# ================= ROUTES =================
@app.post("/api/data")
async def receive_data(data: ESP32Data):
    alert, co_alert, butane_alert, temp_alert, hum_alert = compute_alerts(data)
    timestamp = datetime.utcnow()

    db = SessionLocal()
    record = AirQuality(
        id=f"{data.device_id}_{timestamp.isoformat()}",
        timestamp=timestamp,
        device_id=data.device_id,
        temperature=data.temperature,
        humidity=data.humidity,
        co_ppm=data.co_ppm,
        h2_ppm=data.h2_ppm,
        butane_ppm=data.butane_ppm,
        alert=alert,
        co_alert=co_alert,
        butane_alert=butane_alert,
        temperature_alert=temp_alert,
        humidity_alert=hum_alert
    )
    db.add(record)
    db.commit()

    # -------- حذف السجلات القديمة إذا تجاوزت الحد --------
    count = db.query(AirQuality).filter(AirQuality.device_id == data.device_id).count()
    if count > MAX_RECORDS_PER_DEVICE:
        oldest = db.query(AirQuality)\
                   .filter(AirQuality.device_id == data.device_id)\
                   .order_by(AirQuality.timestamp.asc())\
                   .limit(count - MAX_RECORDS_PER_DEVICE).all()
        for r in oldest:
            db.delete(r)
        db.commit()
    db.close()

    return {"status": "ok"}

@app.get("/latest")
async def latest_data():
    db = SessionLocal()
    last = db.query(AirQuality).order_by(AirQuality.timestamp.desc()).first()
    db.close()
    if not last:
        return {"message": "No data yet"}
    return {
        "timestamp": last.timestamp.isoformat(),
        "device_id": last.device_id,
        "temperature": last.temperature,
        "humidity": last.humidity,
        "co_ppm": last.co_ppm,
        "h2_ppm": last.h2_ppm,
        "butane_ppm": last.butane_ppm,
        "alert": last.alert,
        "co_alert": last.co_alert,
        "butane_alert": last.butane_alert,
        "temperature_alert": last.temperature_alert,
        "humidity_alert": last.humidity_alert
    }

@app.get("/download/csv")
async def download_csv():
    db = SessionLocal()
    records = db.query(AirQuality).all()
    db.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "timestamp","device_id","temperature","humidity",
        "co_ppm","h2_ppm","butane_ppm","alert",
        "co_alert","butane_alert","temperature_alert","humidity_alert"
    ])
    for r in records:
        writer.writerow([
            r.timestamp, r.device_id, r.temperature, r.humidity,
            r.co_ppm, r.h2_ppm, r.butane_ppm, r.alert,
            r.co_alert, r.butane_alert, r.temperature_alert, r.humidity_alert
        ])
    output.seek(0)
    return FileResponse(
        path_or_file=io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        filename="air_quality_data.csv"
    )

@app.get("/health")
async def health():
    return {"status": "running"}
