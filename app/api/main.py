from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.db import init_db
from app.api import routes
from app.ingestion.pipeline import run_etl
import threading
import time

# Background ETL runner for demo purposes (P0.3 requirement: "automatically start ETL")
def start_etl_loop():
    # Wait a bit for DB to be ready
    time.sleep(5)
    print("Starting initial ETL run...")
    try:
        run_etl()
    except Exception as e:
        print(f"Initial ETL failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    # Run ETL in a separate thread on startup so it doesn't block API
    thread = threading.Thread(target=start_etl_loop)
    thread.daemon = True
    thread.start()
    yield
    # Shutdown

app = FastAPI(title="Kasparro Backend", lifespan=lifespan)

app.include_router(routes.router)

@app.get("/")
def root():
    return {"message": "Welcome to Kasparro API. Go to /docs for Swagger UI."}
