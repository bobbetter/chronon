from typing import Union
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from server.routes import router
from server.services.datascanner import DataScanner
import logging
logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize shared app resources once (e.g., DataScanner)."""
    warehouse_path = os.environ.get("SPARK_WAREHOUSE_PATH")
    if not warehouse_path:
        # current_dir = Path(__file__).parent
        server_root = Path(__file__).parent
        warehouse_path = str(server_root / "spark-data/warehouse")
    logger.info(f"Using warehouse path: {warehouse_path}")
    app.state.datascanner = DataScanner(warehouse_path)
    yield


app = FastAPI(lifespan=lifespan)

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)