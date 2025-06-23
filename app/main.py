import logging
import sys
import os
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .middleware.security_headers import SecurityHeadersMiddleware
from .middleware.rate_limiting import RateLimitMiddleware 
from .routers import data_router # <-- New unified router
from .websocket_manager import startup_connection_manager, shutdown_connection_manager
from .services.query_service import query_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - line %(lineno)d - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

app = FastAPI(
    title="EigenKor Trading Platform API",
    description="Unified backend API for historical data, live data feeds, and session management.",
    version="2.0.0",
    docs_url=None,
    redoc_url=None
)

# --- Static Frontend File Serving ---
script_dir = os.path.dirname(__file__)
frontend_dir = os.path.join(os.path.dirname(script_dir), "frontend")
static_dir = os.path.join(frontend_dir, "static")

if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.mount("/dist", StaticFiles(directory=os.path.join(frontend_dir, "dist")), name="dist")
else:
    logging.error(f"Static directory not found at: {static_dir}.")

# --- Middleware ---
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, calls=100, period=60)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:8080"], # Add other origins if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Application Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    logging.info("Application starting up...")
    await startup_connection_manager()
    logging.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Application shutting down...")
    await shutdown_connection_manager()
    await query_service.close() # Close InfluxDB client
    logging.info("Application shutdown complete.")

# --- API Router ---
app.include_router(data_router.router) # <-- Include the single unified router

# --- Root Endpoint ---
@app.get("/", response_class=HTMLResponse)
async def root():
    """Serves the main frontend application."""
    index_html_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_html_path):
        with open(index_html_path, "r") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    raise HTTPException(status_code=404, detail="index.html not found")

# --- Health Check ---
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}