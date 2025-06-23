import logging
import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# --- Middleware Imports ---
from .middleware.security_headers import SecurityHeadersMiddleware
from .middleware.rate_limiting import RateLimitMiddleware 

# --- NEW: Import the single unified router ---
from .routers import unified_data_router

# --- Connection Manager Lifecycle ---
from .websocket_manager import startup_connection_manager, shutdown_connection_manager

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - line %(lineno)d - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- FastAPI Application Initialization ---
app = FastAPI(
    title="Trading Platform API",
    description="Backend API for historical data, live data feeds, and strategy execution.",
    version="2.0.0", # Version bump
    docs_url=None,
    redoc_url=None
)

# --- Static File Serving ---
script_dir = os.path.dirname(__file__)
frontend_dir = os.path.join(os.path.dirname(script_dir), "frontend")

app.mount("/static", StaticFiles(directory=os.path.join(frontend_dir, "static")), name="static")
app.mount("/dist", StaticFiles(directory=os.path.join(frontend_dir, "dist")), name="dist")

# --- Middleware Configuration ---
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
    logging.info("WebSocket connection manager started. Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Application shutting down...")
    await shutdown_connection_manager()
    logging.info("WebSocket connection manager stopped. Application shutdown complete.")

# --- NEW: Include the single unified router ---
app.include_router(unified_data_router.router)

# --- Root Endpoint ---
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root():
    """Serves the main index.html file."""
    index_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r") as f:
            return HTMLResponse(content=f.read())
    raise HTTPException(status_code=404, detail="index.html not found")

# --- Health Check Endpoint ---
@app.get("/health/websocket", tags=["Health"])
async def websocket_health():
    """Provides metrics for the WebSocket connection manager."""
    from .websocket_manager import connection_manager
    return {"status": "healthy", "metrics": connection_manager.get_metrics()}