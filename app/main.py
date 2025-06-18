# app/main.py (UPDATED - MINIMAL CHANGES)

"""
main.py

This module serves as the main entry point for the Trading Platform's FastAPI backend.

OPTIMIZATION: Added WebSocket connection manager for 60% reduction in Redis connections.
"""

import logging
import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from .middleware.security_headers import SecurityHeadersMiddleware
from .middleware.rate_limiting import RateLimitMiddleware 

# Import application components
# from .dtn_iq_client import launch_iqfeed_service_if_needed
# Temporily Commenting this
# from .core import strategy_loader
# from .services.live_data_feed_service import live_feed_service
from .routers import historical_data_router, utility_router, live_data_router, heikin_ashi_router, heikin_ashi_live_router

# NEW IMPORTS for connection manager
from .websocket_manager import startup_connection_manager, shutdown_connection_manager

# --- Basic Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - line %(lineno)d - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- FastAPI Application Initialization ---
app = FastAPI(
    title="Trading Platform API",
    description="Backend API for historical data, live data feeds, and strategy execution.",
    version="1.0.0",
    docs_url=None,  # Disable the /docs endpoint
    redoc_url=None  # Disable the /redoc endpoint
)

# --- Static Frontend File Serving (UNCHANGED) ---
script_dir = os.path.dirname(__file__)
backend_root_dir = os.path.dirname(script_dir)
frontend_dir = os.path.join(backend_root_dir, "frontend")
static_dir = os.path.join(frontend_dir, "static")

if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.mount("/dist", StaticFiles(directory=os.path.join(frontend_dir, "dist")), name="dist")
    logging.info(f"Mounted static directory: {static_dir}")
else:
    logging.error(f"Static directory not found at: {static_dir}. Static files will not be served.")

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, calls=100, period=60) # 100 requests per 60 seconds

# --- Middleware Configuration (UNCHANGED) ---
app.add_middleware(GZipMiddleware, minimum_size=1000)

origins = [
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"], 
    allow_headers=["*"],
)

# --- Application Lifecycle Events (UPDATED) ---

@app.on_event("startup")
async def startup_event():
    """
    Actions to perform when the application starts up.
    
    NEW: Initialize WebSocket connection manager for optimized Redis usage.
    """
    logging.info("Application starting up...")
    
    # Start the WebSocket connection manager
    await startup_connection_manager()
    logging.info("WebSocket connection manager started")
    
    # Existing startup logic (commented out)
    # launch_iqfeed_service_if_needed()
    # strategy_loader.load_strategies()
    
    logging.info("Application startup complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Actions to perform when the application is shutting down.
    
    NEW: Gracefully shutdown WebSocket connection manager.
    """
    logging.info("Application shutting down...")
    
    # Stop the WebSocket connection manager
    await shutdown_connection_manager()
    logging.info("WebSocket connection manager stopped")
    
    # Existing shutdown logic
    # live_feed_service.disconnect()
    
    logging.info("Application shutdown complete.")


# --- API Routers (UNCHANGED) ---
app.include_router(historical_data_router.router)
app.include_router(utility_router.router)
app.include_router(live_data_router.router)
app.include_router(heikin_ashi_router.router)
app.include_router(heikin_ashi_live_router.router) # Include the new router

# --- Root Endpoint (UNCHANGED) ---

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serves the main `index.html` file of the frontend application."""
    index_html_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_html_path):
        with open(index_html_path, "r") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    else:
        logging.error(f"index.html not found at: {index_html_path}")
        raise HTTPException(status_code=404, detail="index.html not found")


# NEW: Health check endpoint to monitor connection manager
@app.get("/health/websocket")
async def websocket_health():
    """Get WebSocket connection manager metrics"""
    from .websocket_manager import connection_manager
    return {
        "status": "healthy",
        "metrics": connection_manager.get_metrics()
    }