# app/middleware/rate_limiting.py
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import time
import json
# NEW: Import redis_client from app.cache
from ..cache import redis_client

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, calls: int = 100, period: int = 60):
        super().__init__(app)
        self.calls = calls
        self.period = period

    async def dispatch(self, request: Request, call_next):
        # Exclude static files and health checks from rate limiting
        if request.url.path.startswith("/static/") or \
           request.url.path.startswith("/dist/") or \
           request.url.path == "/health/websocket" or \
           request.url.path == "/": # Exclude root path (index.html)
            response = await call_next(request)
            return response

        client_ip = request.client.host
        current_time = time.time()

        # Rate limiting key
        key = f"rate_limit:{client_ip}"

        # Get existing data
        data = redis_client.get(key)
        if data:
            requests = json.loads(data)
            # Remove old requests outside the time window
            requests = [req_time for req_time in requests 
                       if current_time - req_time < self.period]
        else:
            requests = []

        # Check if limit exceeded
        if len(requests) >= self.calls:
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded"
            )

        # Add current request
        requests.append(current_time)
        # Use SETEX to set expiration automatically
        redis_client.setex(key, self.period, json.dumps(requests))

        response = await call_next(request)
        return response