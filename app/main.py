"""
FastAPI application entry point.
Main application with routes, middleware, and lifecycle management.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import time
from app.config import settings
from app.db.connection import mongodb
from app.db.redis_client import redis_client

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting DataVault API...")
    
    # Connect to databases
    await mongodb.connect()
    await redis_client.connect()
    
    # Initialize Sentry if configured
    if settings.sentry_dsn and not settings.sentry_dsn.startswith("your_"):
        import sentry_sdk
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.environment,
            traces_sample_rate=0.1 if settings.is_production else 1.0,
        )
        logger.info("Sentry initialized")
    
    logger.info("DataVault API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down DataVault API...")
    await mongodb.disconnect()
    await redis_client.disconnect()
    logger.info("DataVault API shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="DataVault API",
    description="Dataset Discovery and Trend Analysis Platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests with timing."""
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} "
        f"completed in {process_time:.3f}s with status {response.status_code}"
    )
    
    # Add timing header
    response.headers["X-Process-Time"] = str(process_time)
    
    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.debug else "An error occurred"
        }
    )


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    try:
        # Check MongoDB connection
        await mongodb.client.admin.command('ping')
        mongo_status = "healthy"
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        mongo_status = "unhealthy"
    
    try:
        # Check Redis connection
        await redis_client.client.ping()
        redis_status = "healthy"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "unhealthy"
    
    overall_status = "healthy" if mongo_status == "healthy" and redis_status == "healthy" else "degraded"
    
    return {
        "status": overall_status,
        "services": {
            "mongodb": mongo_status,
            "redis": redis_status
        },
        "environment": settings.environment
    }


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "DataVault API",
        "version": "1.0.0",
        "docs": "/docs" if settings.debug else None
    }


# Import and include routers
from app.routes import datasets, analytics, users, reviews, admin

app.include_router(datasets.router, prefix="/api/datasets", tags=["datasets"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["analytics"])
app.include_router(users.router, prefix="/api/users", tags=["users"])
app.include_router(reviews.router, prefix="/api/reviews", tags=["reviews"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info"
    )
