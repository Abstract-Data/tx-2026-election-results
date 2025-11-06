"""FastAPI main application."""
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Config
from src.database import DatabaseConnectionFactory, init_db
from tx_election_results.utils.observers import MetricsObserver, RequestLogger

# Global observers
observers = [RequestLogger(), MetricsObserver()]

# Global database components
engine = None
session_factory = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Lifespan context manager for FastAPI app."""
    global engine, session_factory

    # Startup
    engine, session_factory = DatabaseConnectionFactory.create_connection(
        db_type=Config.DATABASE_TYPE, connection_string=Config.get_database_url()
    )
    await init_db(engine)

    yield

    # Shutdown
    await engine.dispose()


app = FastAPI(
    title="Texas 2026 Election Results API",
    description="API for Texas 2026 Election Results data",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for database session."""
    async with session_factory() as session:
        yield session


@app.middleware("http")
async def observer_middleware(request: Request, call_next):
    """Middleware to notify observers of requests and responses."""
    start_time = time.time()

    # Notify observers of request
    for observer in observers:
        observer.on_request(request.method, str(request.url.path), dict(request.query_params))

    try:
        response = await call_next(request)
        response_time = time.time() - start_time

        # Notify observers of response
        for observer in observers:
            observer.on_response(response.status_code, response_time)

        return response

    except Exception as e:
        response_time = time.time() - start_time

        # Notify observers of error
        for observer in observers:
            observer.on_error(e, {"path": str(request.url.path), "method": request.method})

        raise


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Texas 2026 Election Results API", "version": "1.0.0"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/metrics")
async def metrics():
    """Get API metrics."""
    metrics_observer = next((o for o in observers if isinstance(o, MetricsObserver)), None)
    if metrics_observer:
        return metrics_observer.get_metrics()
    return {"message": "Metrics not available"}


# Import routers
from src.api.endpoints import districts, early_voting, turnout, voters

app.include_router(voters.router, prefix="/api/voters", tags=["voters"])
app.include_router(turnout.router, prefix="/api/turnout", tags=["turnout"])
app.include_router(early_voting.router, prefix="/api/early-voting", tags=["early-voting"])
app.include_router(districts.router, prefix="/api/districts", tags=["districts"])

