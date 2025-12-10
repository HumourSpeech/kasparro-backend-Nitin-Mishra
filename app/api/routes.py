from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select, func
from typing import List, Optional
import time
from app.core.db import get_session, engine
from app.schemas.models import UnifiedUser, ETLJob
from sqlalchemy import text

router = APIRouter()

@router.get("/data")
def get_data(
    session: Session = Depends(get_session),
    limit: int = 10,
    offset: int = 0,
    role: Optional[str] = None,
    source: Optional[str] = None
):
    start_time = time.time()
    
    query = select(UnifiedUser)
    if role:
        query = query.where(UnifiedUser.role == role)
    if source:
        query = query.where(UnifiedUser.source == source)
    
    total_count = session.exec(select(func.count()).select_from(query.subquery())).one()
    results = session.exec(query.offset(offset).limit(limit)).all()
    
    latency = (time.time() - start_time) * 1000
    
    return {
        "metadata": {
            "request_id": str(time.time()), # Simple ID for now
            "api_latency_ms": round(latency, 2),
            "total_records": total_count,
            "limit": limit,
            "offset": offset
        },
        "data": results
    }

@router.get("/health")
def health_check(session: Session = Depends(get_session)):
    # Check DB
    db_status = "unhealthy"
    try:
        session.exec(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        pass

    # Check ETL
    last_run = session.exec(select(ETLJob).order_by(ETLJob.start_time.desc())).first()
    etl_status = {
        "last_run_status": last_run.status if last_run else "never_run",
        "last_run_time": last_run.start_time if last_run else None
    }

    return {
        "database": db_status,
        "etl": etl_status
    }

@router.get("/stats")
def get_stats(session: Session = Depends(get_session)):
    total_processed = session.exec(select(func.sum(ETLJob.records_processed))).one() or 0
    last_success = session.exec(select(ETLJob).where(ETLJob.status == "success").order_by(ETLJob.end_time.desc())).first()
    last_failure = session.exec(select(ETLJob).where(ETLJob.status == "failed").order_by(ETLJob.end_time.desc())).first()
    
    # Calculate average duration
    jobs = session.exec(select(ETLJob).where(ETLJob.end_time != None)).all()
    durations = [(job.end_time - job.start_time).total_seconds() for job in jobs if job.end_time and job.start_time]
    avg_duration = sum(durations) / len(durations) if durations else 0

    return {
        "total_records_processed": total_processed,
        "average_duration_seconds": round(avg_duration, 2),
        "last_success": last_success.end_time if last_success else None,
        "last_failure": last_failure.end_time if last_failure else None,
        "total_runs": len(jobs)
    }
