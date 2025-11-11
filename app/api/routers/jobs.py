"""
Endpoints for tracking import job progress.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.api.schemas.shared import ImportJobListResponse, ImportJobResponse
from app.domain.imports.jobs import get_import_job, list_import_jobs

router = APIRouter(tags=["import-jobs"])


@router.get("/import-jobs/{job_id}", response_model=ImportJobResponse)
async def get_import_job_endpoint(job_id: str):
    job = get_import_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return ImportJobResponse(success=True, job=job)


@router.get("/import-jobs", response_model=ImportJobListResponse)
async def list_import_jobs_endpoint(
    file_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    jobs, total = list_import_jobs(file_id=file_id, limit=limit, offset=offset)
    return ImportJobListResponse(
        success=True,
        jobs=jobs,
        total_count=total,
        limit=limit,
        offset=offset,
    )
