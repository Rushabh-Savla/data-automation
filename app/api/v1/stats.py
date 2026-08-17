from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.models.asset import Asset
from app.db.models.job import Job
from app.domain.enums import JOB_ACTIVE_STATUSES

router = APIRouter(prefix="/stats", tags=["stats"], dependencies=[Depends(require_api_key)])


@router.get("/overview")
def overview(db: Session = Depends(get_db)):
    active_jobs = db.execute(
        select(func.count()).select_from(Job).where(Job.status.in_(JOB_ACTIVE_STATUSES))
    ).scalar_one()
    total_jobs = db.execute(select(func.count()).select_from(Job)).scalar_one()
    total_assets = db.execute(select(func.count()).select_from(Asset)).scalar_one()
    return {
        "active_jobs": active_jobs,
        "total_jobs": total_jobs,
        "total_assets": total_assets,
    }
