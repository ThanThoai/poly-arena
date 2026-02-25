from fastapi import APIRouter

from services.scheduler import scheduler_status

router = APIRouter()


@router.get("/scheduler/status")
def get_scheduler_status():
    return scheduler_status()
