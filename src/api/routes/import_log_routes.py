from typing import List
from fastapi import APIRouter, Depends, Body, Query
from sqlmodel import Session
from pathlib import Path
from ..database import get_session
from ..models.import_log import ImportLog
from ..services.import_log_service import list_imports
from ..importer_runner import run_import
from ..schemas import PipelineResponse

router = APIRouter(prefix="/imports", tags=["Import"])

@router.get("/", response_model=List[ImportLog])
def http_list_imports(limit: int = Query(200), offset: int = Query(0), session: Session = Depends(get_session)):
    return list_imports(session, limit, offset)

@router.post("/run", response_model=PipelineResponse)
def http_run_import(
    data_dir: str | None = Body(default=None, embed=True),
    dry_run: bool = Body(default=False),
    method: str = Body(default="copy"),
):
    return run_import(Path(data_dir) if data_dir else None, dry_run=dry_run, method=method)
