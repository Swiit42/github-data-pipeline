from typing import List
from sqlmodel import Session, select
from ..models.import_log import ImportLog

def list_imports(session: Session, limit: int = 200, offset: int = 0) -> List[ImportLog]:
    stmt = select(ImportLog).order_by(ImportLog.import_date.desc()).offset(offset).limit(limit)
    return session.exec(stmt).all()
