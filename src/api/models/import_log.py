from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field

class ImportLog(SQLModel, table=True):
    __tablename__ = "import_log"

    file_name: str = Field(primary_key=True)
    import_date: datetime = Field(default_factory=datetime.utcnow)
    rows_imported: Optional[int] = None
