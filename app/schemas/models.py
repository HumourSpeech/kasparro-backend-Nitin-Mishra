from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field
from pydantic import EmailStr

class RawData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str
    payload: str  # JSON string or raw CSV line
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    processed: bool = Field(default=False)

class UnifiedUser(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    original_id: str
    name: str
    email: EmailStr
    role: str
    signup_date: datetime
    source: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ETLJob(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str  # "running", "success", "failed"
    records_processed: int = 0
    error_message: Optional[str] = None

class Checkpoint(SQLModel, table=True):
    source_name: str = Field(primary_key=True)
    last_processed_id: Optional[str] = None
    last_processed_timestamp: Optional[datetime] = None
