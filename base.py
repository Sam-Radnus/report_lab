from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional

class Status(Enum):
    CREATED = 'CREATED'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    UPLOAD_STARTED = 'UPLOAD_STARTED'
    FINISHED = 'FINISHED'
    REJECTED = 'REJECTED'
    FAILED = 'FAILED'

class Report(BaseModel):
    report_id: int
    batch_no: int
    status: Status = Status.CREATED
    s3_key: Optional[str] = None
    payload: dict
    error_msg: Optional[str] = None