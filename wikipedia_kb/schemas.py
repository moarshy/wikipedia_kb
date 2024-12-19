from pydantic import BaseModel
from typing import Literal, Optional

class InputSchema(BaseModel):
    mode: Literal["init", "query", "add_data"]
    query: Optional[str] = None
    data: Optional[str] = None
