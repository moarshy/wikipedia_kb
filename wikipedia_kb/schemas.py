from pydantic import BaseModel
from typing import Literal, Optional

class InputSchema(BaseModel):
    mode: Literal["init", "query"]
    query: Optional[str] = None