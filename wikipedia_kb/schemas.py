from pydantic import BaseModel
from typing import Literal, Optional, Dict, Any
from naptha_sdk.schemas import KBConfig

class InputSchema(BaseModel):
    mode: Literal["init", "query", "add_data"]
    query: Optional[str] = None
    data: Optional[str] = None

class WikipediaKBConfig(KBConfig):
    table_name: str
    query_col: str
    answer_col: str
    schema: Dict[str, Any]