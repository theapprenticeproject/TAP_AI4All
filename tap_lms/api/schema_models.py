from pydantic import BaseModel
from typing import Optional, Dict, Any

class QueryRequest(BaseModel):
    q: str
    engine: Optional[str] = None  # "sql" | "graph" | "hybrid"
    params: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    question: str
    answer: str
    engine_used: str
    success: bool = True
    confidence: float = 1.0
