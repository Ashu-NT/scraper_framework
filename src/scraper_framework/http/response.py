from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class HttpResponse:
    """HTTP response data."""

    status_code: int
    headers: Dict[str, str]
    text: str
    json: Any
