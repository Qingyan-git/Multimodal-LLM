from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple


@dataclass
class Chunk:

    id: Optional[int] = None

    type: Optional[str] = "text"
    
    text: Optional[str] = None
    image_data: Optional[bytes] = None

    pages_blocks: List[Dict] = field(default_factory=list)

    document_name : Optional[str] = None
