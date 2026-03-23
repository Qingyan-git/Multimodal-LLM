from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple


@dataclass
class Chunk:

    id: Optional[int] = None

    type: Optional[str] = None
    
    text: Optional[str] = None
    image_data: Optional[bytes] = None

    pages: Optional[List] = None

    document_name : Optional[str] = None
