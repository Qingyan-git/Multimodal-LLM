from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple

@dataclass
class Content:
    text: Optional[str] = field(default_factory=str)

    image_data: Optional[bytes] = field(default_factory=str)

@dataclass
class Layout:
    pages_blocks: Optional[List] = field(default_factory=list)

    bbox: Optional[Tuple[float, float, float, float]] = field(default_factory=tuple)

@dataclass
class Structure:
    document_name: Optional[str] = field(default_factory=str)

@dataclass
class Relationships:
    previous_element : Optional[object] = None
    next_element : Optional[object] = None

@dataclass
class Chunk:
    type : str = field(default_factory=str)
    content: Content = field(default_factory=Content)
    layout: Layout = field(default_factory=Layout)
    structure: Structure = field(default_factory=Structure)
    relationships : Relationships = field(default_factory=Relationships)

