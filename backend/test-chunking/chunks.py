from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Chunk:

    id : Optional[int] = None

    document_name : Optional[str] = ""

    type : Optional[str] = ""

    context : Optional[str] = ""

    content : Optional[str] = ""

    metadata : Optional[dict] = field(default_factory=dict)


@dataclass
class TextChunk(Chunk):
    type : str = 'text'


@dataclass
class ImageChunk(Chunk):
    type : str = 'image'


@dataclass
class TableChunk(Chunk):
    type : str = 'table'
