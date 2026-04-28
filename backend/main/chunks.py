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


@dataclass
class PageChunk(Chunk):
    type : str = 'page'


class ViDoReChunk(Chunk):
    type : str = 'ViDoRe'
    parent_id: Optional[str] = None

    def __post_init__(self):
        if self.parent_id:
            self.metadata['corpus_id'] = self.parent_id