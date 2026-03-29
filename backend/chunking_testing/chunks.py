from dataclasses import dataclass
from typing import Optional

@dataclass
class Chunk:

    id : Optional[int] = None

    type : Optional[str] = None

    context : Optional[str] = None

    content : Optional[dict] = None

    metadata : Optional[dict] = None


@dataclass
class TextChunk(Chunk):

    def __post_init__(self):
        self.type = 'text'
        self.content = {'text': ""}


@dataclass
class ImageChunk(Chunk):

    def __post_init__(self):
        self.type = 'image'
        self.content = {
            'text' : "",
            'image' : b"",
        }


@dataclass
class TableChunk(Chunk):

    def __post_init__(self):
        self.type = 'table'
        self.content = {'markdown_text' : ""}
