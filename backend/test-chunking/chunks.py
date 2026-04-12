from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Chunk:

    id : Optional[int] = None

    document_name : Optional[str] = ""

    type : Optional[str] = ""

    context : Optional[str] = ""

    content : Optional[dict] = field(default_factory=dict)

    metadata : Optional[dict] = field(default_factory=dict)


"""
Remember that context is for the contexutal retrieval and will not be shown in the final result,
while content is the portion of the document that refers to the actual data extracted
"""


@dataclass
class TextChunk(Chunk):

    type : str = 'text'
    content: dict = field(default_factory=lambda: {'text': ""})


@dataclass
class ImageChunk(Chunk):

    type : str = 'image'
    content :  dict = field(default_factory=lambda: {'text': "", 'image': b""})


@dataclass
class TableChunk(Chunk):

    type : str = 'table'
    content :  dict = field(default_factory=lambda: {'table': "", 'surrounding_texts': b""})
