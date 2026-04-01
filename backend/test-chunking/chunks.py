from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Chunk:

    id : Optional[int] = None

    type : Optional[str] = None

    context : Optional[str] = None

    content : Optional[dict] = field(default_factory=dict)

    metadata : Optional[dict] = field(default_factory=dict)


@dataclass
class TextChunk(Chunk):

    type : str = 'text'
    content: str = ""


@dataclass
class ImageChunk(Chunk):

    type : str = 'image'
    content :  dict = field(default_factory=lambda: {'text': "", 'image': b""})


# @dataclass
# class TableChunk(Chunk):

#     def __post_init__(self):
#         self.type = 'table'
#         self.content = {'markdown_text' : ""}
