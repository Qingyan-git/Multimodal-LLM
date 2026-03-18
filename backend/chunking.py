import os
import dotenv
import unicodedata
from pathlib import Path
import pymupdf

from models.chunk import Chunk


def clean_text(text):

    """
    Cleans the input text content
    Returns the text content
    """

    text = unicodedata.normalize('NFKC',text)

    text = text.replace("\r\n", "\n").replace("\r", "")
    text = text.replace("\u2028", "\n").replace("\u2029", "\n\n")
    text = ''.join(char for char in text if char.isprintable() or char in '\n')
    text = text.strip()
    text = text.replace('\n',' ')

    return text


def compare_bbox(chunk_bbox,page_bbox):

    """
    Returns the calculated bbox coordinates for a group of chunks
    """

    return (min(chunk_bbox[0],page_bbox[0]),min(chunk_bbox[1],page_bbox[1]),max(chunk_bbox[2],page_bbox[2]),max(chunk_bbox[3],page_bbox[3]))


def calculate_pages_blocks(chunks):
    """
    Returns the combined sum of pages_blocks for the chunks
    """

    combined_pages_blocks = {}

    for chunk in chunks:
        for pb in chunk.layout.pages_blocks:
                    for page_no, block in pb.items():
                        if page_no in combined_pages_blocks:
                            combined_pages_blocks[page_no].extend(block)
                        else:
                            combined_pages_blocks[page_no] = list(block)

    return combined_pages_blocks


def get_text_blocks(file):

    """
    Reads the pdf file at file arg which is a path, and reads the pdf text
    Returns a list of pdf text 
    """

    if file.is_file() and file.suffix.lower() == '.pdf':

        print(f'Processing {file.name}\n')
        pdf_pages = []

        with pymupdf.open(file) as doc:
            for page_number, page in enumerate(doc):
                page_blocks = page.get_text("blocks")  # (x0, y0, x1, y1, text, block_no, block_type)
                for block in page_blocks:
                    text = block[4]
                    block_no = block[5]
                    block_type = block[6]
                    if block_type == 0: #text block
                        chunk = Chunk()
                        chunk.type = 'text'
                        chunk.content.text = clean_text(text)
                        chunk_page = page_number+1
                        chunk_block = [block_no]
                        chunk.layout.pages_blocks = [{chunk_page: chunk_block}]
                        chunk.structure.document_name = file.name
                        pdf_pages.append(chunk)

        print(f'Finished processing {file.name}\n\n')
        return pdf_pages


def group_text_blocks(blocks,chunk_size=6,overlap=2):

    """
    Chunks the blocks objects with chunk_size and overlap parameters
    Returns chunked objects
    """

    grouped_chunks = []
    total_blocks = len(blocks)
    start_index = 0

    print(f'Chunking {blocks[0].structure.document_name}\n')

    while start_index < total_blocks:

        window_chunks = blocks[start_index:min(start_index+chunk_size,total_blocks)]

        group_chunk = Chunk()
        group_chunk.type = 'text'
        group_chunk.structure.document_name = blocks[0].structure.document_name
        group_chunk.content.text = " ".join(chunk.content.text for chunk in window_chunks)
        group_chunk.layout.pages_blocks = calculate_pages_blocks(window_chunks)

        grouped_chunks.append(group_chunk)
        start_index = start_index+chunk_size-overlap
    
    print(f'Finished chunking {blocks[0].structure.document_name}\n\n')
    return grouped_chunks


def get_all_chunks(folder_path):
    """
    Iterates through folder_path to chunk all of the pdf documents
    """

    all_pdfs = []

    print(f'Processing all pdfs\n')

    for file in folder_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.pdf':

            pdf_chunks = get_text_blocks(file)
            grouped_pdf_chunks = group_text_blocks(pdf_chunks)
            all_pdfs.append(grouped_pdf_chunks)

    print(f'All pdfs processed')
    return(all_pdfs)



dotenv.load_dotenv()

path = Path(os.getenv('raw_dataset_path'))

all_pdf_chunks = get_all_chunks(path)
gov_data_sec_pol = all_pdf_chunks[1]
chunk_1 = gov_data_sec_pol[0]

print(chunk_1)

