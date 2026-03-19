import os
import dotenv
import unicodedata
from pathlib import Path
import pymupdf

from models.chunk import Chunk
from postgre_setups import save_document_chunks

dotenv.load_dotenv()


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


def calculate_pages_blocks(chunks):
    """
    Returns the combined sum of pages_blocks for the chunks
    """

    combined_pages_blocks = {}

    for chunk in chunks:
        for pb in chunk.pages_blocks:
                    for page_no, block in pb.items():
                        if page_no in combined_pages_blocks:
                            combined_pages_blocks[page_no].extend(block)
                        else:
                            combined_pages_blocks[page_no] = list(block)

    return combined_pages_blocks


def sliding_window_chunk(blocks,chunk_size=6,overlap=2):

    """
    Chunks the blocks objects with chunk_size and overlap parameters
    Returns chunked objects
    """

    grouped_chunks = []
    total_blocks = len(blocks)
    start_index = 0

    while start_index < total_blocks:

        window_chunks = blocks[start_index:min(start_index+chunk_size,total_blocks)]

        group_chunk = Chunk()
        group_chunk.type = 'text'
        group_chunk.document_name = blocks[0].document_name
        group_chunk.text = " ".join(chunk.text for chunk in window_chunks)
        group_chunk.pages_blocks = calculate_pages_blocks(window_chunks)

        grouped_chunks.append(group_chunk)
        start_index = start_index+chunk_size-overlap
    
    return grouped_chunks


def get_text_blocks(file):

    """
    Reads the pdf file at file arg which is a path, and reads the pdf text
    Returns a list of pdf text 
    """

    if file.is_file() and file.suffix.lower() == '.pdf':

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
                        chunk.text = clean_text(text)
                        chunk_page = page_number+1
                        chunk_block = [block_no]
                        chunk.pages_blocks = [{chunk_page: chunk_block}]
                        chunk.document_name = file.name
                        pdf_pages.append(chunk)

        return pdf_pages


def process_all_files():
    """
    Iterates through folder_path to process and chunk all of the files
    Saves the chunks to postgresql
    Returns the chunks to embedding
    """

    folder_path = Path(os.getenv('raw_dataset_path'))

    all_pdf_chunks = []

    for file in folder_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.pdf':

            print(f'Processing {file.name} now\n')


            print(f'\tExtracting text data from {file.name}\n')

            pdf_chunks = get_text_blocks(file)
            text_chunks = sliding_window_chunk(pdf_chunks)

            print(f'\tFinished extracting text data from {file.name}\n')


            print(f'\tInserting {file.name}\'s chunks into postgresql now\n')

            save_document_chunks(file.stem,text_chunks)

            print(f'\tFinished inserting chunks into postgresql\n')


            print(f'Finished processing {file.name} now\n\n')

            all_pdf_chunks.append(text_chunks)

    print(f'All pdfs processed\n\n')

    return all_pdf_chunks    




