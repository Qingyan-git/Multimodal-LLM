import os
import dotenv
import unicodedata
from pathlib import Path
import pymupdf
from PIL import Image
from io import BytesIO
import pytesseract
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


def extract_text_from_block(block):
    """
    Extracts clean text from a PyMuPDF text block (type == 0)
    """

    text = ""

    for line in block.get("lines",""):
        for span in line.get("spans", ""):
            text += span.get("text", "") + " "

    text = clean_text(text)

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


def extract_image_bytes(doc,block):
    """
    Extracts out an image's bytes at the block no of doc
    """
    
    image_data = block.get("image")

    if isinstance(image_data, int):
        base_image = doc.extract_image(image_data)
        return base_image['image']
    elif isinstance(image_data, bytes):
        return image_data


def bytes_to_pil(image_bytes):
    """
    Returns a PIL object created from image_bytes
    """

    return Image.open(BytesIO(image_bytes)).convert("RGB")


def ocr_image(image):
    """
    Uses pytesseract to extract out any OCR text from image
    """
    text = pytesseract.image_to_string(image)
    text = clean_text(text)

    return text


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


def get_file_chunks(file):
    """
    Reads the pdf file at file arg which is a path, and reads the pdf text and images
    Returns a tuple containing two lists of text and image chunks
    """

    if file.is_file() and file.suffix.lower() == '.pdf':
        pdf_pages = []
        with pymupdf.open(file) as doc:
            for page_number, page in enumerate(doc):

                page_blocks = page.get_text('dict')['blocks']

                for i, block in enumerate(page_blocks):

                    if block['type'] == 0: #text block
                        chunk = Chunk()
                        chunk.type = 'text'
                        chunk.text = extract_text_from_block(block)
                        chunk.pages_blocks = [{page_number: [i]}]
                        chunk.document_name = file.name

                        pdf_pages.append(chunk)

                    elif block['type'] == 1: #image block

                        block_indices = [i]
                        chunk_text = ""

                        if i - 1 >= 0 and page_blocks[i - 1]["type"] == 0:
                            prev_text = extract_text_from_block(page_blocks[i - 1])
                            chunk_text += prev_text + " "
                            block_indices.append(i - 1)

                        if i + 1 < len(page_blocks) and page_blocks[i + 1]["type"] == 0:
                            next_text = extract_text_from_block(page_blocks[i + 1])
                            chunk_text += next_text + " "
                            block_indices.append(i + 1)

                        image_data = extract_image_bytes(doc,block)
                        image_ocr_text = ocr_image(bytes_to_pil(image_data))
                        chunk_text += image_ocr_text
                        
                        chunk = Chunk()
                        chunk.type = 'image'
                        chunk.text = chunk_text
                        chunk.image_data = image_data
                        chunk.pages_blocks = [{page_number: sorted(block_indices)}]
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


            print(f'\tExtracting chunks from {file.name}\n')

            text_data,image_chunks = get_file_chunks(file)
            text_chunks = sliding_window_chunk(text_data)

            print(f'\tFinished extracting chunks from {file.name}\n')


            print(f'\tInserting {file.name}\'s chunks into postgresql now\n')

            save_document_chunks(file.stem,text_chunks)

            print(f'\tFinished inserting chunks into postgresql\n')


            print(f'Finished processing {file.name} now\n\n')

            all_pdf_chunks.append(text_chunks)

    print(f'All pdfs processed\n\n')

    return all_pdf_chunks    




