import os
import dotenv
import unicodedata
from pathlib import Path
import pymupdf
from PIL import Image
from io import BytesIO
from models.chunk import Chunk
from postgre_setups import save_document_chunks
import pytesseract


pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
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


def calculate_pages(chunks):
    """
    Returns the combined pages for the chunks
    """

    pages = []

    for chunk in chunks:
        for page in chunk.pages:
            if page not in pages:
                pages.append(page)

    return pages


def extract_text(block):
    """
    Extracts text out from a text block element
    """

    full_text = []
    for line in block.get('lines', []):
        for span in line.get('spans', []):
            span_text = span.get('text', '').strip()
            if span_text:
                full_text.append(span_text)

    return " ".join(full_text)


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
        group_chunk.pages = calculate_pages(window_chunks)

        grouped_chunks.append(group_chunk)
        start_index = start_index+chunk_size-overlap
    
    return grouped_chunks


def ocr_image(image):
    """
    Assume PIL image
    Uses pytesseract to extract out any OCR text from image
    """

    text = pytesseract.image_to_string(image)
    text = clean_text(text)

    return text


def filter_image(image,min_dim=100,min_color=5):
    """
    Assumes pixmap image is passed in
    Checks to see if the image is of usuable quality
    Returns image data and useability flag
    """

    use = True

    if image.height < min_dim or image.width < min_dim:
        use = False

    if image.is_monochrome or image.is_unicolor or image.color_count() < min_color:
        use = False

    return image,use


def normalise_image(doc,page,block,zoom=2.0):
    """
    Takes in the parameters to construct a pixmap of the image at doc page block, and then filters to see whether it can be used
    Returns a tuple of the image data and whether it is usuable
    """

    try:
        xref = block.get("xref", 0)
        if xref > 0:
            pix = pymupdf.Pixmap(doc, xref) #Pixmap.__init__(doc,xref)
        else:
            pix = pymupdf.Pixmap(block["image"]) #Pixmap.__init__(stream)
        pix = pymupdf.Pixmap(pymupdf.csRGB, pix) #Pixmap.__init__(colorspace,source)

        pix,use = filter_image(pix)

        return (pix,use)

    except Exception:

        try:
            bbox = block.get("bbox")
            mat = pymupdf.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, clip=bbox, alpha=False)
            pix = pymupdf.Pixmap(pymupdf.csRGB, pix)

            pix,use = filter_image(pix)

            return (pix,use)

        except Exception as e:
            print(f"Bad image data, error {e}\n")
            raise
            


def get_file_chunks(file):
    """
    Reads the pdf file at file arg which is a path, and reads the pdf text and images
    Returns a tuple containing two lists of text and image chunks
    """

    try:

        if file.is_file() and file.suffix.lower() == '.pdf':
            text_chunks = []
            image_chunks = []
            with pymupdf.open(file) as doc:
                for page_number, page in enumerate(doc): # type: ignore

                    page_blocks = page.get_text('dict')['blocks']

                    for block_no, block in enumerate(page_blocks):

                        if block['type'] == 0: #text block
                            chunk = Chunk()
                            chunk.type = 'text'
                            chunk.text = extract_text(block)
                            chunk.pages = [page_number]
                            chunk.document_name = file.name

                            text_chunks.append(chunk)

                        elif block['type'] == 1: #image block

                            image_data,use = normalise_image(doc,page,block)

                            if use:

                                chunk_text = ""
                                for i in range(block_no,-1,-1):
                                    if page_blocks[i]['type'] == 0:
                                        chunk_text += f"Previous Context : {extract_text(page_blocks[i])}"
                                        break

                                chunk_text += f" OCR TEXT : {ocr_image(image_data.pil_image())}"

                                for i in range(block_no+1,len(page_blocks)):
                                    if page_blocks[i]['type'] == 0:
                                        chunk_text += f"Next Context : {extract_text(page_blocks[i])}"
                                        break

                                chunk = Chunk()
                                chunk.type = 'image'
                                chunk.text = chunk_text.strip()
                                chunk.image_data = image_data.tobytes(output='png')
                                chunk.pages = [page_number]
                                chunk.document_name = file.name

                                image_chunks.append(chunk)
                                # print(f'Showing image, {image_data.pil_image().show()}\n')
                            
            return (text_chunks,image_chunks)
        
        else:
            raise Exception(f'{file} is not valid')
    
    except Exception as e:
        print(f'Error when extracting chunks from {file}, error {e}\n')
        raise


def process_all_files():
    """
    Iterates through folder_path to process and chunk all of the files
    Saves the chunks to postgresql
    Returns the chunks to embedding
    """

    folder_path = os.getenv('raw_dataset_path')

    if not folder_path:
        raise ValueError('Environment variable not found, please check your .env file\n')

    folder_path = Path(folder_path)

    for file in folder_path.iterdir():

        if file.is_file() and file.suffix.lower() == '.pdf':

            print(f'Processing {file.name} now\n')


            print(f'\tExtracting chunks from {file.name}\n')

            text_chunks,image_chunks = get_file_chunks(file)

            print(f'\tFinished extracting chunks from {file.name}\n')

            
            print(f'\tChunking text chunks by sliding window chunk\n')

            texts = sliding_window_chunk(text_chunks)

            print(f'\tFinished sliding window chunk\n\n')


            print(f'\tInserting {file.name}\'s chunks into postgresql now\n')

            save_document_chunks(file.stem,texts)
            save_document_chunks(file.stem,image_chunks)

            print(f'\tFinished inserting chunks into postgresql\n')


            print(f'Finished processing {file.name} now\n\n')

    print(f'All pdfs processed\n\n')
