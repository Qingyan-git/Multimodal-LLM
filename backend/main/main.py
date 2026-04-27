
import os

from text_chunking import process_text
from docling_image_chunking import process_images
from docling_table_chunk import process_tables
from page_chunking import process_pages

def chunk_all_files():

    data_path = os.getenv('all_pdfs_path')
    process_text(data_path)
    process_images(data_path)
    process_tables(data_path)