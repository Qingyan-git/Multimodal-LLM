from download_data import download_folder
from chunking import process_all_files
from postgre_setups import create_llm_db, create_db_tables, insert_pdfs
from embedding import embed_documents

import dotenv
import os
from pathlib import Path
import pytesseract
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

if __name__ == "__main__":
    dotenv.load_dotenv()

    # create_llm_db()
    # create_db_tables()

    # # download_folder()

    # raw_dataset_path = os.getenv('raw_dataset_path')
    
    # if not raw_dataset_path:
    #     print(f'Environment variable not found\n')
    #     raise
    # raw_dataset_path = Path(raw_dataset_path)

    # insert_pdfs(raw_dataset_path)

    # process_all_files()

    embed_documents()
