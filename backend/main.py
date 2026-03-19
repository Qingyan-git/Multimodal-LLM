from download_data import download_folder
from chunking import process_all_files
from postgre_setups import create_llm_db, create_db_tables, insert_pdfs, retrieve_chunks
from embedding import embed_documents

import dotenv
import os
from pathlib import Path

if __name__ == "__main__":

    create_llm_db()
    create_db_tables()

    # download_folder()

    insert_pdfs(Path(os.getenv('raw_dataset_path')))

    all_chunks = process_all_files()

    all_chunks = [chunk for doc_chunks in all_chunks for chunk in doc_chunks]

    retrieved_chunks = retrieve_chunks()

    embed_documents()
