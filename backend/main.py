from download_data import download_folder
from chunking import process_all_files
from postgre_setups import create_llm_db, create_db_tables
from embedding import embed_documents

if __name__ == "__main__":

    # create_llm_db()
    # create_db_tables()

    # download_folder()

    # process_all_files()

    embed_documents()
