from pathlib import Path
import os
import dotenv
import json
from langchain_text_splitters import RecursiveCharacterTextSplitter


def retrieve_data(folder_path):
    '''
    Uses folder path to return the full text, page breaks and metadata of the document
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    print(f'Retriving data from folder')

    document_page_offset = []
    running_index = 0
    full_text = ""

    for file in folder_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.json':
            with open(file,'r',encoding='utf-8') as document:
                doc = json.load(document)
                doc_metadata = doc['metadata']
                doc_pages = doc['pages']

                for page in doc_pages:
                    page_number = page['page_number']
                    page_content = page['page_content']
                    page_start = running_index
                    page_end = running_index + len(page_content)

                    full_text += page_content
                    
                    document_page_offset.append({'page':page_number,'start':page_start,'end':page_end})
                    running_index += len(page_content) + 1

    return (full_text,document_page_offset,doc_metadata)



def chunk_text(document_information, chunk_size=300, chunk_overlap=60):
    '''
    Takes in some document_information, chunks the page_content of that document, and returns an 
    object with the chunks labelled with page number and metadata
    '''
    chunks = []
    chunk_id = 1
    pointer = 0

    full_text,document_page_offset,doc_metadata = document_information

    splitter = RecursiveCharacterTextSplitter(
        separators=['.'],
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        keep_separator=True,
        add_start_index=True,
        strip_whitespace=True
    )

    chunks_with_index = splitter.split_text(full_text)
    chunked_data = []

    print(chunks_with_index)
    
















#if __name__ == '__main__':
dotenv.load_dotenv()
chunking_data_download = Path(os.getenv('chunking_data_download'))

doc_data = retrieve_data(chunking_data_download)
chunk_text(doc_data)


