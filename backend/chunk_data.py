from pathlib import Path
import os
import dotenv
import json
# import spacy
from langchain_text_splitters import RecursiveCharacterTextSplitter


from process_pdf import process_doc_text
from postgre_setups import save_chunks


def get_page_breaks(doc):
    """
    Takes in a document dictionary to find out the boundary of each page
    Returns a tuple of the full_text and the page_breaks
    """

    full_text = ""
    page_breaks = []
    start_index = 0
    end_index = 0

    doc_content = doc.get('content')

    for page_content in doc_content:

        page = {
            'page_number' : 0,
            'start_index' : 0,
            'end_index' : 0
        }

        full_text += page_content['text'].replace('\n','')
        end_index = len(full_text)

        page['page_number'] = page_content['page_number']
        page['start_index'] = start_index
        page['end_index'] = end_index

        start_index = end_index

        page_breaks.append(page)


    return (full_text,page_breaks)



def recursive_character_chunk(text,page_breaks,chunk_size=500,overlap=100):
    """
    Uses text_splitters' RecursiveCharacterTextSplitter module to chunk the text
    Accounts for page breaks
    Returns the chunks
    """

    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size,chunk_overlap=overlap)
    paragraphs = splitter.split_text(text)

    start_index = 0
    chunks = []

    for paragraph in paragraphs:
        chunk = {
            'text' : "",
            'pages' : []
        }

        paragraph = paragraph.replace('\n', '')
        chunk['text'] = paragraph

        chunk_start = text.find(paragraph,start_index)
        chunk_end = chunk_start + len(paragraph)

        for page in page_breaks:
            if chunk_start < page["end_index"] and chunk_end > page["start_index"]:
                chunk['pages'].append(page["page_number"])

        chunks.append(chunk)

    return chunks



def recursive_chunk_data(documents):
    """
    Takes in a list of document dictionaries to chunk them using RecursiveCharacterTextSplittersimilar 
    Returns a similar object to be passed into embedding and storage into postgresql
    """

    all_chunks = []

    for doc in documents:
        print(f'Chunking {doc['name']} now\n')

        document = {
            'name' : doc['name'],
            'chunks' : []
        }

        full_text, page_breaks = get_page_breaks(doc)

        doc_chunks = recursive_character_chunk(full_text,page_breaks)
        document['chunks'] = doc_chunks
        all_chunks.append(document)


    print(f'All documents chunked, saving to postgresql db now\n\n')
    
    save_chunks(all_chunks)

    return all_chunks



        


#if __name__ == '__main__':
dotenv.load_dotenv()

raw_dataset_path = os.getenv('raw_dataset_path')

if raw_dataset_path is None:
    raise AttributeError("Environment variable not found, please check your env files \n\n")

documents = process_doc_text(Path(raw_dataset_path))

if documents:
    chunks = recursive_chunk_data(documents)

    if chunks:

        document = chunks[1]

        print(f'document name : {document['name']}')
        print(f'document first chunk : {document['chunks'][0]}')




