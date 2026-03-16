from pathlib import Path
import os
import dotenv
import json
# import spacy
from langchain_text_splitters import RecursiveCharacterTextSplitter
from process_pdf import process_doc_text
from postgresql import save_chunks


def manual_recursive_chunking(folder_path, chunk_size=800, overlap=150):
    '''
    Uses folder path to retrieve the data and returns chunks of paragraphs ready for embedding
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    for file in folder_path.iterdir():

        full_text = ""
        page_breaks = []

        with open(file,'r',encoding='utf-8') as document:
            doc = json.load(document)
            doc_metadata = doc['metadata']
            doc_pages = doc['pages']

            start_index = 0
            for page in doc_pages:
                page_number = page['page_number']
                page_content = page['page_content']

                full_text += page_content
                end_index = len(full_text)
                page_breaks.append({'page_number':page_number, 'start':start_index, 'end':end_index})
                start_index = end_index

        splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=overlap)
        paragraphs = splitter.split_text(full_text)
        
        start_index = 0
        document_paragraphs = []

        for paragraph in paragraphs:
            p = {
                'content' : "",
                'pages' : [],
                'metadata' : doc_metadata
            }

            paragraph = paragraph.replace('\n', "")
            p['content'] = paragraph

            chunk_start = full_text.find(paragraph, start_index)
            chunk_end = chunk_start + len(paragraph)

            for page in page_breaks:
                if chunk_start < page["end"] and chunk_end > page["start"]:
                    p['pages'].append(page["page_number"])

            document_paragraphs.append(p)

        return document_paragraphs

            

def manual_group_by_sentence_chunking(folder_path,chunk_size=8,overlap=2):
    '''
    Uses folder path to retrieve the data and returns chunks of groups of sentences ready for embedding
    '''

    document = manual_sentence_chunking(folder_path)

    pointer = 0
    chunks = []
    while pointer < len(document):

        end_pointer = min((pointer+chunk_size),len(document))
        chunk_sentences = document[pointer:end_pointer]

        chunk = {
            'content' : "",
            'pages' : [],
            'metadata' : chunk_sentences[0]["metadata"]
        }

        for sentence in chunk_sentences:
            chunk['content'] += sentence['content'] + " "
            for p in sentence['pages']:
                if p not in chunk['pages']:
                    chunk['pages'].append(p)

        chunks.append(chunk)
        pointer += chunk_size - overlap

    return chunks

        

def manual_sentence_chunking(folder_path):
    '''
    Uses folder_path to retrieve the data and returns sentence chunks ready for embedding
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    spacy_model = None #spacy.load("en_core_web_sm")
    
    for file in folder_path.iterdir():
        document_sentences = []

        with open(file,'r',encoding='utf-8') as document:
            doc = json.load(document)
            doc_metadata = doc['metadata']
            doc_pages = doc['pages']

            for page in doc_pages:
                page_number = page['page_number']
                page_content = page['page_content']
                cleaned_content = page_content.replace('\n',"")
                page_sentences = spacy_model(cleaned_content).sents
                for sentence in page_sentences:
                    document_sentences.append({
                        'content' : sentence.text,
                        'pages' : [page_number],
                        'metadata' : doc_metadata
                    })
        
        return document_sentences



def manual_store_chunk_data(folder_path,folder_name,chunks):
    '''
    Stores the chunk data locally at folder_path
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")
    
    save_path = folder_path / folder_name

    save_path.mkdir()

    file_path = save_path / 'chunks.json'

    with file_path.open('w', encoding='utf-8') as f:
        json.dump(chunks, f, ensure_ascii=False, indent=4)

    
    print(f'Chunks from {folder_name} saved\n\n')



def manual_retrieve_pdfs_and_store_chunks(folder_path,storage_path,chunking_method=manual_recursive_chunking):
    '''
    Retrieves each pdf from folder_path.
    Extract the chunks from each pdf, and saves the chunk data
    under the chunk directory
    '''
    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    for subfolder_path in folder_path.iterdir():
        print(f'Reading {subfolder_path.name}\n')

        chunks = chunking_method(subfolder_path)

        manual_store_chunk_data(storage_path,subfolder_path.name,chunks)



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
            'page_number' : int,
            'start_index' : int,
            'end_index' : int
        }

        full_text += page_content['text']
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
            'text' : str,
            'pages' : list
        }

        paragraph = paragraph.replace('\n', '')
        chunk['text'] = paragraph

        chunk_start = text.find(paragraph,start_index)
        chunk_end = chunk_start + len(paragraph)

        for page in page_breaks:
            if chunk_start < page["end"] and chunk_end > page["start"]:
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

        document = {
            'name' : doc['name'],
            'chunks' : None
        }

        full_text, page_breaks = get_page_breaks(doc)

        doc_chunks = recursive_character_chunk(full_text,page_breaks)
        document['chunks'] = doc_chunks
        all_chunks.append(document)

    # save_chunks(all_chunks)



        


#if __name__ == '__main__':
dotenv.load_dotenv()
# dataset_process_path = Path(os.getenv('dataset_process_path'))
# chunking_data_path = Path(os.getenv('chunking_data_path'))

# manual_retrieve_pdfs_and_store_chunks(dataset_process_path,chunking_data_path)

raw_dataset_path = Path(os.getenv('raw_dataset_path'))

documents = process_doc_text(raw_dataset_path)
print(documents)

recursive_chunk_data = recursive_chunk_data(documents)



