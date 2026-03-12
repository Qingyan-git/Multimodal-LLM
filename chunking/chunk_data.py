from pathlib import Path
import os
import dotenv
import json
import spacy
from langchain_text_splitters import RecursiveCharacterTextSplitter



def recursive_chunking(folder_path, chunk_size=800, overlap=150):
    '''
    Uses folder path to retrieve the data and returns chunks of paragraphs ready for embedding
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    print(f'Retriving data from folder')

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

            

def group_by_sentence_chunking(folder_path,chunk_size=8,overlap=2):
    '''
    Uses folder path to retrieve the data and returns chunks of groups of sentences ready for embedding
    '''

    document = sentence_chunking(folder_path)

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

        

def sentence_chunking(folder_path):
    '''
    Uses folder_path to retrieve the data and returns sentence chunks ready for embedding
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    print(f'Retriving data from folder')

    spacy_model = spacy.load("en_core_web_sm")
    
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






#if __name__ == '__main__':
dotenv.load_dotenv()
chunking_data_download = Path(os.getenv('chunking_data_download'))

doc_data_sentences = sentence_chunking(chunking_data_download)
doc_data_groupby = group_by_sentence_chunking(chunking_data_download)
doc_data_paragraphs = recursive_chunking(chunking_data_download)
# print(doc_data_sentences[0])
# print()
# print()
# print(doc_data_groupby[0])
# print()
# print()
print(doc_data_paragraphs[0])
print()
print()
print(doc_data_paragraphs[1])


