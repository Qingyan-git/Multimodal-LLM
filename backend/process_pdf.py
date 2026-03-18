import pymupdf
import os
import dotenv
import unicodedata
from pathlib import Path
import json

import models.chunk as chunk



def clean_pdf_text(text):

    '''
    Cleans a pdf page's text content
    '''

    text = unicodedata.normalize('NFKC',text)

    text = text.replace("\r\n", "\n").replace("\r", "")
    text = text.replace("\u2028", "\n").replace("\u2029", "\n\n")
    text = ''.join(char for char in text if char.isprintable() or char in '\n')
    text = text.strip()

    return text


def process_doc_text(folder_path):
    
    '''
    Processes the text of pdf documents at folder_path to extract out the page based content
    Returns the stored text to pass into chunking function
    '''

    all_pdfs = []
    for file in folder_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.pdf':
            print(f'Processing {file.name}\n')

            pdf_document = {
                        'name' : "",
                        'content' : []
                    }
            
            with pymupdf.open(file) as doc:
                if doc:
                    pdf_document['name'] = file.stem
                    for page_no, page in enumerate(doc): # type: ignore
                        if page.get_text().strip():
                            clean_page_text = clean_pdf_text(page.get_text())

                            content = {
                                'page_number' : page_no,
                                'text' : clean_page_text
                            }

                            pdf_document['content'].append(content)

            if pdf_document:
                all_pdfs.append(pdf_document)

    print(f'All files processed\n\n')

    return all_pdfs if not None else None





# if __name__ == '__main__':
dotenv.load_dotenv()
raw_dataset_path = os.getenv('raw_dataset_path')

if raw_dataset_path == None:
    raise AttributeError("Environment variable not found, please check your env files \n\n")

documents = process_doc_text(Path(raw_dataset_path))

