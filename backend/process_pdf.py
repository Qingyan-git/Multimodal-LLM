import pymupdf
import os
import dotenv
import unicodedata
from pathlib import Path
import json
from download_data import delete_all_files_in_folder



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



def iterate_folder(folder_path, output_path):
    '''
    Iterates through a folder and passes any pdf files with the output_path to 
    read and process data
    '''

    if not Path.exists(folder_path) or not Path.exists(output_path):
        raise FileNotFoundError("Please check that the path input is correct")

    for file in Path.iterdir(folder_path):
        if file.is_file() and file.suffix.lower() == '.pdf':
            print(f'Processing {file.name}.pdf now')
            read_doc_text(file,output_path)



def read_doc_text(file, output_path):
    '''
    Takes a file provided by iterate_folder function and processes its textual content, then writes
    the output into output_path, returns the path where the file was saved
    '''

    filename = file.stem
    output_path = output_path / filename

    if output_path.exists():
        print(f'Files already exist, deleting them for overwriting\n\n')
        delete_all_files_in_folder(output_path)
    else:
        print(f'No folder found, creating a directory there\n\n')
        Path.mkdir(output_path)

    with pymupdf.open(file) as doc:
        print(f'Processing {filename}, saving to {output_path}')
        doc_metadata = doc.metadata.copy()  # pylint: disable=no-member

        document = {'metadata': doc_metadata,
                     'pages' : []
                    }

        for i, page in enumerate(doc):
            page_text = page.get_text()

            if page_text.strip():   #skips empty page (q smart eh)
                clean_text = clean_pdf_text(page_text)

                document['pages'].append({'page_number' : i+1,
                                          'page_content' : clean_text
                                          })

        save_file_name = f'{filename}.json'
        save_path = output_path / save_file_name
        with open(save_path,'w',encoding='utf-8') as save:
            json.dump(document, save, ensure_ascii=False, indent=4)
        print(f'Saved document {save_file_name}\n')



def process_doc_text(folder_path):
    
    '''
    Processes the text of pdf documents at folder_path to extract out the page based content
    Returns the stored text to pass into chunking function
    '''

    all_pdfs = []
    for file in folder_path.iterdir():

        pdf_document = {
                        'name' : "",
                        'content' : []
                    }

        if file.is_file() and file.suffix.lower() == '.pdf':
            print(f'Processing {file.name}\n')
            with pymupdf.open(file) as doc:
            
                pdf_document['name'] = file.stem

                for page_no, page in enumerate(doc):
                    if page.get_text().strip():
                        clean_page_text = clean_pdf_text(page.get_text())
                        content = {
                            'page_number' : page_no,
                            'text' : clean_page_text
                        }

                        pdf_document['content'].append(content)

            all_pdfs.append(pdf_document)

    return all_pdfs





# if __name__ == '__main__':
dotenv.load_dotenv()
raw_dataset_path = Path(os.getenv('raw_dataset_path'))
dataset_process_path = Path(os.getenv('dataset_process_path'))

# iterate_folder(raw_dataset_path,dataset_process_path)

documents = process_doc_text(raw_dataset_path)
# print(documents[1].keys())
# print(documents[0].get('content')[0])
