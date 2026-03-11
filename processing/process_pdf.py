import pymupdf
import os
import dotenv
import unicodedata
from pathlib import Path
import json



# from download_data import delete_all_files_in_folder 
# not too sure how to do this but temporarily i will treat this as a fix

def delete_all_files_in_folder(folder_path):
    '''
    Deletes all files in the folder specified by folder_path
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path entered is correct")

    count = 0
    for file in folder_path.iterdir():
        if file.is_file():
            filename = file.name
            print(f'Removing {filename}...\n')
            file.unlink()
            count += 1

    print(f'{count} files removed.\n\n')




def clean_pdf_text(text):

    '''
    Cleans a pdf page's text content
    '''

    text = unicodedata.normalize('NFKC',text)

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u2028", "\n").replace("\u2029", "\n")

    ''.join(char for char in text if char.isprintable() or char in '\n')

    return text



def iterate_folder(folder_path, output_path):
    '''
    Iterates through a folder and passes any pdf files with the output_path to another function to 
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
    the output into output_path
    '''

    filename = file.stem
    output_path = output_path / filename

    if output_path.exists():
        print(f'Files already exist at {output_path}, deleting them for overwriting')
        delete_all_files_in_folder(output_path)
    else:
        print(f'No folder found at {output_path}, creating a directory there')
        Path.mkdir(output_path)

    with pymupdf.open(file) as doc:
        print(f'Processing {filename}, saving pages to {output_path}')
        doc_metadata = doc.metadata.copy()  # pylint: disable=no-member

        for i, page in enumerate(doc):
            page_text = page.get_text()

            if page_text.strip():   #skips empty page (q smart eh)
                clean_text = clean_pdf_text(page_text)
                page_json = {
                    'page_content' : clean_text,
                    'metadata' : {
                        'page_number' : i+1,
                        'pdf_name' : filename,
                        'document_metadata' : doc_metadata
                        }
                }

                save_file_name = f'{filename}_page_{i+1}.json'
                save_path = output_path / save_file_name
                with open(save_path,'w',encoding='utf-8') as save:
                    json.dump(page_json, save, ensure_ascii=False, indent=4)
                print(f'Saved page {i+1}')






# def read_doc_text(doc_path, output_path):

#     '''
#     Reads a pdf for its textual content only
#     '''

#     with pymupdf.open(doc_path) as doc:
#         doc_title = doc.metadata.get('title', f'No title found {datetime.datetime.now()}')

#         write_path = os.path.join(output_path, f'{doc_title}.txt')
#         with open(write_path, 'w', encoding='utf-8') as f:
#             for page in doc:
#                 text = page.get_text()
#                 clean_text = clean_pdf_text(text)
#                 f.write(clean_text)

#     print(f'{doc_title} read and output saved.\n')



# if __name__ == '__main__':
dotenv.load_dotenv()
dataset_download_path = Path(os.getenv('dataset_download_path'))
dataset_process_path = Path(os.getenv('dataset_process_path'))
iterate_folder(dataset_download_path,dataset_process_path)