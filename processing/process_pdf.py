import pymupdf
import os
import datetime
import dotenv
import re
import unicodedata

def clean_pdf_text(text):

    '''
    Cleans a pdf page's text content
    '''

    text = unicodedata.normalize('NFKC',text)

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u2028", "\n").replace("\u2029", "\n")

    ''.join(char for char in text if char.isprintable() or char in '\n')

    return text

def read_doc_text(doc_path, output_path):

    '''
    Reads a pdf for its textual content only
    '''

    with pymupdf.open(doc_path) as doc:
        doc_title = doc.metadata.get('title', f'No title found {datetime.datetime.now()}')

        write_path = os.path.join(output_path, f'{doc_title}.txt')
        with open(write_path, 'w', encoding='utf-8') as f:
            for page in doc:
                text = page.get_text()
                clean_text = clean_pdf_text(text)
                f.write(clean_text)

    print(f'{doc_title} read and output saved.\n')



# if __name__ == '__main__':
dotenv.load_dotenv()
test_file_path = os.getenv('test_file_path')
test_file_output = os.getenv('test_file_output')

read_doc_text(test_file_path,test_file_output)
