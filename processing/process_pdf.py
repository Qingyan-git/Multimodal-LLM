import pymupdf
import os
import datetime
import dotenv
import re
import unicodedata

def clean_pdf_text(page):

    '''
    Cleans a pdf page's text content
    '''

    def unicode_normalization(page):
        return unicodedata.normalize('NFKC',page)

    def remove_unprintable_chars(page):
        return ''.join(char for char in page if char.isprintable())

    page = unicode_normalization(page)
    page = remove_unprintable_chars(page)

    return page

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
                cleaned_text = clean_pdf_text(text)
                f.write(cleaned_text)

    print(f'{doc_title} read and output saved.\n')



# if __name__ == '__main__':
dotenv.load_dotenv()
test_file_path = os.getenv('test_file_path')
test_file_output = os.getenv('test_file_output')

read_doc_text(test_file_path,test_file_output)
