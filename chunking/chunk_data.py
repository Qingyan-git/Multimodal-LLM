from pathlib import Path
import os
import dotenv
import json


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
        if file.is_file and file.suffix.lower() == '.json':
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



def chunk_text(full_text,page_breaks,metadata):
    pass















#if __name__ == '__main__':
dotenv.load_dotenv()
chunking_data_download = Path(os.getenv('chunking_data_download'))

doc_data = retrieve_data(chunking_data_download)
print(*doc_data)

# print(len("\n\nGovernment Data Security Policies  |   !1\nGOVERNMENT  \nDATA SECURITY \nPOLICIES\nThis document contains general information for the \npublic only. It is not intended to be relied upon as a \ncomprehensive or definitive guide on each agency’s \npolicies and practices. The data security measures \nimplemented by each agency will differ depending on \nvarious factors such as the sensitivity of the data and \nthe agency’s assessment of data security risks. The \nGovernment may update the policies set out in this \ndocument without publishing such updates to the \npublic.  \n"))



