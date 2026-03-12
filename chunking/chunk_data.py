from pathlib import Path
import os
import dotenv
import json
import spacy


def group_by_sentence_chunking(folder_path,chunk_size=8,overlap=2):
    '''
    Uses folder path to retrieve the data and returns chunks of groups of sentences ready for embedding
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path input is correct")

    print(f'Retriving data from folder')

    spacy_model = spacy.load("en_core_web_sm")
    

    for file in folder_path.iterdir():
        if file.is_file() and file.suffix.lower() == '.json':

            document_sentences = []

            with open(file,'r',encoding='utf-8') as document:
                doc = json.load(document)
                doc_metadata = doc['metadata']
                doc_pages = doc['pages']

                for page in doc_pages:
                    page_number = page['page_number']
                    page_content = page['page_content']
                    cleaned_contet = page_content.replace('\n',"")
                    page_sentences = spacy_model(cleaned_contet).sents
                    for sentence in page_sentences:
                        document_sentences.append({
                            'sentence' : sentence.text,
                            'page_number' : page_number,
                        })

            pointer = 0
            chunks = []

            while pointer < len(document_sentences):
                chunk = {
                    'content' : "",
                    'pages' : [],
                    'metadata' : doc_metadata
                }

                end_pointer = min((pointer+chunk_size),len(document_sentences))
                chunk_sentences = document_sentences[pointer:end_pointer]

                chunk['content'] = " ".join(s['sentence'] for s in chunk_sentences)
                chunk['pages'] = list(range(chunk_sentences[0]['page_number'], chunk_sentences[-1]['page_number']+1))
                chunk['metadata'] = doc_metadata

                chunks.append(chunk)
                pointer += chunk_size - overlap

            return chunks

            

        else:
            raise FileNotFoundError("File in folder not of correct type")






#if __name__ == '__main__':
dotenv.load_dotenv()
chunking_data_download = Path(os.getenv('chunking_data_download'))

doc_data = group_by_sentence_chunking(chunking_data_download)
print(doc_data[0])


