from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
from langchain_text_splitters import RecursiveCharacterTextSplitter

from llms_and_models import OpenAIModel
from chunks import TextChunk,ImageChunk
from postgres import save_document_chunks, insert_pdf
from qdrant import upload_to_qdrant, format_embeddings

dotenv.load_dotenv()

def save_to_file(filename,content,filepath=os.getenv('markdown_texts_path')):

    save_path = Path(filepath) / filename
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open('w', encoding='utf-8') as f:
        f.writelines(content)


def clean_text(text):

    image_chunk_identifier = r"\*\*==> picture \[\d* x \d*\] intentionally omitted <==\*\*"
    picture_text_pattern = r"\*\*----- (Start|End) of picture text -----\*\*"
    break_patterns = r"\<br\>+"
    cleaned_text = re.sub(image_chunk_identifier, '', text)
    cleaned_text = re.sub(picture_text_pattern, '', cleaned_text)
    cleaned_text = re.sub(break_patterns, '', cleaned_text)
    cleaned_text = re.sub(r'\n{3,}','\n\n',cleaned_text)

    return cleaned_text.strip()


def get_text_chunks(file):
    
    model = OpenAIModel()

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True)

    cleaned_markdown_text = clean_text(markdown_text)
    filename = str(file.stem) + '.md'
    save_to_file(filename,cleaned_markdown_text)

    semantic_chunks = model.semantic_chunker(cleaned_markdown_text)
    
    max_chunk_size = 1000
    chunk_overlap = 200
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=max_chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ".", " "]
    )
    cleaned_chunks = []
    for chunk in semantic_chunks:
        if len(chunk) > max_chunk_size:
            smaller_chunks = splitter.split_text(chunk)
            cleaned_chunks.extend(smaller_chunks)
        else:
            cleaned_chunks.append(chunk)
    
    final_text_chunks = []
    current_page_number = 1
    for chunk in cleaned_chunks:
        chunk = chunk.strip()
        page_pattern = r"\s*--- end of page\.page_number=(\d+) ---\s*"
        content = re.sub(page_pattern,'\n\n',chunk).strip()
        if len(content) != 0:
            text_chunk = TextChunk()
            chunk_context = model.get_context(cleaned_markdown_text, chunk)
            pages = set()
            for match in re.finditer(page_pattern, chunk):
                page = int(match.group(1))
                current_page_number = page + 1
                if match.start() != 0:
                    pages.add(page)
                if match.end() != len(chunk):
                    pages.add(page+1)
            if not pages:
                pages = [current_page_number]

            text_chunk.document_name = file.name
            text_chunk.context = chunk_context
            text_chunk.content['text'] = content
            text_chunk.metadata['pages'] = list(pages)

            final_text_chunks.append(text_chunk)

    return final_text_chunks


def embed_text_chunks(chunks):

    model = OpenAIModel()

    texts = []

    for chunk in chunks:

        text = f"""
        Context : {chunk.context}
        Content : {chunk.content['text']}
        """

        texts.append(text)

    vectors = model.embed_texts(texts)

    embeddings = format_embeddings(chunks,vectors)

    return embeddings


def ingest_all_pdfs(folder_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                print(f'Inserting {file.name} into pgsql\n\n')

                with pymupdf.open(file) as doc:
                    metadata = doc.metadata.copy()

                print(f'Finished inserting {file.name}\n\n')

                insert_pdf(file,metadata)

                print(f'Processing {file.name}\n\n')

                chunks = get_text_chunks(file)
                save_document_chunks(file.name,chunks)
                embeddings = embed_text_chunks(chunks)
                upload_to_qdrant(embeddings)

                print(f'Finished processing {file.name}\n\n')

            print(f'Finished processing all files\n\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs from {folder_path.name}, error {e}')




if __name__ == '__main__':
    print(f'Ingestion running\n\n\n')
    text_pdfs = Path(os.getenv('text_pdfs_path'))
    ingest_all_pdfs(text_pdfs)


"""
whole pipeline for text is in this file, like all things like id, filename, etc etc 
is handled here by iterating through the folder of pdfs
"""

