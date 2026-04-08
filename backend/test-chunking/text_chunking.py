from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import traceback

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


def get_page_numbers(chunks,starting_page_no=1,page_pattern = r"\s*--- end of page\.page_number=(\d+) ---\s*"):

    final_text_chunks = []
    
    current_page_number = starting_page_no
    
    for chunk in chunks:
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
 
        final_text_chunks.append(pages)

    return final_text_chunks


async def get_text_chunks(file):
    
    model = OpenAIModel()

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True)

    cleaned_markdown_text = clean_text(markdown_text)
    filename = str(file.stem) + '.md'
    save_to_file(filename,cleaned_markdown_text)

    semantic_texts = model.semantic_chunker(cleaned_markdown_text)

    # print(f'\n\nFile : {file.name}\n\n')
    # for text in semantic_texts:
    #     print(f'Semantic chunk : {text}\n\n')
    
    max_chunk_size = 1000
    chunk_overlap = 200
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=max_chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["#","##","###","\n\n", "\n", "."]
    )

    final_texts = []
    page_pattern = r"\s*--- end of page\.page_number=(\d+) ---\s*"
    for text in semantic_texts:
        if len(re.sub(page_pattern, '', text).strip()) > 0:
            if len(text) > max_chunk_size:
                split_texts = splitter.split_text(text)
                final_texts.extend(split_texts)
            else:
                final_texts.append(text)
    
    tasks = [model.get_context(cleaned_markdown_text,text) for text in final_texts]
    texts_context = await asyncio.gather(*tasks)

    texts_page_numbers = get_page_numbers(final_texts)

    final_chunks = []
    for i,text in enumerate(final_texts):
        container = TextChunk()
        container.document_name = file.name
        container.context = texts_context[i]
        container.content['text'] = text
        container.metadata['pages'] = list(texts_page_numbers[i])
        final_chunks.append(container)

    return final_chunks


async def embed_text_chunks(chunks):

    model = OpenAIModel()

    texts = []

    for chunk in chunks:

        text = f"""
        Context : {chunk.context}
        Content : {chunk.content['text']}
        """

        texts.append(text)

    vectors = await model.embed_texts(texts)

    embeddings = format_embeddings(chunks,vectors)

    return embeddings


async def ingest_all_pdfs(folder_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                print(f'Processing {file.name}\n\n')

                with pymupdf.open(file) as doc:
                    metadata = doc.metadata.copy()

                print(f'\tFinished inserting pdf\n\n')

                insert_pdf(file,metadata)

                chunks = await get_text_chunks(file)

                print(f'\tFinished getting text chunks\n\n')

                returned_chunks = save_document_chunks(file.name,chunks)

                print(f'\tFinished saving chunks into postgresdb\n\n')

                embeddings = await embed_text_chunks(returned_chunks)

                print(f'\tFinished getting chunk embeddings\n\n')

                upload_to_qdrant(embeddings)

                print(f'\tFinished uploading chunk embeddings to qdrant\n\n')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        traceback.print_exc() 
        raise




if __name__ == '__main__':
    print(f'Ingestion running\n\n\n')
    text_pdfs = Path(os.getenv('text_pdfs_path'))
    asyncio.run(ingest_all_pdfs(text_pdfs))


"""
whole pipeline for text is in this file, like all things like id, filename, etc etc 
is handled here by iterating through the folder of pdfs
"""

