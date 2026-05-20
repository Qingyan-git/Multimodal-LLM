from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
from langchain_community.callbacks.manager import get_openai_callback

from llms_and_models import OpenAIModel, RecursiveSplitter, SparseEmbedder, ColBERTEmbedder
from chunks import TextChunk,ImageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant


dotenv.load_dotenv()



def save_to_file(filename,content,filepath=os.getenv('text_results_path'),method='w'):

    save_path = Path(filepath) / filename
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open(method, encoding='utf-8') as f:
        f.write('\n')
        if isinstance(content, list):
            for item in content:
                f.write(f"{item}\n\n")
        else:
            f.write(content)
        f.write('\n')


def delete_all_files_in_folder(folder_path):

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


# def clean_text(text):

#     image_chunk_identifier = r"\*\*==> picture \[\d* x \d*\] intentionally omitted <==\*\*"
#     picture_text_pattern = r"\*\*----- (Start|End) of picture text -----\*\*"
#     break_patterns = r"\<br\>+"
#     cleaned_text = re.sub(image_chunk_identifier, '', text)
#     cleaned_text = re.sub(picture_text_pattern, '', cleaned_text)
#     cleaned_text = re.sub(break_patterns, '', cleaned_text)
#     cleaned_text = re.sub(r'\n{3,}','\n\n',cleaned_text)

#     return cleaned_text.strip()


def get_page_numbers(chunks, starting_page_no=1, page_pattern=r"\s*--- end of page\.page_number=(\d+) ---\s*"):
    final_text_chunks = []
    
    # Track the background page state as a strict 1-indexed integer
    current_page_number = starting_page_no
    
    for chunk in chunks:
        pages = set()
        for match in re.finditer(page_pattern, chunk):
            # The regex group matches exactly what is printed (which is 1-indexed)
            found_page_no = int(match.group(1))
            # Update our tracker state
            current_page_number = found_page_no
            
            # Boundary check for split chunks
            if match.start() != 0:
                # Text before the marker belongs to the page that is ending
                pages.add(found_page_no)
            if match.end() != len(chunk):
                # Text after the marker belongs to the next page
                pages.add(found_page_no + 1)
        
        if not pages:
            # Fallback for chunks that do not contain a marker tag
            pages.add(current_page_number)
 
        final_text_chunks.append(sorted(list(pages)))

    return final_text_chunks


async def extract_text(file):
    
    semaphore = asyncio.Semaphore(5)
    model = OpenAIModel()
    recursive_splitter = RecursiveSplitter()
    max_chunk_size = recursive_splitter.get_max_chunk_size()

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=True, footer=True, page_separators=True)

    # cleaned_markdown_text = clean_text(markdown_text)
    filename = str(file.stem) + '.md'
    save_to_file(filename,markdown_text)

    semantic_texts = model.semantic_chunker(markdown_text)
    
    # final_texts = []
    # for text in semantic_texts:
    #     if len(text) > max_chunk_size:
    #         split_texts = recursive_splitter.recursive_split(text)
    #         final_texts.extend(split_texts)
    #     else:
    #         final_texts.append(text)
    # final_texts.extend(semantic_texts)

    # tasks = [model.get_context(markdown_text,text) for text in semantic_texts]

    async def sem_task(markdown_text,text):
        async with semaphore:
            return await model.get_context(markdown_text,text)

    tasks = [sem_task(markdown_text,text) for text in semantic_texts]
    texts_context = await asyncio.gather(*tasks)
    texts_page_numbers = get_page_numbers(semantic_texts)

    final_chunks = []
    for i,text in enumerate(semantic_texts):

        save_text = f"Final chunk number {i}, content : \n{text}\n"
        save_to_file(filename,save_text,method='a')

        container = TextChunk()
        container.document_name = file.name
        container.context = texts_context[i]
        container.content = text
        container.metadata['pages'] = list(texts_page_numbers[i])
        final_chunks.append(container)

    return final_chunks


async def process_text(folder_path):
    try:

        model = OpenAIModel()
        sparse_embedder = SparseEmbedder()
        late_embedder = ColBERTEmbedder()

        if folder_path.is_dir():

            for file in folder_path.iterdir():

                if file.is_file():

                    print(f'Processing {file.stem}\n\n')

                    insert_pdfs(file)

                    print(f'\tFinished inserting pdf to postgresdb\n\n')

                    with get_openai_callback() as cb:

                        chunks = await extract_text(file)

                        print(f'\tFinished getting text chunks\n\n')

                        token_cost = f"Token cost to TEXT chunk {file.name} : {cb.total_tokens}"
                        money_cost = f"Money cost to TEXT chunk {file.name} : {cb.total_cost}"
                        total_cost = [token_cost,money_cost]

                        save_to_file(filename=f'{file.stem}.txt',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    if chunks:

                        returned_chunks = save_document_chunks(file.name,chunks,type='text')

                        print(f'\tFinished saving chunks into postgresdb\n\n')

                        dense_embeddings,cost = await model.embed_texts(returned_chunks)
                        token_cost = f"Token cost to EMBED TEXT {file.name} : {cost[0]}"
                        money_cost = f"Money cost to EMBED TEXT {file.name} : {cost[1]}"
                        total_cost = [token_cost,money_cost]
                        save_to_file(filename=f'{file.stem}.txt',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                        sparse_embeddings = await asyncio.to_thread(sparse_embedder.embed_texts, returned_chunks)
                        late_embeddings = await asyncio.to_thread(late_embedder.embed_texts, returned_chunks)

                        print(f'\tFinished getting embeddings\n\n')

                        upload_to_qdrant(returned_chunks,dense_embeddings,sparse_embeddings,late_embeddings)

                        print(f'\tFinished uploading embeddings to qdrant\n\n')

                    print(f'Finished processing {file.stem}\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise




if __name__ == '__main__':
    print(f'Ingestion running\n\n\n')
    text_pdfs_path = Path(os.getenv('all_pdfs_path'))
    text_results_path = Path(os.getenv('text_results_path'))

    delete_all_files_in_folder(text_results_path)
    
    asyncio.run(process_text(text_pdfs_path))


"""
whole pipeline for text is in this file, like all things like id, filename, etc etc 
is handled here by iterating through the folder of pdfs
"""

