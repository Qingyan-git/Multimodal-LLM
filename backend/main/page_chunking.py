from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
from langchain_community.callbacks.manager import get_openai_callback

from llms_and_models import OpenAIModel
from chunks import PageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant
from text_chunking import save_to_file
from ..main.vidore.vidore_eval import convert_PIL_to_pdf



async def extract_pages(filepath):

    model = OpenAIModel()
    all_chunks = []

    with pymupdf.open(filepath) as doc:

        full_text = pymupdf4llm.to_markdown(doc,header=True,footer=True)

        for page_no,page in enumerate(doc):

            pix = page.get_pixmap() 
            image_bytes = pix.tobytes(output='jpg', jpg_quality=95)
            image_description = await model.get_image_description(image_bytes)
            context = await model.get_context(full_text,image_description)

            page_chunk = PageChunk()
            page_chunk.document_name = filepath.name
            page_chunk.context = context
            page_chunk.content = image_description
            page_chunk.metadata = {'pages' : page_no}
            all_chunks.append(page_chunk)

    return all_chunks




async def process_pages(folder_path):
    try:

        if folder_path.is_dir():

            model = OpenAIModel()

            for file in folder_path.iterdir():

                insert_pdfs(file)

                print(f'Finished inserting pdf to postgresdb\n\n')

                with get_openai_callback() as cb:

                    chunks = await extract_pages(file)

                    print(f'\tFinished getting text chunks\n\n')

                    token_cost = f"Token cost to PAGE chunk {file.name} : {cb.total_tokens}"
                    money_cost = f"Money cost to PAGE chunk {file.name} : {cb.total_cost}"
                    total_cost = [token_cost,money_cost]

                    save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                if chunks:

                    returned_chunks = save_document_chunks(file.name,chunks,type='page')

                    print(f'\tFinished saving chunks into postgresdb\n\n')

                    embeddings,cost = await model.embed_texts(returned_chunks)

                    print(f'\tFinished getting embeddings\n\n')

                    token_cost = f"Token cost to EMBED TEXT {file.name} : {cost[0]}"
                    money_cost = f"Money cost to EMBED TEXT {file.name} : {cost[1]}"
                    total_cost = [token_cost,money_cost]

                    save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    upload_to_qdrant(embeddings)

                    print(f'\tFinished uploading embeddings to qdrant\n\n')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise



if __name__ == '__main__':
    print(f'Ingestion running\n\n\n')
    text_pdfs_path = Path(os.getenv('all_pdfs_paths'))
    text_results_path = Path(os.getenv('text_results_path'))
    
    asyncio.run(process_pages(text_pdfs_path))

