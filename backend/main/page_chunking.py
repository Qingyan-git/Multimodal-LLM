from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
from langchain_community.callbacks.manager import get_openai_callback

from llms_and_models import OpenAIModel, SparseEmbedder, ColBERTEmbedder
from chunks import PageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant
from text_chunking import save_to_file




def convert_PIL_to_pdf(pil_image):
    """
    Converts a PIL object into a PyMuPDF Document object.
    """
    # 1. Create a new empty PDF
    doc = pymupdf.open()

    # 2. Convert PIL Image to bytes in memory (JPEG is usually fastest)
    img_byte_arr = BytesIO()
    pil_image.save(img_byte_arr, format='JPEG')
    img_bytes = img_byte_arr.getvalue()

    # 3. Create a page matching the image dimensions
    # PIL uses (width, height)
    page = doc.new_page(width=pil_image.width, height=pil_image.height)

    # 4. Insert the image to fill the page
    # (0, 0, width, height) defines the target rectangle
    page.insert_image(page.rect, stream=img_bytes)

    return doc


async def extract_pages(filepath):

    model = OpenAIModel()
    all_chunks = []

    with pymupdf.open(filepath) as doc:
        full_text = pymupdf4llm.to_markdown(doc, header=True, footer=True)
        
        # Define a helper to process a single page
        async def process_page(page_no, page):
            pix = page.get_pixmap() 
            image_bytes = pix.tobytes(output='jpg', jpg_quality=95)

            image_description = await model.get_image_description(image_bytes)
            context = await model.get_context(full_text, image_description)

            page_chunk = PageChunk()
            page_chunk.document_name = filepath.name
            page_chunk.context = context
            page_chunk.content = image_description
            page_chunk.metadata = {'pages': list(page_no)}

            return page_chunk

        # Create tasks for all pages and run them in parallel
        tasks = [process_page(i, page) for i, page in enumerate(doc)]
        all_chunks = await asyncio.gather(*tasks)

    return all_chunks


async def process_pages(folder_path):
    try:

        model = OpenAIModel()
        sparse_embedder = SparseEmbedder()
        late_embedder = ColBERTEmbedder()

        if folder_path.is_dir():

            for file in folder_path.iterdir():

                if file.is_file():

                    insert_pdfs(file)

                    print(f'Finished inserting pdf to postgresdb\n\n')

                    with get_openai_callback() as cb:

                        chunks = await extract_pages(file)

                        print(f'\tFinished getting page chunks\n\n')

                        token_cost = f"Token cost to PAGE chunk {file.name} : {cb.total_tokens}"
                        money_cost = f"Money cost to PAGE chunk {file.name} : {cb.total_cost}"
                        total_cost = [token_cost,money_cost]

                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    if chunks:

                        returned_chunks = save_document_chunks(file.name,chunks,type='page')

                        print(f'\tFinished saving chunks into postgresdb\n\n')

                        dense_embeddings,cost = await model.embed_texts(returned_chunks)
                        token_cost = f"Token cost to EMBED PAGE {file.name} : {cost[0]}"
                        money_cost = f"Money cost to EMBED PAGE {file.name} : {cost[1]}"
                        total_cost = [token_cost,money_cost]
                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                        sparse_embeddings = await asyncio.to_thread(sparse_embedder.embed_texts, returned_chunks)
                        late_embeddings = await asyncio.to_thread(late_embedder.embed_texts, returned_chunks)

                        print(f'\tFinished getting embeddings\n\n')

                        upload_to_qdrant(returned_chunks,dense_embeddings,sparse_embeddings,late_embeddings)

                        print(f'\tFinished uploading embeddings to qdrant\n\n')

                    print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise



if __name__ == '__main__':
    print(f'Ingestion running\n\n\n')
    text_pdfs_path = Path(os.getenv('all_pdfs_path'))
    text_results_path = Path(os.getenv('text_results_path'))
    
    asyncio.run(process_pages(text_pdfs_path))

