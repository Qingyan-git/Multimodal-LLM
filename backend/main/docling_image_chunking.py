
from docling.datamodel.document import DocItemLabel, TextItem
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, PictureDescriptionBaseOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from langchain_community.callbacks.manager import get_openai_callback

import asyncio
from pathlib import Path
import os
import dotenv
import pandas as pd
import numpy as np
import io
import re
import unicodedata

from text_chunking import save_to_file,delete_all_files_in_folder
from llms_and_models import OpenAIModel, SparseEmbedder, ColBERTEmbedder
from chunks import ImageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant



def clean_image_text(text: str) -> str:
    if not text:
        return ""

    # 1. Remove NUL bytes for Postgres
    text = text.replace('\x00', '')

    # 2. Normalize Unicode (Fixes ligatures like 'ﬁ' -> 'fi')
    # NFKC decomposes combined characters and replaces them with their standard equivalents
    text = unicodedata.normalize('NFKC', text)

    # 3. Replace various whitespace (non-breaking spaces, tabs, etc.) with standard space
    text = re.sub(r'\s+', ' ', text)

    # 4. Strip control characters (except for newlines and tabs if you want them)
    # This removes things like "Bell", "Escape", or "Backspace" characters
    text = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C" or ch in "\n\r\t")

    return text.strip()


def get_surrounding_text(document,item):

    item_ref = item.get_ref()
    page_no = item.prov[0].page_no

    page_items = list(document.iterate_items(page_no=page_no))

    prev_text = ""
    post_text = ""

    for i, (curr_item,level) in enumerate(page_items):
        if curr_item.get_ref() == item_ref:
            item_idx = i
            break

    if item_idx > 0:
        for prev_idx in range(i-1,-1,-1):
            candidate_item = page_items[prev_idx][0]
            if isinstance(candidate_item,TextItem): #and candidate_item.label == DocItemLabel.PARAGRAPH:
                prev_text = candidate_item.text
                break

    if item_idx < len(page_items)-1:
        for post_idx in range(i+1,len(page_items),1):
            candidate_item = page_items[post_idx][0]
            if isinstance(candidate_item,TextItem): #and candidate_item.label == DocItemLabel.PARAGRAPH:
                post_text = candidate_item.text
                break

    prev_text = clean_image_text(prev_text)
    post_text = clean_image_text(post_text)

    return (prev_text,post_text)


def save_image(image,filename,path=Path(os.getenv('image_results_path'))):
    
    path.mkdir(parents=True, exist_ok=True)
    save_path = path / filename
    image.save(save_path)


def is_useable_image(img, page_w, page_h, min_dim=100, area_threshold=0.15):
    prov = img.prov[0]
    bbox = prov.bbox
    
    # 1. Basic Dimension Check
    if bbox.height == 0 or bbox.width == 0:
        return False
    
    if bbox.height < min_dim or bbox.width < min_dim:
        return False

    # 2. Aspect Ratio Check (Your 0.2 to 5.0 range)
    ratio = bbox.height / bbox.width 
    if ratio > 5 or ratio < 0.2:
        return False
        
    # 3. Normalized Area Check
    image_area = bbox.width * bbox.height
    page_area = page_w * page_h
    normalized_area = image_area / page_area
    
    if normalized_area < area_threshold:
        return False
        
    return True


async def extract_images(filepath):

    model = OpenAIModel()

    # Keep page/element images so they can be exported. The `images_scale` controls
    # the rendered image resolution (scale=1 ~ 72 DPI). The `generate_*` toggles
    # decide which elements are enriched with images.
    pipeline_options = PdfPipelineOptions()
    pipeline_options.images_scale = 1.0 
    pipeline_options.generate_page_images = True
    pipeline_options.generate_picture_images = True
    pipeline_options.picture_description_options.picture_area_threshold = 0.15

    docling = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    #Object that holds the document
    document = docling.convert(filepath).document

    #All document images
    document_images = document.pictures

    all_chunks = []
    batch_size = 5

    useable_images = []
    for img in document_images:
        # 1. Get the page index from the image provenance
        page_no = img.prov[0].page_no 
        
        # 2. Access the specific page item to get its dimensions
        # Docling pages are 1-indexed in provenance but 0-indexed in the list
        # Check if your version uses document.pages[page_no - 1] or [page_no]
        page_item = document.pages[page_no] 
        
        page_w = page_item.size.width
        page_h = page_item.size.height

        # 3. Now pass these dynamic dimensions into your function
        if is_useable_image(img, page_w, page_h):
            useable_images.append(img)

    for batch_no in range(0,len(useable_images),batch_size):
        batch = useable_images[batch_no:batch_no+batch_size]
        batch_tasks = []

        for i,image in enumerate(batch):
            filename = f"{filepath.stem}_{batch_no}_{i}.jpg"
            image_PIL = image.get_image(doc=document)
            save_image(image_PIL,filename)

            task = asyncio.create_task(
                process_single_image(model,document,image)
            )
            batch_tasks.append(task)

        image_chunks = await asyncio.gather(*batch_tasks)
        all_chunks.extend(image_chunks)

    return all_chunks


async def process_single_image(model,document,image):
    image_PIL = image.get_image(doc=document)
    image_caption = image.caption_text(doc=document)

    #Get the surrounding 1 paragraph of text surrounding the image
    prev_text,post_text = get_surrounding_text(document,image)
    
    #Gets text representation of image
    buffer = io.BytesIO()
    image_PIL.save(buffer, format="JPEG")
    image_bytes = buffer.getvalue()
    image_summary = await model.get_image_description(image_bytes)

    #Build the text data that is the "image chunk"
    chunk_text = f"""
    Text before image : {prev_text if prev_text.strip() else "No text before image"}
    Image description : {image_summary if image_summary.strip() else "No image description"}
    Image caption : {image_caption if image_caption.strip() else "No image caption"}
    Text after image : {post_text if post_text.strip() else "No text after image"}
    """

    #Gets image context using chunk_text
    image_context = await model.get_context(document.export_to_markdown(), chunk_text)

    #Some image metadata
    metadata = {
        'source': image.source,
        'pages' : list(image.prov[0].page_no),
    }

    #All into image_chunk object and return
    image_chunk = ImageChunk()
    image_chunk.document_name = document.origin.filename
    image_chunk.context = image_context
    image_chunk.content = chunk_text
    image_chunk.metadata = metadata

    return image_chunk



async def process_images(folder_path):
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

                        chunks = await extract_images(file)

                        print(f'\tFinished getting text chunks\n\n')

                        token_cost = f"Token cost to TEXT chunk {file.name} : {cb.total_tokens}"
                        money_cost = f"Money cost to TEXT chunk {file.name} : {cb.total_cost}"
                        total_cost = [token_cost,money_cost]

                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    if chunks:

                        returned_chunks = save_document_chunks(file.name,chunks,type='image')

                        print(f'\tFinished saving chunks into postgresdb\n\n')

                        dense_embeddings,cost = await model.embed_texts(returned_chunks)
                        token_cost = f"Token cost to EMBED TEXT {file.name} : {cost[0]}"
                        money_cost = f"Money cost to EMBED TEXT {file.name} : {cost[1]}"
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



if __name__ == "__main__":

    print(f'Ingestion running\n\n\n')
    image_pdfs_path = Path(os.getenv('all_pdfs_path'))
    image_results_path = Path(os.getenv('image_results_path'))

    delete_all_files_in_folder(image_results_path)

    asyncio.run(process_images(image_pdfs_path))


