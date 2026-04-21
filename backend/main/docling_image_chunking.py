
from docling.datamodel.document import DocItemLabel, TextItem
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from langchain_community.callbacks.manager import get_openai_callback

import asyncio
from pathlib import Path
import os
import dotenv
import pandas as pd
import numpy as np
import io

from text_chunking import save_to_file,delete_all_files_in_folder
from llms_and_models import OpenAIModel
from chunks import ImageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant




def get_surrounding_text(document,item):

    item_ref = item.get_ref()
    page_no = item.prov[0].page_no

    page_items = list(document.iterate_items(page_no=page_no))

    prev_text = ""
    post_text = ""

    for i, (item,level) in enumerate(page_items):
        if item.get_ref() == item_ref:
            if i > 0:
                for prev_idx in range(i-1,-1,-1):
                    candidate_item = page_items[prev_idx][0]
                    if isinstance(candidate_item,TextItem) and candidate_item.label == DocItemLabel.PARAGRAPH:
                        prev_text = candidate_item.text
                        break

            if i < len(page_items)-1:
                for post_idx in range(i+1,len(page_items),1):
                    candidate_item = page_items[post_idx][0]
                    if isinstance(candidate_item,TextItem) and candidate_item.label == DocItemLabel.PARAGRAPH:
                        post_text = candidate_item.text
                        break

    return (prev_text,post_text)










def save_image(image,filename,path=Path(os.getenv('image_results_path'))):
    
    path.mkdir(parents=True, exist_ok=True)
    save_path = path / filename
    image.save(save_path)


def useable_image(image, min_dim=50):
    bbox = image.prov[0].bbox
    height = bbox.height
    width = bbox.width

    if height == 0 or width == 0:
        return False
    
    if height < min_dim or width < min_dim:
        return False

    ratio = height / width 
    if ratio > 5 or ratio < 0.2:
        return False
        
    return True


async def extract_images(filepath):

    model = OpenAIModel()

    # Keep page/element images so they can be exported. The `images_scale` controls
    # the rendered image resolution (scale=1 ~ 72 DPI). The `generate_*` toggles
    # decide which elements are enriched with images.
    pipeline_options = PdfPipelineOptions()
    pipeline_options.images_scale = 2.0 
    pipeline_options.generate_page_images = True
    pipeline_options.generate_picture_images = True

    docling = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    #Object that holds the document
    document = docling.convert(filepath).document

    #All document images
    document_images = document.pictures

    tasks = []
    for i,image in enumerate(document_images):

        if useable_image(image):

            filename = f"{i}.jpg"
            image_PIL = image.get_image(doc=document)
            save_image(image_PIL,filename)

            task = asyncio.create_task(
                process_single_image(model,document,image)
            )
            tasks.append(task)

    image_chunks = await asyncio.gather(*tasks)

    return image_chunks


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
    Text before image : {prev_text}
    Image description : {image_summary}
    Image caption : {image_caption}
    Text after image : {post_text}
    """

    #Gets image context using chunk_text
    image_context = await model.get_context(document.export_to_markdown(), chunk_text)

    #Some image metadata
    metadata = {
        'source': image.source,
        'pages' : image.prov[0].page_no,
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
        if folder_path.is_dir():

            model = OpenAIModel()

            insert_pdfs(folder_path)

            print(f'Finished inserting pdfs to postgresdb\n\n')

            for file in folder_path.iterdir():

                with get_openai_callback() as cb:

                    image_chunks = await extract_images(file)

                    print(f'\tFinished getting image chunks\n\n')

                    token_cost = f"Token cost to IMAGE chunk {file.name} : {cb.total_tokens}"
                    money_cost = f"Money cost to IMAGE chunk {file.name} : {cb.total_cost}"
                    total_cost = [token_cost,money_cost]

                    save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                if image_chunks:

                    returned_chunks = save_document_chunks(file.name,image_chunks,type='image')

                    print(f'\tFinished saving chunks into postgresdb\n\n')

                    embeddings,cost = await model.embed_texts(returned_chunks)

                    print(f'\tFinished getting embeddings\n\n')

                    token_cost = f"Token cost to EMBED TEXT {file.name} : {cost[0]}"
                    money_cost = f"Money cost to EMBED TEXT {file.name} : {cost[1]}"
                    total_cost = [token_cost,money_cost]

                    save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    upload_to_qdrant(embeddings)

                    print(f'\tFinished uploading embeddings to qdrant\n\n')

                else:

                    print(f'\tNo chunks found')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise



if __name__ == "__main__":

    print(f'Ingestion running\n\n\n')
    image_pdfs_path = Path(os.getenv('image_pdfs_path'))
    image_results_path = Path(os.getenv('image_results_path'))

    delete_all_files_in_folder(image_results_path)

    asyncio.run(process_images(image_pdfs_path))


