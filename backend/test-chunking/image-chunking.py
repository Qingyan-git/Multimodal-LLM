from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import traceback

from langchain_text_splitters import RecursiveCharacterTextSplitter

from text_chunking import embed_chunks
from llms_and_models import OpenAIModel
from chunks import TextChunk,ImageChunk
from postgres import save_document_chunks, insert_pdf
from qdrant import upload_to_qdrant, format_embeddings

dotenv.load_dotenv()


def clear_folder(folder_path=os.getenv('image_folder_path')):

    path = Path(folder_path)
    if not path.exists():
        return
    # rglob("*") finds everything in the folder and its subdirectories
    for item in path.rglob("*"):
        if item.is_file():
            item.unlink()  # Deletes the file, but leaves the folder


def save_image(image,filename,path=Path(os.getenv('image_folder_path'))):
    
    path.mkdir(parents=True, exist_ok=True)
    save_path = path / filename
    
    image.save(save_path, output='jpeg', jpg_quality=95)



def get_surrounding_text(page,image_bbox,margin=3):

    blocks = page.get_text('blocks',sort=True)

    image_block_no = 0
    for i,block in enumerate(blocks):
        if block[:4] == image_bbox:
            image_block_no = i

    pre_text_limit = max(image_block_no-margin,0)
    post_text_limit = min(image_block_no+margin+1,len(blocks))

    text = f"Text before image : "
    for i in range(pre_text_limit,image_block_no):
        if blocks[i][6] == 0: #Text block
            text += blocks[i][4] + " "

    text += f"\nText after image : "
    for i in range(image_block_no+1,post_text_limit):
        if blocks[i][6] == 0: #Text block
            text += blocks[i][4] + " "


    return text



async def extract_images(filepath):

    filename = filepath.name
    model = OpenAIModel()
    
    seen_hashes = set()
    image_chunks = []
    tasks = []

    with pymupdf.open(filepath) as doc:

        document_text = pymupdf4llm.to_markdown(doc)
        
        for page_no, page in enumerate(doc):
            images = page.get_image_info(hashes=True,xrefs=True)
            for image in images:

                mask = image['has-mask']
                xref = image['xref']
                number = image['number']
                digest = image['digest']
                image_bbox = image['bbox']

                pass_filters = (not mask and digest not in seen_hashes and xref)

                if pass_filters:
                    seen_hashes.add(image['digest'])

                    pix = pymupdf.Pixmap(doc,xref)
                    pix = pymupdf.Pixmap(pymupdf.csRGB,pix)
                    filename = f"{xref}_page_{page_no + 1}_image_{number}.jpg"

                    save_image(pix,filename)

                    """
                    Clarify functions to get context and get content so that image context is the LLM contextual placing
                    of the image text summary and its surrounding text for contextual retrieval
                    and that image content is the text summary and the surrounding text of the image itself
                    """
                    
                    surrounding_text = get_surrounding_text(page,image_bbox)
                    task = asyncio.create_task(model.get_image_context(document_text,pix,surrounding_text))

                    tasks.append(task)

                    image_chunk = ImageChunk()
                    image_chunk.document_name = filename
                    image_chunk.content['text'] = surrounding_text
                    image_chunk.content['image'] = pix.tobytes('jpg')
                    image_chunk.content['metadata'] = {
                        'xref' : xref,
                        'pages' : [page_no],
                        'digest' : digest,
                        'number' : number,
                        'bbox' : image_bbox
                    }

                    image_chunks.append(image_chunk)

    contexts = await asyncio.gather(*tasks)

    for i,context in enumerate(contexts):
        image_chunks[i].context = context

    """
    Like this is weird to see maybe just consolidate the creation to chunks to one part of the code then dont need
    to set contents and contexts differently
    """

    return image_chunks



async def process_images(folder_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                print(f'Processing {file.name}\n\n')

                with pymupdf.open(file) as doc:
                    metadata = doc.metadata.copy()

                insert_pdf(file,metadata)

                print(f'\tFinished inserting pdf\n\n')

                chunks = await extract_images(file)

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




if __name__ == "__main__":

    image_pdfs = Path(os.getenv('image_pdfs'))

    clear_folder()

    for file in image_pdfs.iterdir():
        extract_images(file)

