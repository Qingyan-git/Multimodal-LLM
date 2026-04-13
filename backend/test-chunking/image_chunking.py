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

    image_block_no = -1
    for i,block in enumerate(blocks):
        block_rect = pymupdf.Rect(block[:4])
        if block_rect.intersects(img_rect):
            image_block_no = i
            break

    if image_block_no == -1:
        return "No surrounding text found"

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


async def process_single_image(model, document_text, page, image_bbox, image_bytes, filename, metadata):

    surrounding_text = get_surrounding_text(page, image_bbox)
    image_summary = await model.get_image_description(image_bytes)
    image_context = await model.get_image_context(document_text, image_bytes, surrounding_text)

    """
    image_context should use the image_summary string form so that it takes less tokens?
    """
    
    image_chunk = ImageChunk()
    image_chunk.document_name = filename
    image_chunk.context = image_context
    image_chunk.content = image_summary
    image_chunk.metadata = metadata

    return image_chunk


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
                    
                    pix = pymupdf.Pixmap(doc,xref)
                    pix = pymupdf.Pixmap(pymupdf.csRGB,pix)
                    seen_hashes.add(image['digest'])

                    filename = f"{xref}_page_{page_no + 1}_image_{number}.jpg"
                    save_image(pix,filename)
                    
                    image_bytes = pix.tobytes('jpg',jpg_quality=95)
                    metadata = {
                        'pages' : page_no,
                        'xref': xref,
                        'digest': digest,
                        'number': image['number'],
                        'bbox': image_bbox
                    }

                    task = asyncio.create_task(
                        process_single_image(model, document_text, page, image_bbox, image_bytes, filename, metadata)
                    )
                    tasks.append(task)

    image_chunks = await asyncio.gather(*tasks)

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

    def main():

        image_pdfs = Path(os.getenv('image_pdfs'))

        clear_folder()

        for file in image_pdfs.iterdir():
            await extract_images(file)

    asyncio.run(main())

