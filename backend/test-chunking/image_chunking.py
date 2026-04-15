from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import traceback

from langchain_text_splitters import RecursiveCharacterTextSplitter

from text_chunking import embed_chunks,delete_all_files_in_folder
from llms_and_models import OpenAIModel
from chunks import TextChunk,ImageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant, format_embeddings

dotenv.load_dotenv()


def clear_folder(folder_path=os.getenv('image_results_path')):

    path = Path(folder_path)
    if not path.exists():
        return
    # rglob("*") finds everything in the folder and its subdirectories
    for item in path.rglob("*"):
        if item.is_file():
            item.unlink()  # Deletes the file, but leaves the folder


def save_image(image,filename,path=Path(os.getenv('image_results_path'))):
    
    path.mkdir(parents=True, exist_ok=True)
    save_path = path / filename
    
    image.save(save_path, output='jpeg', jpg_quality=95)




def get_surrounding_text(page, image_bbox, v_margin=200):

    blocks = page.get_text("blocks", sort=True)
    
    img_y0, img_y1 = image_bbox[1], image_bbox[3]
    
    before_text = []
    after_text = []
    
    for block in blocks:
        block_y0, block_y1, block_text, block_type = block[1], block[3], block[4], block[6]
        
        if block_type == 0:
            if block_y1 < img_y0 and (img_y0 - block_y1) < v_margin:
                before_text.append(block_text.strip())

            elif block_y0 > img_y1 and (block_y0 - img_y1) < v_margin:
                after_text.append(block_text.strip())
                
    return " ".join(before_text), " ".join(after_text)



# def get_surrounding_text(page,image_block_no,margin=3):

#     blocks = page.get_text('blocks',sort=True)

#     pre_text_limit = max(image_block_no-margin,0)
#     post_text_limit = min(image_block_no+margin+1,len(blocks))

#     before_text = f""
#     for i in range(pre_text_limit,image_block_no):
#         if blocks[i][6] == 0: #Text block
#             before_text += blocks[i][4] + " "

#     after_text += f""
#     for i in range(image_block_no+1,post_text_limit):
#         if blocks[i][6] == 0: #Text block
#             after_text += blocks[i][4] + " "

#     return before_text, after_text


# async def process_single_image(model, document_text, filepath, page_no, image_bbox, image_bytes, metadata):

#     with pymupdf.open(filepath) as doc:

#         page = doc[page_no]

#         before_text,after_text = get_surrounding_text(page, image_bbox)
#         image_summary = await model.get_image_description(image_bytes)
#         chunk_text = f"""
#             Text before image : {before_text}
#             Image description : {image_summary}
#             Text after image : {after_text}
#             """
#         image_context = await model.get_context(document_text, chunk_text)
        
#         image_chunk = ImageChunk()
#         image_chunk.document_name = filename
#         image_chunk.context = image_context
#         image_chunk.content = chunk_text
#         image_chunk.metadata = metadata

#         return image_chunk


async def process_single_image(model,document_text,filepath,page_no,image):

    with pymupdf.open(filepath) as doc:

        page = doc[page_no]
        xref = image['xref']
        number = image['number']
        image_bbox = image['bbox']

        #Get a pymupdf.Pixmap object from the image and convert to RGB colourspace
        pix = pymupdf.Pixmap(doc,xref)
        pix = pymupdf.Pixmap(pymupdf.csRGB,pix)

        #Save to file to see what images were extracted
        filename = f"{xref}_page_{page_no + 1}_block_{number}.jpg"
        save_image(pix,filename)

        #Gets text around image
        before_text,after_text = get_surrounding_text(page, image_bbox)

        #Gets text representation of image
        image_summary = await model.get_image_description(pix.tobytes("jpg", jpg_quality=95))

        #Build the text data that is the "image chunk"
        chunk_text = f"""
            Text before image : {before_text}
            Image description : {image_summary}
            Text after image : {after_text}
            """
        
        #Gets image context using "image chunk"
        image_context = await model.get_context(document_text, chunk_text)

        metadata = {
            'xref': xref,
            'pages' : page_no,
            'block': number,
            'bbox': image_bbox
        }
        
        image_chunk = ImageChunk()
        image_chunk.document_name = filename
        image_chunk.context = image_context
        image_chunk.content = chunk_text
        image_chunk.metadata = metadata

        return image_chunk




async def extract_images(filepath):

    filename = filepath.name
    model = OpenAIModel()
    
    image_chunks = []
    tasks = []

    with pymupdf.open(filepath) as doc:

        document_text = pymupdf4llm.to_markdown(doc)
        
        for page_no, page in enumerate(doc):
            images = page.get_image_info(hashes=True,xrefs=True)
            for image in images:

                mask = image['has-mask']
                xref = image['xref']
                
                pass_filters = (not mask and xref)

                if pass_filters:

                    task = asyncio.create_task(
                        process_single_image(model,document_text,filepath,page_no,image)
                    )

                    # number = image['number']
                    # image_bbox = image['bbox']
                    
                    # #Get a pymupdf.Pixmap object from the image and convert to RGB colourspace
                    # pix = pymupdf.Pixmap(doc,xref)
                    # pix = pymupdf.Pixmap(pymupdf.csRGB,pix)

                    # #Save to file to see what images were extracted
                    # filename = f"{xref}_page_{page_no + 1}_block_{number}.jpg"
                    # save_image(pix,filename)
                    
                    # #Get bytes and metadata to pass to helper function
                    # image_bytes = pix.tobytes('jpg',jpg_quality=95)
                    # metadata = {
                    #     'xref': xref,
                    #     'pages' : page_no,
                    #     'block': image['number'],
                    #     'bbox': image_bbox
                    # }

                    # task = asyncio.create_task(
                    #     process_single_image(model, document_text, filepath, page_no, image_bbox, image_bytes, metadata)
                    # )
                    # tasks.append(task)

    image_chunks = await asyncio.gather(*tasks)

    return image_chunks



async def process_images(folder_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                image_chunks = await extract_images(file)

                print(f'\tFinished getting image chunks\n\n')

                # returned_chunks = save_document_chunks(image_chunks)

                # print(f'\tFinished saving chunks into postgresdb\n\n')

                # embeddings = await embed_chunks(returned_chunks)

                # print(f'\tFinished getting embeddings\n\n')

                # upload_to_qdrant(embeddings)

                # print(f'\tFinished uploading embeddings to qdrant\n\n')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        traceback.print_exc() 
        raise




if __name__ == "__main__":

    print(f'Ingestion running\n\n\n')
    image_pdfs_path = Path(os.getenv('image_pdfs_path'))
    image_results_path = Path(os.getenv('image_results_path'))

    insert_pdfs(image_pdfs_path)
    delete_all_files_in_folder(image_results_path)

    image_chunks = asyncio.run(process_images(image_pdfs_path))
    datas = []
    for chunk in image_chunks:
        data = f"""
        Chunk from : {document_name}\n
        Chunk context : {chunk.context}\n
        Chunk content : {chunk.content}\n
        Chunk metadata : {chunk.metadata}\n
        """
        datas.append(data)

    save_to_file("image-chunks",datas,image_results_path)



"""

Check the chunking strategies again, maybe try unstructured for image chunking to get the nearest neighbours

"""

