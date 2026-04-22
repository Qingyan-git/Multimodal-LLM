from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import json

from langchain_text_splitters import RecursiveCharacterTextSplitter

from text_chunking import delete_all_files_in_folder,save_to_file
from llms_and_models import OpenAIModel
from chunks import ImageChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant, format_embeddings



async def extract_images(filepath):

    model = OpenAIModel()
    all_chunks = []

    with pymupdf.open(filepath) as doc:

        full_text = pymupdf4llm.to_markdown(
            doc,
            header=False,
            footer=False,
            force_text=True,
        )

        #Disable pymupdf layout module to enable image_size_limit filter
        pymupdf4llm.use_layout(yes=False)
        #Saves the images detected to file, and returns a list of pages
        pages = pymupdf4llm.to_markdown(
            doc,
            force_text=True,
            image_format='jpg',
            image_path = os.getenv('images_store_path'),
            image_size_limit=0.20,
            write_images=True,
            page_chunks=True,
        )

        #Iterates through each page to process each image
        image_pattern = r"!\[[^\]]*\]\((.*?)\)"
        margin = 200
        for idx,page in enumerate(pages):
            save_to_file('markdown_test',json.dumps(page, indent=4, default=str),method='a')
            text = page['text']

            #For each image item found, get the text before and after the image for context
            # Strip out any other image strings found to not confuse LLM
            for match in re.finditer(image_pattern,text):
                path = match.group(1)

                start = match.start()
                end = match.end()
                start_idx = max(0,start-margin)
                end_idx = min(len(text), end+margin)
                prev_text = text[start_idx:start].strip()
                post_text = text[end+1:end_idx].strip()
                prev_text = re.sub(image_pattern, "", prev_text)
                post_text = re.sub(image_pattern, "", post_text)

                #Save the image bytes that were written to disk
                with open(path, "rb") as image_file:
                    image_bytes = image_file.read()

                #Remove the image now that the data is held in RAM
                try:
                    os.remove(path)
                except OSError as e:
                    print(f"Error: {path} : {e.strerror}")

                #Text description of the image
                image_description = await model.get_image_description(image_bytes)
                
                #Build the text description of the image chunk
                chunk_text = f"""
                Text before image : {prev_text}
                Image description : {image_description}
                Text after image : {post_text}
                """

                #Gets image context using chunk_text
                image_context = await model.get_context(full_text,chunk_text)

                chunk = ImageChunk()
                chunk.document_name = filepath.name
                chunk.context = image_context
                chunk.content = chunk_text
                chunk.metadata = {'pages' : page['metadata']['page']}
                
                all_chunks.append(chunk)

    return all_chunks




async def process_images(folder_path):
    try:
        if folder_path.is_dir():

            model = OpenAIModel()

            for file in folder_path.iterdir():

                image_chunks = await extract_images(file)

                print(f'\tFinished getting image chunks\n\n')

                datas = []
                for chunk in image_chunks:
                    data = f"""
                    Chunk from : {chunk.document_name}\n
                    Chunk context : {chunk.context}\n
                    Chunk content : {chunk.content}\n
                    Chunk metadata : {chunk.metadata}\n
                    """
                    datas.append(data)

                save_to_file("image-chunks",datas,os.getenv('images_store_path'),method='a')

                returned_chunks = save_document_chunks(file.name,image_chunks)

                print(f'\tFinished saving chunks into postgresdb\n\n')

                embeddings = await model.embed_texts(returned_chunks)

                print(f'\tFinished getting embeddings\n\n')

                upload_to_qdrant(embeddings)

                print(f'\tFinished uploading embeddings to qdrant\n\n')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise




if __name__ == "__main__":

    print(f'Ingestion running\n\n\n')
    image_pdfs_path = Path(os.getenv('image_pdfs_path'))
    images_store_path = Path(os.getenv('images_store_path'))

    insert_pdfs(image_pdfs_path)
    delete_all_files_in_folder(images_store_path)

    asyncio.run(process_images(image_pdfs_path))