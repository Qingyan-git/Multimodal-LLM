
import os
import asyncio
import sys
import pandas as pd
from pathlib import Path
from collections import defaultdict

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to 
# the sys.path.
sys.path.append(parent)

# now we can import the module in the parent
# directory.

from llms_and_models import OpenAIModel,ContextMessage,SparseEmbedder,ColBERTEmbedder
from text_chunking import save_to_file
from postgres import retrieve_pdf
from qdrant import get_similar_chunks



def get_page_as_jpeg(page_obj):
    # 1. Render the page to a Pixmap (RGB)
    # Use 'matrix' to increase resolution if the text is too small (e.g., zoom=2)
    pix = page_obj.get_pixmap()
    
    # 2. Convert to JPEG bytes
    image_bytes = pix.tobytes("jpg")
    
    # 3. Encode to Base64 (standard for OpenAI/Anthropic/Qwen API calls)
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    
    return base64_image


def get_relevant_chunks_images(dense,sparse,late):

    similar_chunks = get_similar_chunks(dense,sparse,late)
    relevant_chunks = defaultdict(set)

    for chunk in similar_chunks:
        document_name = chunk['document_name']
        pages = chunk['metadata']['pages']
        relevant_chunks[document_name].update(pages)

    images = []
    for document_name, pages in relevant_chunks.items():
        document_path = retrieve_pdf(document_name)
        with pymupdf.open(document_path) as doc:
            for page_no in pages:
                page = doc[page_no]
                jpeg_base64 = get_page_as_jpeg(page)
                images.append({
                    "image_data": jpeg_base64,
                    "source": f"Document: {document_name}, Page: {page_no}"
                })

    return images,relevant_chunks


async def answer_testset_images(filepath,model,sparse,late):

    df = pd.read_csv(filepath)
    questions = df.iloc[:, 0]
    results = []
    for question in questions:

        dense_vector = await model.get_query_vector(question)
        sparse_vector = sparse.embed_query(question)
        late_vector = late.embed_query(question)

        images, sources = get_relevant_chunks(dense_vector,sparse_vector,late_vector)

        answer = await model.answer_questions_images(question,images)

        results.append({'Question' : question ,'Answer': answer, 'Sources' : sources})

    results_df = pd.DataFrame(results)

    filename = Path(filepath).stem
    save_path = Path(os.getenv('user_results_path')) / f"{filename}_image-based_answers.csv"

    results_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    return results_df


def get_relevant_chunks(dense, sparse, late):
    
    similar_chunks = get_similar_chunks(dense,sparse,late)

    formatted_chunks = []
    relevant_chunks = []

    for chunk in similar_chunks:
        document_name = chunk['document_name']
        pages = ','.join(map(str,chunk['metadata']['pages']))
        text = chunk['content']

        relevant_chunks.append((document_name,pages,text))
        formatted_chunks.append(ContextMessage(chunk).get_message())

    sources = [f"\nChunk Text : {text} from {document_name}, page {pages}\n" for (document_name,pages,text) in relevant_chunks]
    sources_quote = "".join(sources)

    return (formatted_chunks,sources_quote)


async def answer_testset(filepath,model,sparse,late):

    df = pd.read_csv(filepath)
    questions = df.iloc[:, 0]
    results = []
    for question in questions:

        dense_vector = await model.get_query_vector(question)
        sparse_vector = sparse.embed_query(question)
        late_vector = late.embed_query(question)

        formatted_chunks, sources = get_relevant_chunks(dense_vector,sparse_vector,late_vector)

        answer = await model.answer_question(question,formatted_chunks)

        results.append({'Question' : question ,'Answer': answer, 'Sources' : sources})

    results_df = pd.DataFrame(results)

    filename = Path(filepath).stem
    save_path = Path(os.getenv('user_results_path')) / f"{filename}_answers.csv"

    results_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    return results_df


async def answer_all(folder_path):

    try:

        model = OpenAIModel()
        sparse_embedder = SparseEmbedder()
        late_embedder = ColBERTEmbedder()

        if folder_path.is_dir():
            for file in folder_path.iterdir():
                if file.is_file() and file.suffix == '.csv':
                    
                    await answer_testset(file,model,sparse_embedder,late_embedder)

                    await answer_testset_images(file,model,sparse_embedder,late_embedder)

                    print(f"\tFinished processing {file.stem}\n\n")
            
            print(f'All tests completed\n\n')

    
    except Exception as e:
        print(f'Unable to verify all testsetes, error {e}\n\n')


async def answer_user_query():

    model = OpenAIModel()
    sparse = SparseEmbedder()
    late = ColBERTEmbedder()

    while True:
        # Use a simpler loop break
        user_query = input(f'\n\nEnter a query (-9999 to exit): ')
        if user_query == '-9999':
            break

        try:
                
            dense_vector = await model.get_query_vector(user_query)
            sparse_vector = sparse.embed_query(user_query)
            late_vector = late.embed_query(user_query)

            formatted_chunks,sources = get_relevant_chunks(dense_vector,sparse_vector,late_vector)

            answer = await model.answer_question(user_query,formatted_chunks)

            response = f"User query : {user_query}\nAnswer : {answer}\nDocuments and pages that were considered in answer : {sources}\n"
            save_to_file(filename=f"User Queries.txt",content=response,filepath=os.getenv('user_results_path'),method='a')
        except Exception as e:
            print(f'An error occured : {e}\n\n')



if __name__ == '__main__':

    user_testset_path = Path(os.getenv('user_testset_path'))
    asyncio.run(answer_all(user_testset_path))




