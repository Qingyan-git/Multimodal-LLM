
import os
import asyncio
import sys

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
from qdrant import get_similar_chunks

async def answer_user_query():

    model = OpenAIModel()
    sparse = SparseEmbedder()
    late = ColBERTEmbedder()

    try:

        user_query = input(f'\n\nEnter a query for retrieval, enter -9999 to exit : ')

        while user_query != '-9999':
            
            dense_vector = await model.get_query_vector(user_query)
            sparse_vector = sparse.embed_query(user_query)
            late_vector = late.embed_query(user_query)

            retrieved_chunks = get_similar_chunks(dense_vector,sparse_vector,late_vector)
            formatted_chunks = [ContextMessage(chunk).get_message() for chunk in retrieved_chunks]

            answer = await model.answer_question(user_query,formatted_chunks)

            response = f"User query : {user_query}, answer : {answer}"
            save_to_file(filename=f"User Queries.txt",content=response,filepath=os.getenv('user_results_path'),method='a')

            user_query = input(f'\n\nEnter a query for retrieval, enter -9999 to exit : ')

    except Exception as e:
        print(f'An error occured : {e}\n\n')

if __name__ == '__main__':

    asyncio.run(answer_user_query())




