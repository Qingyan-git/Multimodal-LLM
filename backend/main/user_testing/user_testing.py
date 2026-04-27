
import os
import asyncio


from ..main.llms_and_models import OpenAIModel,ContextMessage
from ..main.text_chunking import save_to_file
from ..main.qdrant import get_similar_chunks

async def answer_user_query():

    model = OpenAIModel()
    user_query = input(f'Enter a query for retrieval, enter -9999 to exit')

    while user_query != '-9999':
        
        query_vector = await model.get_query_vector(user_query)
        retrieved_chunks = get_similar_chunks(query_vector)
        formatted_chunks = [ContextMessage(chunk).get_message() for chunk in retrieved_chunks]
        answer = await model.answer_question(user_query,formatted_chunks)

        response = f"User query : {user_query}, answer : {answer}"
        save_to_file(filename=f"{user_query[:20]}.txt",content=retrieved_chunks,filepath=os.getenv('user_results_path'),method='a')

        user_query = input(f'Enter a query for retrieval, enter -9999 to exit')

if __name__ == '__main__':

    asyncio.run(answer_user_query())




