
import os
import asyncio
import sys
import pandas as pd
from pathlib import Path

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



async def answer_testset(filepath,model,sparse,late):

    df = pd.read_csv(filepath)
    questions = df.iloc[:, 0]
    results = []
    for question in questions:

        dense_vector = await model.get_query_vector(question)
        sparse_vector = sparse.embed_query(question)
        late_vector = late.embed_query(question)

        retrieved_chunks = get_similar_chunks(dense_vector,sparse_vector,late_vector)
        formatted_chunks = [ContextMessage(chunk).get_message() for chunk in retrieved_chunks]

        answer = await model.answer_question(question,formatted_chunks)

        results.append({'Question' : question, 'Answer': answer})

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

            retrieved_chunks = get_similar_chunks(dense_vector,sparse_vector,late_vector)
            formatted_chunks = [ContextMessage(chunk).get_message() for chunk in retrieved_chunks]

            answer = await model.answer_question(user_query,formatted_chunks)

            response = f"User query : {user_query}\nAnswer : {answer}\nDocuments and pages that were considered in answer : {[f"{chunk['document_name']}, {chunk['metadata']['pages']}" for chunk in retrieved_chunks]}\n"
            save_to_file(filename=f"User Queries.txt",content=response,filepath=os.getenv('user_results_path'),method='a')
        except Exception as e:
            print(f'An error occured : {e}\n\n')



if __name__ == '__main__':

    user_testset_path = Path(os.getenv('user_testset_path'))
    asyncio.run(answer_all(user_testset_path))




