from qdrant_setups import similarity_search
from models.qwen import Qwen_V3_VL


def process_user_query():

    model = Qwen_V3_VL()

    for _ in range(1):
        user_query = input(f"Enter a query for retrieval, enter 'exit' to exit : ")
        query_vector = model.process_user_query(user_query)
        similar_chunks = similarity_search(query_vector)

        if similar_chunks:
            for chunk in similar_chunks:
                print(f'Similar chunk : id {chunk['id']}\n')
                print(f'Chunk similarity : {chunk['similarity']}\n')
                print(f'Chunk payload : {chunk['payload']}\n')
                print('\n\n')


process_user_query()
