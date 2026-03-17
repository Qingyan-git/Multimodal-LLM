import os
import dotenv

from embedding import get_qdrant_client
from embedding import load_model


def embed_query(user_query,model):
    """
    Embeds a user query using the model to produce the vector for that query
    """
    return model.encode(user_query).tolist()


def search_qdrant(qdrant_client,collection_name,query_vector,limit=5,filter=None):
    """
    Retrieves chunks relating to the user query from qdrant
    """
    results = qdrant_client.query_points(
        collection_name=collection_name,
        query=query_vector,
        limit=limit,
        query_filter=filter
    )

    similar_chunks = []

    for result in results.points:
        chunk = {
            'id' : "",
            'similarity' : 0.00,
            'document_name' : "",
            'pages' : [],
            'text' : "",
            'metadata' : {}
        }

        chunk['id'] = result.id
        chunk['similarity'] = result.score
        res_metadata = result.payload
        chunk['document_name'] = res_metadata['title']
        chunk['pages'] = res_metadata['pages']
        chunk['text'] = res_metadata['text']
        chunk['metadata'] = res_metadata['metadata']

        similar_chunks.append(chunk)

    return similar_chunks




def process_user_query(qdrant_client,collection_name,model,user_query):
    """
    Retrieves most similar chunks to input user query
    """

    embedded_query = embed_query(user_query,model)

    similar_chunks = search_qdrant(qdrant_client,collection_name,embedded_query)

    for chunk in similar_chunks:
        print(chunk)
        print()
        print()


def get_user_query():

    qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
    qdrant_api_key = os.getenv('qdrant_api_key')
    collection_name = os.getenv('qdrant_collection_name')

    model = load_model()
    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)


    for _ in range(1):
        user_query = input(f"Enter a query for retrieval, enter 'exit' to exit : ")
        process_user_query(qdrant_client,collection_name,model,user_query)


    # exit = False
    # while not exit:
    #     user_query = input(f"Enter a query for retrieval, enter 'exit' to exit : ")

    #     if user_query == 'exit':
    #         exit = True
    #     else:
    #         process_user_query(qdrant_client,collection_name,model,user_query)



get_user_query()
