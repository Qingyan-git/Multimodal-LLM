import os
from embedding import get_qdrant_client
from models.qwen import Qwen_V3_VL

def embed_query(user_query,model):
    """
    Embeds a user query using the model to produce the vector for that query
    """
    return model.process_user_query(user_query)


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
            'id' : result.id,
            'similarity' : result.score,
            'chunk' : result.payload
        }

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

    model = Qwen_V3_VL()
    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    for _ in range(1):
        user_query = input(f"Enter a query for retrieval, enter 'exit' to exit : ")
        process_user_query(qdrant_client,collection_name,model,user_query)



get_user_query()
