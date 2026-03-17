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
    results = qdrant_client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=limit,
        filter=filter
    )

    similar_chunks = []

    for result in results:
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




def process_user_query(qdrant_cluster_endpoint,qdrant_api_key,collection_name,user_query):
    """
    Retrieves most similar chunks to input user query
    """

    model = load_model()

    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    embedded_query = embed_query(user_query,model)

    similar_chunks = search_qdrant(qdrant_client,collection_name,embedded_query)

    for chunk in similar_chunks:
        print(chunk)
        print()
        print()



