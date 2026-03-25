from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
from postgre_setups import retrieve_image_data
import os

from models.chunk import Chunk

def get_qdrant_client():
    """
    Returns a qdrant_client object
    """
    try: 

        qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
        qdrant_api_key = os.getenv('qdrant_api_key')

        qdrant_client = QdrantClient(
            url=qdrant_cluster_endpoint,
            api_key=qdrant_api_key,
            timeout=30
        )

        return qdrant_client

    except Exception as e:
        print(f'Failed to connect to qdrant using parameters, error {e}\n\n')


# def create_qdrant_collection(qdrant_client, collection_name, model):
#     """
#     Sets up the qdrant collection
#     """

#     try:
#         existing_collection = qdrant_client.collection_exists(collection_name)
#         if existing_collection:
#             print(f'Collection {collection_name} already exists, deleting and recreating now...\n')

#             qdrant_client.delete_collection(collection_name=collection_name)

#             print(f'Collection {collection_name} deleted\n')
            
#             qdrant_client.create_collection(
#                 collection_name=collection_name,
#                 vectors_config=VectorParams(
#                     size=model.get_dims(),
#                     distance=qdrant_client.models.Distance.COSINE
#                 )
#             )

#             print(f'Collection {collection_name} created\n\n')
        
#         else:
#             print(f'Collection {collection_name} does not exist, creating now\n')

#             qdrant_client.create_collection(
#                 collection_name=collection_name,
#                 vectors_config=VectorParams(
#                     size=model.get_dims(),
#                     distance=qdrant_client.models.Distance.COSINE
#                 )
#             )

#             print(f'Collection {collection_name} created\n\n')

#     except Exception as e:
#         print(f'Failed to connect to qdrant, error {e}\n\n')


def upload_to_qdrant(embeddings):
    """
    Uploads the embeddings into collection_name using qdrant_client
    """
    try : 

        collection_name = os.getenv('qdrant_collection_name')

        qdrant_client = get_qdrant_client()

        if qdrant_client:
            qdrant_client.upsert(
                collection_name = collection_name, #type:ignore
                points = embeddings
            )

    except Exception as e:
        print(f'Unable to upload embeddings to qdrant cloud, error {e}\n\n')


def similarity_search(query_vector,limit=5,query_filter=None):
    try:

        collection_name = os.getenv('qdrant_collection_name')

        qdrant_client = get_qdrant_client()

        results = qdrant_client.query_points( #type:ignore
            collection_name=collection_name, #type:ignore
            query=query_vector,
            limit=limit,
            query_filter=query_filter
        )

        if results:
            similar_chunks = []
            for result in results.points:
                if result.payload:
                    chunk = Chunk()
                    chunk.id = result.payload['id']
                    chunk.type = result.payload['type']
                    chunk.text = result.payload['text']
                    if result.payload['type']=='image':
                        chunk.image_data = retrieve_image_data(result.payload['id'])
                    chunk.pages = result.payload['pages']
                    chunk.document_name = result.payload['document_name']

                item = {
                    'id' : result.id,
                    'similarity' : result.score,
                    'payload' : result.payload
                }

                similar_chunks.append(item)

            return similar_chunks

    except Exception as e:
        print(f'Unable to conduct similarity search on qdrant, error {e}\n')
        raise
