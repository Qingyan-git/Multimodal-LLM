from qdrant_client import QdrantClient, models
from qdrant_client.models import PointStruct
from chunks import Chunk
import os
import uuid
from dataclasses import asdict


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


def delete_points(collection_name):

    try:

        qdrant_client = get_qdrant_client()

        print(f'Attempting to delete all points from cloud\n\n')

        qdrant_client.delete(
            collection_name=collection_name,
            points_selector=models.Filter(must=[])
        )

        print(f'Finished deleteing all points from cloud\n\n')

    except Exception as e:
        print(f'Unable to delete points from qdrant cloud, error {e}\n\n')


def format_embeddings(chunks,vectors):

    embeddings = []

    for i, chunk in enumerate(chunks):
        embedding = PointStruct(
            id=chunk.id,
            vector=vectors[i],
            payload=asdict(chunk)
        )

        embeddings.append(embedding)

    return embeddings


def upload_to_qdrant(embeddings):
    """
    Uploads the embeddings into collection_name using qdrant_client
    """
    try : 
        collection_name = os.getenv('qdrant_collection_name')
        qdrant_client = get_qdrant_client()

        if qdrant_client:
            qdrant_client.upsert(
                collection_name = collection_name,
                points = embeddings
            )

    except Exception as e:
        print(f'Unable to upload embeddings to qdrant cloud, error {e}\n\n')





def get_similar_chunks(query_vector,limit=5,filters=None):

    try:
        collection_name = os.getenv('qdrant_collection_name')
        qdrant_client = get_qdrant_client()

        if qdrant_client:
            results = qdrant_client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=limit,
                query_filter=filters,
                with_payload=True,
                with_vectors=False
            ).points

            if results:
                similar_chunks = []
                for result in results:

                    item = {
                        'score' : result.score, 
                        'document_name' : result.payload['document_name'], 
                        'context' : result.payload['context'],
                        'content' : result.payload['content'], 
                        'metadata' : result.payload['metadata']
                    }

                    similar_chunks.append(item)

                similar_chunks.sort(key=lambda item: item['score'], reverse=True)

                return similar_chunks
        print('No results found for this query.\n\n')

    except Exception as e:
        print(f'Unable to get similar chunks, error {e}\n\n')
        raise


if __name__ == '__main__':
    delete_points(os.getenv('qdrant_collection_name'))
