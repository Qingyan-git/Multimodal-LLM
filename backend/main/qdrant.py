from qdrant_client import QdrantClient, models
from qdrant_client.models import PointStruct, Distance, VectorParams, models

import os
from dataclasses import asdict

from chunks import Chunk



# Creation functions

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
            timeout=120
        )

        return qdrant_client

    except Exception as e:
        print(f'Failed to connect to qdrant using parameters, error {e}\n\n')


def create_collection():

    try:

        qdrant_client = get_qdrant_client()
        collection_name = os.getenv('qdrant_collection_name')

        if qdrant_client.collection_exists(collection_name=collection_name):

            print(f'Collection exists currently, deleting to reset\n')
            qdrant_client.delete_collection(collection_name=collection_name)

        qdrant_client.create_collection(
            collection_name,
            vectors_config={
                "dense": models.VectorParams(
                    size=1536,
                    distance=models.Distance.COSINE,
                ),
                "multi": models.VectorParams(
                    size=96,
                    distance=models.Distance.COSINE,
                    multivector_config=models.MultiVectorConfig(
                        comparator=models.MultiVectorComparator.MAX_SIM,
                    ),
                    hnsw_config=models.HnswConfigDiff(m=0)  #  Disable HNSW for reranking
                ),
            },
            sparse_vectors_config={
                "sparse": models.SparseVectorParams(modifier=models.Modifier.IDF)
            }
        )

        print(f'Collection created\n\n')

    except Exception as e:
        print(f'Unable to create collection on qdrant cloud, error {e}\n\n')


def delete_points(filters):

    try:

        qdrant_client = get_qdrant_client()
        collection_name = os.getenv('qdrant_collection_name')

        print(f'Attempting to delete all points from cloud\n\n')

        qdrant_client.delete(
            collection_name=collection_name,
            points_selector=filters
        )

        print(f'Finished deleteing points from cloud\n\n')

    except Exception as e:
        print(f'Unable to delete points from qdrant cloud, error {e}\n\n')



# Execution functions

def upload_to_qdrant(chunks, dense, sparse, late):

    try : 
        qdrant_client = get_qdrant_client()
        collection_name = os.getenv('qdrant_collection_name')
        
        points=[
            models.PointStruct(
                id=chunk.id,
                vector={
                    # 1. Dense (OpenAI)
                    "dense": dense[i], 
                    
                    # 2. Sparse (BM25/SPLADE) - Using the dict from your SparseEmbedder
                    "sparse": models.SparseVector(
                        indices=sparse[i]["indices"],
                        values=sparse[i]["values"]
                    ),
                    
                    # 3. Multi (ColBERT) - Ensure this is a list of lists (float)
                    "multi": late[i]
                },
                payload=asdict(chunk)
            ) for i, chunk in enumerate(chunks)]

        # 3. Perform the upload
        qdrant_client.upload_points(
            collection_name=collection_name,
            points=points,
            parallel=4,
            batch_size=32,
            wait=True
        )

    except Exception as e:
        print(f'Unable to upload embeddings to qdrant cloud, error {e}\n\n')


def get_similar_chunks(dense,sparse,late,limit1=20,limit2=4,filters=None):

    try:
        collection_name = os.getenv('qdrant_collection_name')
        client = get_qdrant_client()

        prefetch = [
            models.Prefetch(
                query=dense,
                using="dense",
                limit=limit1,
            ),
            models.Prefetch(
                query=models.SparseVector(
                    indices=sparse["indices"], 
                    values=sparse["values"]
                ),
                using="sparse",
                limit=limit1,
            ),
        ]

        results = client.query_points(
            collection_name=collection_name,
            prefetch=prefetch,
            query=late, 
            using="multi",
            with_payload=True,
            limit=limit2,
            query_filter=filters
        )

        if results:
            similar_chunks = []
            for result in results.points:

                item = {
                    'id' : result.id,
                    'score' : result.score, 
                    'document_name' : result.payload['document_name'], 
                    'context' : result.payload['context'],
                    'content' : result.payload['content'], 
                    'metadata' : result.payload['metadata']
                }

                similar_chunks.append(item)

            similar_chunks.sort(key=lambda item: item['score'], reverse=True)

            return similar_chunks

        else:
            print('No results found for this query.\n\n')
            return []

    except Exception as e:
        print(f'Unable to get similar chunks, error {e}\n\n')
        raise



if __name__ == '__main__':

    sure = input('Are you sure? Enter Y to continue : ')

    if sure == 'Y':

        recreate = 1

        if recreate:
            create_collection()

        else:
            chunk_type = 2
            chunks = {
                0:None,
                1:'text',
                2:'image',
                3:'tables'
            }
            # 1. Get the string value (e.g., 'image')
            target = chunks.get(chunk_type)

            if target:
                # 2. Construct the Qdrant Filter
                # Change "chunk_type" to the actual key name in your payload
                delete_filter = models.Filter(
                    must=[
                        models.FieldCondition(
                            key="type", 
                            match=models.MatchValue(value=target)
                        )
                    ]
                )

                # 3. Call the delete function
                delete_points(delete_filter)
                print(f'Points of type {target} deleted\n')

            else:
                print(f'No type selected, not deleting anything\n')

    else:
        print('Aborted\n\n')

