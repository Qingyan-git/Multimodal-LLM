from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams

def get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key):
    """
    Returns a qdrant_client object
    """
    try: 
        qdrant_client = QdrantClient(
            url=qdrant_cluster_endpoint,
            api_key=qdrant_api_key
        )

        return qdrant_client

    except Exception as e:
        print(f'Failed to connect to qdrant using parameters, error {e}\n\n')


def create_qdrant_collection(qdrant_client, collection_name, model):
    """
    Sets up the qdrant collection
    """

    try:
        existing_collection = qdrant_client.collection_exists(collection_name)
        if not existing_collection:
            print(f'Creating collection {collection_name} now...\n')
            
            qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=model.get_dims(),
                    distance=qdrant_client.models.Distance.COSINE
                )
            )

            print(f'Collection {collection_name} created\n\n')
        
        else:
            print(f'Collection {collection_name} already exists\n\n')

    except Exception as e:
        print(f'Failed to connect to qdrant, error {e}\n\n')


def upload_to_qdrant(qdrant_client,collection_name,embeddings):
    """
    Uploads the embeddings into collection_name using qdrant_client
    """
    try : 
        qdrant_client.upsert(
            collection_name = collection_name,
            points = embeddings
        )

    except Exception as e:
        print(f'Unable to upload embeddings to qdrant cloud, error {e}\n\n')