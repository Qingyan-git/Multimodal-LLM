from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
from pathlib import Path
from transformers import PreTrainedModel, AutoModel
import torch
import os

from postgre_setups import retrieve_chunks


#Setting up functions

def load_model(name="jinaai/jina-embeddings-v4", device=None):
    """
    Loads Jina v4 embedding model
    """

    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    model = AutoModel.from_pretrained(
        name,
        trust_remote_code=True,
        torch_dtype=torch.float16 if device == "cuda" else torch.float32,
    ).to(device)
    model.eval()

    return model


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


def create_qdrant_collection(qdrant_client, collection_name,model):
    """
    Sets up the qdrant collection
    """

    try:
        existing_collection = qdrant_client.get_collection(collection_name)
        if not existing_collection:

            print(f'Creating collection {collection_name} now\n')

            dummy_embedding = model.encode([{"text": "dummy"}])
            vector_size = len(dummy_embedding[0])

            qdrant_client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=qdrant_client.models.Distance.cosine
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


#Execution functions


def embed_chunks(chunks,model):
    """
    Takes in the chunks for a document
    Embeds those chunks using model
    Returns embeddings
    """
    
    inputs = []

    for chunk in chunks:
        if chunk.type == 'text':
            inputs.append({'text' : chunk.text })
        elif chunk.type == 'image':
            inputs.append({'text' : chunk.text, 'image' : chunk.image_data })

    vectors = model.encode(inputs)

    embeddings = []
    for i, chunk in enumerate(chunks):
        embedding = {
            "id": chunk.id,
            "vector": vectors[i],
            "payload": chunk
        }
        embeddings.append(embedding)

    return embeddings


def embed_documents():
    """
    Embeds documents
    """

    qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
    qdrant_api_key = os.getenv('qdrant_api_key')
    qdrant_collection_name = os.getenv('qdrant_collection_name')

    model = load_model()

    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    create_qdrant_collection(qdrant_client,qdrant_collection_name,model)

    all_documents = retrieve_chunks()

    for document in all_documents:
        document_embeddings = embed_chunks(document,model)

        upload_to_qdrant(document_embeddings)

    print(f'Done\n\n')

