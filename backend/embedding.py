from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
from transformers import AutoModel, AutoProcessor
import torch
import os
from PIL import Image
from io import BytesIO

from postgre_setups import retrieve_chunks
from models.qwen import Qwen


#Setting up functions


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
        existing_collection = qdrant_client.get_collection(collection_name)
        if not existing_collection:

            print(f'Creating collection {collection_name} now\n')

            qdrant_client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=model.get_sentence_embedding_dimension(),
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

    # need to figure out how to use Qwenv3 to embed my inputs

    vectors = model.encode(chunks)

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

    if not (qdrant_cluster_endpoint and qdrant_api_key and qdrant_collection_name):
        raise ValueError("Environment variables unable to be initialised, please check environment variables\n")
    
    print(f'Loading model...\n')

    model = Qwen()

    print(f'Model loaded successfullly\n\n')


    print(f'Getting qdrant client now...\n')

    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    print(f'Qdrant client successfully received\n\n')


    print(f'Checking for existence of collection...\n')

    create_qdrant_collection(qdrant_client,qdrant_collection_name,model)

    print(f'Collection created successfully\n\n')


    print(f'Retrieving all chunks from postgresql now\n')

    all_documents = retrieve_chunks()

    print(f'All chunks retrieved\n\n')


    print(f'Embedding documents now and uploading to qdrant\n')

    for document in all_documents:
        document_embeddings = embed_chunks(document,model)
        upload_to_qdrant(qdrant_client,qdrant_collection_name,document_embeddings)

    print(f'Documents successfully uploaded\n\n')

    print(f'Done\n\n')

