from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
from pathlib import Path
from transformers import Blip2Processor, Blip2Model
import torch
import os

from postgre_setups import retrieve_chunks


#Setting up functions

def load_text_model(name='Salesforce/blip2-flan-t5-xl', device=None):
    """
    Loads the model as specified in the input argument
    """
    device = "cuda" if torch.cuda.is_available() else "cpu"
    processor = Blip2Processor.from_pretrained(name)
    model = Blip2Model.from_pretrained(name).to(device)
    
    return model, processor, device


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

            qdrant_client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=model.text_projection.out_features,
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


def embed_text_chunks(chunks,model,processor,device):
    """
    Takes in the chunks for a document
    Embeds those chunks using model
    Returns embeddings
    """
    
    embeddings = []

    for chunk in chunks:

        processed_text = processor(text=[chunk.text], return_tensors="pt").to(device)

        dummy_image = torch.zeros(1,3,224,224)

        processed_text = processor(
            text=[chunk.text],
            images = dummy_image,
            return_tensors="pt",
            padding=True
        ).to(device)

        with torch.no_grad():
            text_embedding = model(**processed_text)

        embedding = {
            'id' : chunk.id,
            'vector' : text_embedding,
            'payload' : chunk
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

    model,processor,device = load_text_model()

    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    create_qdrant_collection(qdrant_client,qdrant_collection_name,model)

    all_documents = retrieve_chunks()

    for document in all_documents:
        document_embeddings = embed_text_chunks(document,model,processor,device)

        upload_to_qdrant(document_embeddings)

    print(f'Done\n\n')

