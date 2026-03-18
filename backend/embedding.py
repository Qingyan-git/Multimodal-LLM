from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
import dotenv
import os
from pathlib import Path

from postgre_setups import retrieve_chunks



def load_model(name='all-MiniLM-L6-v2'):
    """
    Loads the model as specified in the input argument
    """
    model = SentenceTransformer(name)

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


def embed_chunks(document_name, document_metadata, chunks, model):
    """
    Takes in the document_id and its metadata along with all of the associated chunks
    Embeds the chunks and attaches relevant metadata
    Returns embeddings
    """

    embeddings = []
    texts = [chunk['text'] for chunk in chunks]
    vectors = model.encode(texts)

    for index,chunk in enumerate(chunks):
        embedding = {
            'id': chunk['id'],
            'vector' : vectors[index].tolist(),
            'payload' : {
                'title' : document_name,
                'text' : chunk['text'],
                'pages' : chunk['pages'],
                'metadata' : document_metadata
            }
        }

        embeddings.append(embedding)

    return embeddings


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


def embed_documents(qdrant_cluster_endpoint,qdrant_api_key,collection_name):
    """
    Embeds the chunks from all documents in the postgresql db and uploads into qdrant
    """
    
    model = load_model()

    qdrant_client = get_qdrant_client(qdrant_cluster_endpoint,qdrant_api_key)

    create_qdrant_collection(qdrant_client,collection_name,model)

    all_chunks = retrieve_chunks()

    for document_chunks in all_chunks:
        name = document_chunks['name']
        metadata = document_chunks['metadata']
        chunks = document_chunks['chunks']

        document_embeddings = embed_chunks(name,metadata,chunks,model)

        print(f'Uploading chunks from {name} to qdrant now\n')
        upload_to_qdrant(qdrant_client,collection_name,document_embeddings)
        print(f'Upload complete\n\n')

    print(f'All chunks uploaded\n\n')




# if __name__ == "__main__":

dotenv.load_dotenv()

qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
qdrant_api_key = os.getenv('qdrant_api_key')
qdrant_collection_name = os.getenv('qdrant_collection_name')

if not qdrant_cluster_endpoint or not qdrant_api_key or not qdrant_collection_name:
    raise AttributeError("Environment variable not found, please check your env files \n\n")

embed_documents(qdrant_cluster_endpoint,qdrant_api_key,qdrant_collection_name)


# all_chunks = retrieve_chunks()

# second_doc = all_chunks[1]

# name = second_doc['name']
# metadata = second_doc['metadata']
# chunks = second_doc['chunks']

# print(f'document name : {name}')
# print(f'document metadata : {metadata}')
# print(f'document first chunk : {chunks[0]}')



