
import os

from postgre_setups import retrieve_chunks
from qdrant_setups import upload_to_qdrant
from models.qwen import Qwen_V3_VL


def embed_documents():
    """
    Embeds documents
    """

    qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
    qdrant_api_key = os.getenv('qdrant_api_key')

    if not (qdrant_cluster_endpoint and qdrant_api_key):
        raise ValueError("Environment variables unable to be initialised, please check environment variables\n")
    

    print(f'Loading model...\n')

    model = Qwen_V3_VL()

    print(f'Model loaded successfullly\n\n')


    print(f'Retrieving all chunks from postgresql now\n')

    all_documents = retrieve_chunks()

    print(f'All chunks retrieved\n\n')


    print(f'Embedding documents now and uploading to qdrant\n')

    for i,document in enumerate(all_documents):

        print(f'\t Embedding and uploading document {i} now\n')

        embeddings = model.encode_chunks(document)

        print(f'\t Finished embedding document, uploading document now\n')

        upload_to_qdrant(embeddings)

        print(f'\t Finished uploading document\n')

    print(f'Documents successfully uploaded\n\n')

    print(f'Done\n\n')

