from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
import dotenv
import os

def embed_chunks(chunks, use='all-MiniLM-L6-v2'):
    '''
    Takes in chunks, and applies the model provided in the use argument to embed the chunks
    Returns the embedded chunks
    '''

    model = SentenceTransformer(use)
    embeddings = []

    texts = [chunk['content'] for chunk in chunks]
    vectors = model.encode(texts)

    for index, chunk in enumerate(chunks):

        chunk_content = chunk['content']
        chunk_pages = chunk['pages']
        chunk_metadata = chunk['metadata']

        embed = {
            'id' : index,
            'vector' : vectors[index].tolist(), #because vectors is a NumPy arr and qdrant expects regular python lists
            'payload' : {
                'title' : chunk['metadata']['title'],
                'pages' : chunk_pages,
                'metadata' : chunk_metadata
            }
        }

        embeddings.append(embed)

    return (embeddings,model)



def upload_to_qdrant(qdrant_cluster_endpoint,qdrant_api_key,chunks,use='all-MiniLM-L6-v2'):

    qdrant_client = QdrantClient(
        url=qdrant_cluster_endpoint, 
        api_key=qdrant_api_key,
    )

    embeddings, model = embed_chunks(chunks,use)


    collection_name = "document_chunks"

    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=model.get_sentence_embedding_dimension(),  # embedding dimension
            distance="Cosine"  # or "Dot" or "Euclid"
        )
    )

    qdrant_client.upsert(
    collection_name=collection_name,
    points=embeddings
)




# if __name__ == "__main__":

dotenv.load_dotenv()

qdrant_cluster_endpoint = os.getenv('qdrant_cluster_endpoint')
qdrant_api_key = os.getenv('qdrant_api_key')


example_chunk = [{'content': 'Government Data Security Policies  |   !1GOVERNMENT  DATA SECURITY POLICIES'
'This document contains general information for the public only. It is not intended to be relied upon as a '
'comprehensive or definitive guide on each agency’s policies and practices. The data security measures '
'implemented by each agency will differ depending on various factors such as the sensitivity of the data and '
'the agency’s assessment of data security risks. The Government may update the policies set out in this document '
'without publishing such updates to the public.The Government takes its responsibility as a custodian of data '
'very seriously. Since 2001, the Government’s data security policies have been set out in the Government', 
'pages': [1, 2], 
'metadata': {
    'format': 'PDF 1.4', 
    'title': 'Government Data Securities', 
    'author': '', 
    'subject': '', 
    'keywords': '', 
    'creator': 'Pages', 
    'producer': 'Mac OS X 10.11.6 Quartz PDFContext', 
    'creationDate': "D:20201117020638Z00'00'", 
    'modDate': "D:20201117020638Z00'00'", 
    'trapped': '', 
    'encryption': None}}]


embeddings,model = embed_chunks(example_chunk)
print(embeddings)