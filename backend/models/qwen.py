from PIL import Image
from io import BytesIO
import numpy as np

from scripts.qwen3_vl_embedding import Qwen3VLEmbedder

class Qwen_V3_VL():

    # quant_config = BitsAndBytesConfig(
    #     load_in_4bit=True,
    #     bnb_4bit_compute_dtype=torch.bfloat16,
    #     bnb_4bit_quant_type="nf4"
    # )

    def __init__(self, model="Qwen/Qwen3-VL-Embedding-2B", dims=2048):
        
        self.embedder = Qwen3VLEmbedder(
            model,
            # quantization_config = self.quant_config
            )
        
        self.model = model

        self.dims = dims

    
    def get_dims(self):
        return self.dims
    

    def encode_chunks(self,chunks):

        print(f'\t\t Preprocessing chunks now\n')

        processed_chunks = []
        for chunk in chunks:
            if chunk.type == 'text':
                processed_chunks.append({
                    'text' : chunk.text
                })
            elif chunk.type == 'image':
                processed_chunks.append({
                    'text' : chunk.text,
                    'image' : Image.open(BytesIO(chunk.image_data)).convert("RGB")
                })

        print(f'\t\t Finished preprocessing chunks\n')


        print(f'\t\t Calculating embeddings for chunks now\n')

        vectors = []

        for chunk in processed_chunks:
            vectors.append(self.embedder.process([chunk])[0])

        print(f'\t\t Finished calculating embeddings for chunks\n')


        print(f'\t\t Transforming chunks into Qdrant recognised format now\n')

        all_embeddings = []
        for i,chunk in enumerate(chunks):
            all_embeddings.append({
                'id' : chunk.id,
                'vector' : vectors[i],
                'payload' : {
                    'type' : chunk.type,
                    'text' : chunk.text,
                    'document_name' : chunk.document_name,
                    'pages' : chunk.pages,
                    'embedded using' : self.model
                }
            })

        print(f'\t\t Finished transforming chunks into Qdrant recognised format\n')

        return all_embeddings
    

    def process_user_query(self):
        pass