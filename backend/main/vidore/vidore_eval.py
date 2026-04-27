
import os
import numpy as np
import mteb
from PIL import Image
from typing import List, Dict, Any
import tempfile
import sys
from pathlib import Path

from ..main.llms_and_models import OpenAIModel
from ..main.text_chunking import extract_text
from ..main.docling_image_chunking import extract_images
from ..main.docling_table_chunk import extract_tables
from ..main.page_chunking import extract_pages



def convert_PIL_to_pdf(pil_image):
    """
    Converts a PIL object into a PyMuPDF Document object.
    """
    # 1. Create a new empty PDF
    doc = pymupdf.open()

    # 2. Convert PIL Image to bytes in memory (JPEG is usually fastest)
    img_byte_arr = BytesIO()
    pil_image.save(img_byte_arr, format='JPEG')
    img_bytes = img_byte_arr.getvalue()

    # 3. Create a page matching the image dimensions
    # PIL uses (width, height)
    page = doc.new_page(width=pil_image.width, height=pil_image.height)

    # 4. Insert the image to fill the page
    # (0, 0, width, height) defines the target rectangle
    page.insert_image(page.rect, stream=img_bytes)

    return doc





class MyVidoreRetriever:
    """
    Standard MTEB interface for Multimodal Retrieval.
    """

    def __init__(self):
        # Initialize your local model (e.g., Qwen3-VL) or API client
        self.model = OpenAIModel()


    def encode_queries(self, queries: List[str], batch_size: int = 16, **kwargs) -> np.ndarray:
        """
        Input: list of text strings (queries).
        Output: numpy array of shape (num_queries, embedding_dim).
        """

        return asyncio.run(self._async_encode_queries(queries,batch_size))


    async def _async_encode_queries(self,queries,batch_size):
        tasks = [self.model.get_query_vector(query) for query in queries]
        embeddings = await asyncio.gather(*tasks)
        
        return np.array(embeddings)


    def encode_corpus(self, corpus: List[Dict[str, Any]], batch_size: int = 16, **kwargs) -> np.ndarray:
        """
        Input: list of dicts. ViDoRe provides [{'image': PIL.Image, 'text': str}, ...]
        Output: numpy array of shape (num_corpus_items, embedding_dim).
        """

        return asyncio.run(self._async_encode_corpus(corpus, batch_size))


    async def _async_encode_corpus(self, corpus, batch_size):

        all_page_embeddings = []

        # Break the corpus into batches of size 'batch_size'
        for i in range(0, len(corpus), batch_size):
            batch = corpus[i : i + batch_size]
            
            # Process all PDFs in the current batch concurrently
            tasks = [self._process_single_item(item) for item in batch]
            batch_results = await asyncio.gather(*tasks)
            
            all_page_embeddings.extend(batch_results)

        return np.array(all_page_embeddings)


    async def _process_single_item(self,item):

        # ViDoRe is vision-centric; extract the PIL image
        pil_image = item["image"] 
        page_doc = convert_PIL_to_pdf(pil_image)

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True, delete_on_close=False) as temp_pdf:
            temp_path = temp_pdf.name
            page_doc.save(temp_path)

            page_vec = None # Initialize to prevent UnboundLocalError
            try:
                all_chunks = []
                all_chunks.extend(await extract_text(temp_path))
                all_chunks.extend(await extract_images(temp_path))
                all_chunks.extend(await extract_tables(temp_path))
                all_chunks.extend(await extract_pages(temp_path))

                embeddings, cost = await self.model.embed_texts(all_chunks)
                chunk_vectors = [e.vector for e in embeddings]

                if not chunk_vectors:
                    # Fallback: if no content was found, use a zero vector
                    page_vec = np.zeros(self.model.dimension)
                else:
                    # 3. MEAN POOLING
                    page_vec = np.mean(chunk_vectors, axis=0)
                    
                    # 4. L2 NORMALIZATION (Crucial for MTEB!)
                    norm = np.linalg.norm(page_vec)
                    if norm > 0:
                        page_vec = page_vec / norm

            finally:
                page_doc.close()

        return page_vec


def run_vidore_benchmark():
    # 1. Setup the model and retriever
    # If your OpenAIModel requires an API key, ensure it's in your env vars
    retriever = MyVidoreRetriever()

    # 2. Setup the output path
    # Replace the env var with a default if it doesn't exist
    base_path = Path(os.getenv('vidore_results_path'))
    save_path = base_path / "vidore_v3_finance"
    save_path.mkdir(parents=True, exist_ok=True)

    # 3. Select ViDoRe V3 tasks
    # 'Vidore3FinanceRetrieval' covers U.S. Public Company Annual Reports
    tasks = mteb.get_tasks(tasks=["Vidore3FinanceRetrieval"])

    # 4. Initialize MTEB with the selected tasks
    evaluation = mteb.MTEB(tasks=tasks)

    # 5. Run evaluation
    # encode_kwargs passes the batch_size down to your encode_corpus method
    results = evaluation.run(
        retriever, 
        output_folder=Path(save_path),
        batch_size=4,           # MTEB-level query batching
    )

    print(f"Evaluation complete\n\n")


if __name__ == "__main__":
    run_vidore_benchmark()