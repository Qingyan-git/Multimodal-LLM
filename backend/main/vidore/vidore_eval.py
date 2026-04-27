
import os
import numpy as np
from PIL import Image
from typing import List, Dict, Any
import tempfile
import sys
from pathlib import Path
import asyncio

import mteb
from mteb.types import PromptType, Array
from mteb.models import ModelMeta, EncoderProtocol

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to 
# the sys.path.
sys.path.append(parent)

# now we can import the module in the parent
# directory.
from llms_and_models import OpenAIModel, SparseEmbedder, ColBERTEmbedder
from text_chunking import extract_text
from docling_image_chunking import extract_images
from docling_table_chunk import extract_tables
from page_chunking import extract_pages, convert_PIL_to_pdf


"""
Havent tested ViDoRe testsuite yet, as well as generated testsuite
"""


class MyCustomEncoder(EncoderProtocol):
    """
    Standard MTEB interface for Multimodal Retrieval.
    """

    def __init__(self):
        # Initialize your local model (e.g., Qwen3-VL) or API client
        self.model = OpenAIModel()
        self.sparse = SparseEmbedder()
        self.late = ColBERTEmbedder()


    @property
    def mteb_model_meta(self) -> ModelMeta:
        return ModelMeta(
            name="local/MyTripleHybridEncoder",
            revision="0.1.0",
            release_date="2026-04-27",
            languages=["eng"], 
            modalities=["text", "image"],
            # These fields are MANDATORY for Pydantic validation
            loader=None,
            n_parameters=None,
            memory_usage_mb=None,
            max_tokens=None, # Changed from 0 to None for safety
            embed_dim=None, 
            license=None,
            open_weights=False,
            public_training_code=None,
            public_training_data=None,
            framework=[], 
            similarity_fn_name="cosine",
            use_instructions=False,
            training_datasets=None 
        )


    def encode(
        self,
        inputs: None,
        task_metadata: None,
        hf_split: str,
        hf_subset: str,
        prompt_type: PromptType | None = None,
        **kwargs,
    ) -> Array:
        """Encodes the given sentences using the encoder.

        Args:
            inputs: The inputs to encode.
            task_metadata: The name of the task.
            hf_subset: The subset of the dataset.
            hf_split: The split of the dataset.
            prompt_type: The prompt type to use.
            **kwargs: Additional arguments to pass to the encoder.

        Returns:
            The encoded sentences.
        """

        print([item for item in inputs])


        """
        Doesn't work because ViDoRe requires some further logic for their dataset structure
        """



        if hasattr(inputs, "__iter__") and not isinstance(inputs, list):
            data_to_encode = []
            for batch in inputs:
                if isinstance(batch, list):
                    data_to_encode.extend(batch)
                else:
                    data_to_encode.append(batch)
        else:
            data_to_encode = inputs

        if isinstance(data_to_encode[0], str):
            # Process as Queries
            return asyncio.run(self._async_encode_queries(data_to_encode))
        else:
            # Process as Corpus (ViDoRe gives [{'image': PIL, 'text': str}, ...])
            return asyncio.run(self._async_encode_corpus(data_to_encode))


    async def _async_encode_queries(self,queries,batch_size=16):

        embeddings, _ = await self.model.embed_texts(queries)
        dense_m = np.array([e.vector for e in embeddings])
        sparse_m = np.array(self.sparse.embed_texts(queries))
        late_m = np.array(self.late.embed_texts(queries))

        dense_m /= (np.linalg.norm(dense_m, axis=1, keepdims=True) + 1e-9)
        sparse_m /= (np.linalg.norm(sparse_m, axis=1, keepdims=True) + 1e-9)
        late_m /= (np.linalg.norm(late_m, axis=1, keepdims=True) + 1e-9)

        hybrid_matrix = np.concatenate([dense_m, sparse_m, late_m], axis=1)

        return hybrid_matrix


    async def _async_encode_corpus(self, corpus, batch_size=16):

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

            try:
                all_chunks = []
                all_chunks.extend(await extract_text(temp_path))
                all_chunks.extend(await extract_images(temp_path))
                all_chunks.extend(await extract_tables(temp_path))
                all_chunks.extend(await extract_pages(temp_path))

                embeddings, cost = await self.model.embed_texts(all_chunks)
                dense_embeddings = [e.vector for e in embeddings]
                sparse_embeddings = self.sparse.embed_texts(all_chunks)
                late_embeddings = self.late.embed_texts(all_chunks)

                # 2. Mean Pool each representation individually
                dense_vec = np.mean(dense_embeddings, axis=0)
                sparse_vec = np.mean(sparse_embeddings, axis=0)
                late_vec = np.mean(late_embeddings, axis=0)

                # 3. L2 Normalize each individually before combining
                dense_vec /= np.linalg.norm(dense_vec)
                sparse_vec /= np.linalg.norm(sparse_vec)
                late_vec /= np.linalg.norm(late_vec)

                # 4. Concatenate into a single hybrid representation
                hybrid_vec = np.concatenate([dense_vec, sparse_vec, late_vec])

            finally:
                page_doc.close()

        return hybrid_vec


def run_vidore_benchmark():
    # 1. Setup the model and retriever
    retriever = MyCustomEncoder()
    task = 'Vidore3FinanceEnRetrieval.v2'

    # 2. Setup the output path
    base_path = Path(os.getenv('vidore_results_path'))
    save_path = base_path / task
    save_path.mkdir(parents=True, exist_ok=True)

    # 3. Select ViDoRe V3 tasks
    tasks = mteb.get_tasks(tasks=[task])
    cache = mteb.ResultCache(cache_path=save_path)
    results = mteb.evaluate(model=retriever, tasks=tasks, cache=cache)

    print(f"Evaluation complete\n\n")

if __name__ == "__main__":
    run_vidore_benchmark()