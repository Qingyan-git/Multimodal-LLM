from langchain_core.documents import Document
from ragas.llms import LangchainLLMWrapper
from langchain_openai import ChatOpenAI
from ragas.embeddings import OpenAIEmbeddings
import openai
from ragas.testset import TestsetGenerator
import pymupdf
import pymupdf4llm
from ragas import experiment
from ragas.metrics.collections import Faithfulness,AnswerRelevancy,ContextPrecision,ContextRecall
from pydantic import BaseModel
from ragas.llms import llm_factory
from openai import AsyncOpenAI
import os
import pandas as pd
from pathlib import Path
import asyncio
import sys


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

from llms_and_models import OpenAIModel, ContextMessage, SparseEmbedder, ColBERTEmbedder
from text_chunking import clean_text, save_to_file
from qdrant import get_similar_chunks






"""
Haven't tested the new implementation yet
"""




def generate_testset(filepath, testset_size=5, max_tokens=4096):
    
    # page_texts = pymupdf4llm.to_markdown(filepath,page_separators=True,page_chunks=True)

    # langchain_docs = []
    # for page in page_texts:
    #     page_text = page['text']
    #     cleaned_content = clean_text(page_text)
    #     doc = Document(page_content=cleaned_content,metadata={'source':filepath})
    #     langchain_docs.append(doc)

    full_text = pymupdf4llm.to_markdown(filepath)
    cleaned_content = clean_text(full_text)
    
    # If the doc is short, one Document is actually better for Ragas 
    # as it sees all the 'Must-Knows' at once.
    langchain_docs = [Document(page_content=cleaned_content, metadata={'source': str(filepath)})]

    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'))
    generator_llm = llm_factory(client=client,model=os.getenv('openai_chat_model'), max_tokens=max_tokens)
    embeddings = OpenAIEmbeddings(client=client,model=os.getenv('openai_embedding_model'))

    generator = TestsetGenerator(llm=generator_llm, embedding_model=embeddings)
    dataset = generator.generate_with_langchain_docs(langchain_docs, testset_size=testset_size)

    df = dataset.to_pandas()
    save_path = Path(os.getenv('ragas_results_path')) / f"{filepath.stem}.csv"
    df.to_csv(path_or_buf=save_path, index=False, encoding='utf-8-sig')

    return df


async def evaluate_testset(testset,filename,max_tokens=4096):

    # Single unified approach - works everywhere
    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'))

    llm = llm_factory(client=client, model=os.getenv('openai_chat_model'), max_tokens=max_tokens)
    embeddings = OpenAIEmbeddings(client=client, model=os.getenv('openai_embedding_model'))
    model = OpenAIModel()
    sparse_embedder = SparseEmbedder()
    late_embedder = ColBERTEmbedder()

    metrics = [
        Faithfulness(llm=llm),
        AnswerRelevancy(llm=llm,embeddings=embeddings),
        ContextPrecision(llm=llm),
        ContextRecall(llm=llm)
    ]

    results = []
    for index, row in testset.iterrows():
        user_input = row['user_input']
        reference = row['reference']
        reference_contexts = row['reference_contexts']

        dense_vector = await model.get_query_vector(user_input)
        sparse_vector = sparse_embedder.embed_query(user_input)
        late_vector = late_embedder.embed_query(user_input)

        similar_chunks = get_similar_chunks(dense_vector,sparse_vector,late_vector)
        chunk_messages = [ContextMessage(chunk).get_message() for chunk in similar_chunks]

        answer = await model.answer_question(user_input,chunk_messages)

        tasks = [
            metrics[0].ascore(user_input=user_input,response=answer,retrieved_contexts=chunk_messages),
            metrics[1].ascore(user_input=user_input,response=answer),
            metrics[2].ascore(user_input=user_input,retrieved_contexts=chunk_messages,reference=reference),
            metrics[3].ascore(user_input=user_input,retrieved_contexts=chunk_messages,reference=reference)
        ]
        result = await asyncio.gather(*tasks)

        item = {
            #From testset
            '--- TESTSET ---': '---',
            'user_input' : user_input,
            'reference' : reference,
            'source' : reference_contexts,

            #LLM answer
            '--- LLM ANSWER ---': '---',
            'chunk_messages' : chunk_messages,
            'llm_answer' : answer,

            #Metrics
            '--- METRICS ---': '---',
            'faithfulness' : float(result[0].value),
            'answer_relevancy' : float(result[1].value),
            'context precision' : float(result[2].value),
            'context_recall' : float(result[3].value),
        }

        results.append(item)

    df = pd.DataFrame(results)
    column_order = [
        '--- TESTSET ---', 'user_input', 'reference', 'source',
        '--- LLM ANSWER ---', 'chunk_messages', 'llm_answer',
        '--- METRICS ---', 'faithfulness', 'answer_relevancy', 'context precision', 'context_recall'
    ]
    df = df[column_order]

    save_path = Path(os.getenv('ragas_results_path')) / f"{filename} evaluation results.csv"
    df.to_csv(path_or_buf=save_path,index=False)

    return df


async def evaluate_pdfs(folder_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                try:
                    # Attempt to generate the testset
                    testset = generate_testset(file)
                    
                    if testset is None or len(testset) == 0:
                        print(f'\t[SKIP] Testset generation failed or returned empty for {file.name}\n')
                        continue

                    print(f'\tTestset generated successfully.')

                    # Evaluate the generated testset
                    results_df = await evaluate_testset(testset, file.stem)
                    print(f'\tResults calculated and saved.\n')

                except Exception as file_error:
                    # Catch and log the error, then skip to the next file
                    print(f'\t[ERROR] Failed to process {file.name}: {file_error}')
                    print(f'\tSkipping to next file...\n')
                    continue

                print(f'\nFinished evaluating {file.name}\n')

            print(f'\nFinished evaluating all files\n\n')

    except Exception as e:
        print(f'Unable to evaluate pdfs, error {e}')
        raise




if __name__ == "__main__":

    evaluation_pdfs_path=Path(os.getenv('all_pdfs_path'))
    asyncio.run(evaluate_pdfs(evaluation_pdfs_path))

