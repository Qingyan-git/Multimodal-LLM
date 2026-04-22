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

from ..main.llms_and_models import OpenAIModel, ContextMessage
from ..main.text_chunking import clean_text, save_to_file
from ..main.qdrant import get_similar_chunks


#Copied directly from ragas "latest" docs btw, so any incompatibilities only can blame them already

def generate_testset(filepath, testset_size=5, max_tokens=4096):
    
    page_texts = pymupdf4llm.to_markdown(filepath,page_separators=True,page_chunks=True)

    langchain_docs = []
    for page in page_texts:
        page_text = page['text']
        cleaned_content = clean_text(page_text)
        doc = Document(page_content=cleaned_content,metadata={'source':filepath})
        langchain_docs.append(doc)

    # docs = [Document(page_content=document_markdown,metadata={'source':filepath})]

    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'))
    generator_llm = llm_factory(client=client,model=os.getenv('openai_chat_model'), max_tokens=max_tokens)
    embeddings = OpenAIEmbeddings(client=client,model=os.getenv('openai_embedding_model'))

    generator = TestsetGenerator(llm=generator_llm, embedding_model=embeddings)
    dataset = generator.generate_with_langchain_docs(langchain_docs, testset_size=testset_size)

    df = dataset.to_pandas()
    save_path = Path(os.getenv('ragas_results_path')) / f"{filepath.stem}.csv"
    df.to_csv(path_or_buf=save_path, index=False, encoding='utf-8-sig')

    return df


async def evaluate_testset(df,filename,max_tokens=4096):

    # Single unified approach - works everywhere
    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'))


    """
    Fix max tokens issue for openai API call
    """


    llm = llm_factory(client=client, model=os.getenv('openai_chat_model'), max_tokens=max_tokens)
    embeddings = OpenAIEmbeddings(client=client, model=os.getenv('openai_embedding_model'))
    model = OpenAIModel()

    metrics = [
        Faithfulness(llm=llm),
        AnswerRelevancy(llm=llm,embeddings=embeddings),
        ContextPrecision(llm=llm),
        ContextRecall(llm=llm)
    ]

    eval_tasks = [evaluate_row(df,i,metrics,model) for i in range(len(df))]
    results = await asyncio.gather(*eval_tasks)

    df = pd.DataFrame(results)
    column_order = [
        '--- TESTSET ---', 'user_input', 'reference', 'source',
        '--- LLM ANSWER ---', 'similar_chunks', 'llm_answer',
        '--- METRICS ---', 'faithfulness', 'answer_relevancy', 'context precision', 'context_recall'
    ]
    df = df[column_order]

    save_path = Path(os.getenv('ragas_results_path')) / f"{filename} evaluation results.csv"
    df.to_csv(path_or_buf=save_path,index=False)

    return df


async def evaluate_row(df,idx,metrics,model):
    row = df.iloc[idx]
    user_input = row['user_input']
    reference = row['reference']
    reference_contexts = row['reference_contexts']

    user_input_vector = await model.get_query_vector(user_input)
    similar_chunks = get_similar_chunks(user_input_vector)
    context_messages = [ContextMessage(chunk).get_message() for chunk in similar_chunks]
    answer = await model.answer_question(user_input,context_messages)

    tasks = [
        metrics[0].ascore(user_input=user_input,response=answer,retrieved_contexts=context_messages),
        metrics[1].ascore(user_input=user_input,response=answer),
        metrics[2].ascore(user_input=user_input,retrieved_contexts=context_messages,reference=reference),
        metrics[3].ascore(user_input=user_input,retrieved_contexts=context_messages,reference=reference)
    ]

    results = await asyncio.gather(*tasks)

    item = {
        #From testset
        '--- TESTSET ---': '---',
        'user_input' : user_input,
        'reference' : reference,
        'source' : reference_contexts,

        #LLM answer
        '--- LLM ANSWER ---': '---',
        'similar_chunks' : context_messages,
        'llm_answer' : answer,

        #Metrics
        '--- METRICS ---': '---',
        'faithfulness' : float(results[0].value),
        'answer_relevancy' : float(results[1].value),
        'context precision' : float(results[2].value),
        'context_recall' : float(results[3].value),
    }

    return item


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

    evaluation_pdfs_path=Path(os.getenv('all_pdfs_paths'))
    asyncio.run(evaluate_pdfs(evaluation_pdfs_path))

