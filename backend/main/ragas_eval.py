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

from llms_and_models import OpenAIModel, ContextMessage
from text_chunking import clean_text, save_to_file
from qdrant import get_similar_chunks


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

    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'),max_tokens=max_tokens)
    generator_llm = llm_factory(client=client,model='gpt-4o-mini')
    embeddings = OpenAIEmbeddings(client=client,model="text-embedding-3-small")

    generator = TestsetGenerator(llm=generator_llm, embedding_model=embeddings)
    dataset = generator.generate_with_langchain_docs(langchain_docs, testset_size=testset_size)

    df = dataset.to_pandas()
    save_path = Path(os.getenv('evaluation_results_path')) / f"{filepath.stem}"
    df.to_csv(path_or_buf=save_path, index=False, encoding='utf-8-sig')

    return df


async def evaluate_testset(df,filename):

    # Single unified approach - works everywhere
    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'),max_tokens=max_tokens)


    """
    Fix max tokens issue for openai API call
    """


    llm = llm_factory(client=client, model="gpt-4o-mini")
    embeddings = OpenAIEmbeddings(client=client,model="text-embedding-3-small")
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

    save_path = Path(os.getenv('evaluation_results_path')) / f"{filename} evaluation results.csv"
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

                print(f'\nEvaluating {file.name}\n')

                testset = generate_testset(file)

                print(f'\tTestset generated\n')

                results_df = await evaluate_testset(testset,file.stem)

                print(f'\tResults calculated\n')

                print(f'\nFinished evaluating {file.name}\n')

            print(f'\nFinished evaluating all files\n\n')

    except Exception as e:
        print(f'Unable to evaluate pdfs, error {e}')
        raise




if __name__ == "__main__":

    evaluation_pdfs_path=Path(os.getenv('evaluation_pdfs_path'))

    asyncio.run(evaluate_pdfs(evaluation_pdfs_path))







# # Define experiment result structure
# class ExperimentResult(BaseModel):
#     faithfulness: float
#     answer_relevancy: float
#     context_precision: float
#     context_recall : float

# # Create experiment function
# @experiment(ExperimentResult)
# async def run_evaluation(row):
#     faithfulness = Faithfulness(llm=llm)
#     answer_relevancy = AnswerRelevancy(llm=llm)
#     context_precision = ContextPrecision(llm=llm)
#     context_recall = ContextRecall(llm=llm)

#     faith_result = await faithfulness.ascore(
#         user_input=row.user_input
#         response=row.response,
#         retrieved_contexts=row.contexts,
#         ground_truth=row.ground_truth
#     )

#     relevancy_result = await answer_relevancy.ascore(
#         user_input=row.user_input
#         response=row.response,
#         retrieved_contexts=row.contexts,
#         ground_truth=row.ground_truth
#     )

#     c_precision_result = await context_precision.ascore(
#         user_input=row.user_input
#         response=row.response,
#         retrieved_contexts=row.contexts,
#         ground_truth=row.ground_truth
#     )

#     c_recall_result = await context_recall.ascore(
#         user_input=row.user_input
#         response=row.response,
#         retrieved_contexts=row.contexts,
#         ground_truth=row.ground_truth
#     )

#     return ExperimentResult(
#         faithfulness=faith_result.value,
#         answer_relevancy=relevancy_result.value
#         context_precision=c_precision_result.value
#         context_recall=c_recall_result.value
#     )

