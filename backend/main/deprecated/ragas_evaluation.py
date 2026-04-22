
from ragas.testset import TestsetGenerator
from ragas.testset.synthesizers import default_query_distribution
from ragas.testset.transforms import default_transforms, apply_transforms
from ragas.llms import llm_factory
from ragas.embeddings.base import embedding_factory
from langchain_openai import OpenAIEmbeddings
from openai import AsyncOpenAI

from ragas import experiment, EvaluationDataset
from ragas.metrics.collections import Faithfulness, AnswerRelevancy, ContextPrecision, ContextRecall

from langchain_core.documents import Document

from pathlib import Path
import pymupdf4llm
import pymupdf
import os
import dotenv
import asyncio
import pandas as pd
from dataclasses import dataclass
import traceback

from text_chunking import clean_text
from llms_and_models import OpenAIModel, ContextMessage
from qdrant import get_similar_chunks

dotenv.load_dotenv()


def get_llms():
    client = AsyncOpenAI(api_key=os.getenv('openai_api_key'))
    generator_llm = llm_factory(
        model="gpt-4o-mini", 
        client=client,
        # Custom requirements
        temperature=0,
        max_tokens=2048,
        timeout=None,
        max_retries=2,
    )
    embedding_model = embedding_factory(
            'openai', 
            model='text-embedding-3-small', 
            client=client,
            interface='modern'
            # Custom requirements
            # dimensions=256
        )

    return (generator_llm,embedding_model)


def save_to_csv(df,folder_path,filename,generate=True):

    if generate:
        stem = f"{filename}_testset.csv"
    else:
        stem = f"{filename}_results.csv"

    folder_path.mkdir(parents=True,exist_ok=True)
    save_path = folder_path / stem
    df.to_csv(save_path, index=False, encoding='utf-8')

    return save_path


def generate_testset(filepath, output_path,testset_size=10):
    try:
        if filepath.is_file():

            generator_llm,embedding_model = get_llms()

            with pymupdf.open(filepath) as doc:
                page_texts = pymupdf4llm.to_markdown(
                    doc, 
                    header=False, 
                    footer=False, 
                    page_separators=True, 
                    page_chunks=True
                )

            langchain_docs = []
            for page in page_texts:
                page_text = page['text']
                cleaned_content = clean_text(page_text)
                doc = Document(page_content=cleaned_content)
                langchain_docs.append(doc)

            generator = TestsetGenerator(
                llm=generator_llm, 
                embedding_model=embedding_model
            )

            query_distribution = default_query_distribution(generator_llm)

            dataset = generator.generate_with_langchain_docs(
                documents=langchain_docs, 
                testset_size=testset_size,
                query_distribution=query_distribution
            )

            df = dataset.to_pandas()
            save_path = save_to_csv(df, output_path, filepath.stem, generate=True)

            return save_path

    except Exception as e:
        print(f'Unable to generate testset, error: {e}')
        traceback.print_exc() 
        raise


async def evaluate_testset(testset_path):

    try:

        model = OpenAIModel()

        generator_llm,embedding_model = get_llms()

        df = pd.read_csv(testset_path)
        
        metrics = [
            Faithfulness(llm=generator_llm),
            AnswerRelevancy(llm=generator_llm, embeddings=embedding_model),
            ContextPrecision(llm=generator_llm),
            ContextRecall(llm=generator_llm)
        ]

        async def evaluate_row(row_idx):

            row = df.iloc[row_idx]
            question = row['user_input']
            ground_truth = row['reference']

            query_vector = await model.get_query_vector(question)
            similar_chunks = get_similar_chunks(query_vector)
            
            supporting_information = [ContextMessage(chunk).get_message() for chunk in similar_chunks]
            supporting_string = "\n".join(supporting_information)
            
            answer = await model.answer_question(question, supporting_string)

            tasks = [
                metrics[0].ascore(user_input=question, response=answer, retrieved_contexts=supporting_information),
                metrics[1].ascore(user_input=question, response=answer),
                metrics[2].ascore(user_input=question, retrieved_contexts=supporting_information, reference=ground_truth),
                metrics[3].ascore(user_input=question, retrieved_contexts=supporting_information, reference=ground_truth)
            ]
            
            scores = await asyncio.gather(*tasks)
            
            return {
                "user_input": question,
                "answer" : answer,
                "ground_truth" : ground_truth,
                "faithfulness": float(scores[0].value),
                "answer_relevancy": float(scores[1].value),
                "context_precision": float(scores[2].value),
                "context_recall": float(scores[3].value),
            }

        eval_tasks = [evaluate_row(i) for i in range(len(df))]
        final_results = await asyncio.gather(*eval_tasks)

        results_df = pd.DataFrame(final_results)
        save_to_csv(results_df, testset_path.parent, testset_path.stem, generate=False)

        return results_df

    except Exception as e:
        print(f'Unable to evaluate testset, error {e}')
        traceback.print_exc()
        raise


async def evaluate_pdfs(folder_path,testset_path):
    try:
        if folder_path.is_dir():
            for file in folder_path.iterdir():

                print(f'\nEvaluating {file.name}\n')

                save_path = generate_testset(file,testset_path)

                print(f'\tTestset generated\n')

                results_df = await evaluate_testset(save_path)

                print(f'\tResults calculated\n')

                print(f'\nFinished evaluating {file.name}\n')

            print(f'\nFinished evaluating all files\n\n')

    except Exception as e:
        print(f'Unable to evaluate pdfs, error {e}')
        traceback.print_exc() 
        raise




if __name__ == '__main__':

    print(f'Running evaluation\n\n\n')
    pdfs_path = Path(os.getenv('text_pdfs_path'))
    testset_path = Path(os.getenv('testset_path'))
    asyncio.run(evaluate_pdfs(pdfs_path,testset_path))