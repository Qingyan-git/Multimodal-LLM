
from ragas.testset import TestsetGenerator
from ragas.testset.graph import KnowledgeGraph, Node
from ragas.testset.synthesizers import default_query_distribution
from ragas.testset.transforms import default_transforms, apply_transforms

from ragas import experiment, EvaluationDataset
from ragas.metrics.collections import Faithfulness, AnswerRelevancy, ContextPrecision, ContextRecall

from pathlib import Path
import pymupdf4llm
import pymupdf
import os
import dotenv
import asyncio
import pandas as pd
from dataclasses import dataclass

from text_chunking import clean_text
from llms_and_models import OpenAIModel, ContextMessage
from qdrant import get_similar_chunks

dotenv.load_dotenv()


def save_to_csv(df,folder_path,filename,generate=True):

    if generate:
        stem = f"{filename}_testset.csv"
    else:
        stem = f"{filename}_results.csv"

    folder_path.mkdir(parents=True,exist_ok=True)
    save_path = folder_path / stem
    df.to_csv(save_path, index=False, encoding='utf-8')

    return save_path


def generate_testset(filepath, output_path):
    try:
        if filepath.is_file():

            model = OpenAIModel()
            generator_llm = model.get_chat_model()
            embedding_model = model.get_embedding_model()

            with pymupdf.open(filepath) as doc:
                page_texts = pymupdf4llm.to_markdown(
                    doc, 
                    header=False, 
                    footer=False, 
                    page_separators=True, 
                    page_chunks=True
                )

            nodes = []
            for chunk in page_texts:
                cleaned_content = clean_text(chunk["text"])
                if cleaned_content:
                    node = Node(
                        properties={"page_content" : cleaned_content}
                    )
                    nodes.append(node)

            """
            ragas knowledge graph node have some issue
            """

            kg = KnowledgeGraph(nodes=nodes)

            transform = default_transforms(documents=nodes, llm=generator_llm, embedding_model=embedding_model)
            apply_transforms(kg, transform)

            generator = TestsetGenerator(
                llm=generator_llm, 
                embedding_model=embedding_model,
                knowledge_graph=kg,
            )

            query_distribution = default_query_distribution(generator_llm)
            dataset = generator.generate(testset_size=10,query_distribution=query_distribution)

            df = dataset.to_pandas()
            save_path = save_to_csv(df, output_path, filepath.stem, generate=True)

            return save_path

    except Exception as e:
        print(f'Unable to generate testset, error: {e}')
        raise



async def evaluate_testset(testset_path):

    try:
        semaphore = asyncio.Semaphore(5)

        model = OpenAIModel()
        generator_llm = model.get_chat_model()
        embedding_model = model.get_embedding_model()

        df = pd.read_csv(testset_path)
        
        metrics = [
            Faithfulness(llm=ragas_llm),
            AnswerRelevancy(llm=ragas_llm, embeddings=ragas_embedding),
            ContextPrecision(llm=ragas_llm),
            ContextRecall(llm=ragas_llm)
        ]

        async def evaluate_row(row_idx):

            row = df.iloc[row_idx]
            question = row['user_input']
            ground_truth = row['reference']

            query_vector = model.get_query_vector(question)
            similar_chunks = get_similar_chunks(query_vector, limit=3)
            
            contexts = [ContextMessage(chunk).get_message() for chunk in similar_chunks]
            context_string = "\n".join(contexts)
            
            answer = model.answer_question(question, context_string)

            tasks = [
                metrics[0].ascore(response=answer, retrieved_contexts=contexts),
                metrics[1].ascore(user_input=question, response=answer),
                metrics[2].ascore(user_input=question, retrieved_contexts=contexts, reference=ground_truth),
                metrics[3].ascore(user_input=question, retrieved_contexts=contexts, reference=ground_truth)
            ]
            
            scores = await asyncio.gather(*tasks)
            
            return {
                "question": question,
                "answer": answer,
                "contexts": contexts,
                "ground_truth": ground_truth,
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


@dataclass 
class ExperimentResult():
    faithfulness: float
    answer_relevancy: float
    context_precision: float
    context_recall : float


@experiment(ExperimentResult)
async def run_evaluation(row):
    m_faith = Faithfulness(llm=ragas_llm)
    m_relevancy = AnswerRelevancy(llm=ragas_llm)
    m_precision = ContextPrecision(llm=ragas_llm)
    m_recall = ContextRecall(llm=ragas_llm)

    # Faithfulness: Needs Response + Contexts
    faith_res = await m_faith.ascore(
        response=row.response, 
        retrieved_contexts=row.contexts
    )

    # Relevancy: Needs User Input + Response
    rel_res = await m_relevancy.ascore(
        user_input=row.user_input, 
        response=row.response
    )

    # Precision: Needs User Input + Contexts + Reference (Ground Truth)
    prec_res = await m_precision.ascore(
        user_input=row.user_input,
        retrieved_contexts=row.contexts,
        reference=row.reference
    )

    # Recall: Needs User Input + Contexts + Reference
    recall_res = await m_recall.ascore(
        user_input=row.user_input,
        retrieved_contexts=row.contexts,
        reference=row.reference
    )

    return ExperimentResult(
        faithfulness=faith_res,
        answer_relevancy=rel_res,
        context_precision=prec_res,
        context_recall=recall_res
    )



if __name__ == '__main__':
    filepath = Path(os.getenv('generate_testset_path'))
    testset_path = Path(os.getenv('testset_path'))

    save_path = generate_testset(filepath,testset_path)
    results_df = evaluate_testset(save_path)