# --- LangChain Imports (Used in your OpenAIModel) ---
# from langchain_openai import ChatOpenAI, OpenAIEmbeddings
# from langchain_experimental.text_splitter import SemanticChunker
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.documents import Document
# --- Ragas Test Set Generation ---
from ragas.testset import TestsetGenerator

# --- Ragas Evaluation ---
from ragas import evaluate
from ragas.metrics.collections import (
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
)

from pathlib import Path
import pymupdf4llm
import pymupdf
import os
import pandas as pd

from text_chunking import clean_text
from llms_and_models import OpenAIModel, ContextMessage
from qdrant import get_similar_chunks


def generate_testset(filepath):
    try:
        if filepath.is_file():

            generator_llm,generator_embeddings = OpenAIModel().get_ragas_llms()

            with pymupdf.open(filepath) as doc:
                page_texts = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True,page_chunks=True)

            documents = []
    
            for chunk in page_texts:
                raw_text = chunk["text"]
                cleaned_text = clean_text(raw_text)
                page = Document(
                    page_content=cleaned_text,
                )
                documents.append(page)

            generator = TestsetGenerator(llm=generator_llm, embedding_model=generator_embeddings)
            dataset = generator.generate_with_langchain_docs(documents, testset_size=10)

            df = dataset.to_pandas()
            df.to_csv("ragas_testset.csv", index=False, encoding='utf-8')

            return df

    except Exception as e:
        print(f'Unable to generate testset, error {e}')
        raise


def get_testset_answers(testset_path):

    model = OpenAIModel()

    df = pd.read_csv(testset_path)
    print(f"Info : {df.info()}")
    print(f"Columns : {df.columns}")

    questions = df['user_input'].tolist()
    truths = df['reference'].tolist()

    answers = {
        'question': [],
        'contexts' : [],
        'answer' : [],
        'ground_truth' : []
    }

    for i,question in enumerate(questions):

        query_vector = model.get_query_vector(question)
        similar_chunks = get_similar_chunks(query_vector)

        messages = []
        context_string_for_llm = ""

        for chunk_no,chunk in enumerate(similar_chunks):

            chunk_context = ContextMessage(chunk_no,**chunk).get_message() + '\n'
            context_string_for_llm += chunk_context

            messages.append(chunk['context'])

        answer = model.answer_question(question,context_string_for_llm)
        
        answers['question'].append(question)
        answers['contexts'].append(messages)
        answers['answer'].append(answer)
        answers['ground_truth'].append(truths[i])

    return answers








if __name__ == '__main__':
    path = Path(r"C:\Users\Chu Qingyan\Documents\WFH\Multimodal-LLM\data\text-pdfs\government-data-security-policies.pdf")
    testset = generate_testset(path)

    testset_path = Path(r"C:\Users\Chu Qingyan\Documents\WFH\Multimodal-LLM\backend\test-chunking\testsets\ragas_testset.csv")
    answer_questions(testset_path)