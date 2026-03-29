from dotenv import load_dotenv
import os
import torch
import gc
from langchain_openai import ChatOpenAI
from langchain.messages import SystemMessage, HumanMessage
from langchain_experimental.text_splitter import SemanticChunker
from langchain_huggingface import HuggingFaceEmbeddings

load_dotenv()

class GenerationLLM:

    def __init__(self,model_name='gpt-5-nano-2025-08-07'):

        api_key = os.getenv('OPENAI_API_KEY')

        self.chat_model = ChatOpenAI(
            model=model_name,
            temperature=0,
            max_completion_tokens=300,
            timeout=None,
            max_retries=2,
            api_key=api_key, #type:ignore
        )
        
    
    def summarise_text(self,text):

        messages = [
        SystemMessage(
            content=('You are a helpful assistant who summarises the text content that is given to you. '
                    'You are summarising the text to provide context and background information on the content that will be used in a RAG pipeline. '
                    'Use only the text below as your data source, and do not recall any other data from any other sources. '
        )),
        HumanMessage(
            content=text
        )]

        summary = self.chat_model.invoke(messages).content

        return summary


class EmbeddingModel:
    def __init__(self,model_name='Qwen/Qwen3-VL-Embedding-2B'):

        gc.collect()
        torch.cuda.empty_cache() 

        model_kwargs = {'device': 'cuda', 'trust_remote_code': True}
        encode_kwargs = {'normalize_embeddings': True}

        self.embedder = HuggingFaceEmbeddings(
            model_name=model_name,
            model_kwargs=model_kwargs,
            encode_kwargs=encode_kwargs,
        )

        gc.collect()
        torch.cuda.empty_cache() 


    def semantic_chunker(self,text):

        text_splitter = SemanticChunker(
            self.embedder, 
            breakpoint_threshold_type="percentile"
        )
        gc.collect()
        torch.cuda.empty_cache() 

        with torch.no_grad():
            chunks = text_splitter.split_text(text)

        gc.collect()
        torch.cuda.empty_cache() 

        return chunks

