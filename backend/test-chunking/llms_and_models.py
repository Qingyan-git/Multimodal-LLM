from dotenv import load_dotenv
import os
import io
import base64
import torch
from PIL import Image
import open_clip
from transformers import CLIPModel, CLIPProcessor
from langchain_openai import ChatOpenAI
from langchain.messages import SystemMessage, HumanMessage
from langchain_experimental.text_splitter import SemanticChunker
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

load_dotenv()

class GenerationLLM:

    def __init__(self,model_name='gpt-4o-mini'):

        api_key = os.getenv('openai_api_key')

        self.chat_model = ChatOpenAI(
            model=model_name,
            temperature=0,
            max_completion_tokens=1000,
            timeout=None,
            max_retries=2,
            api_key=api_key,
        )
        
    
    def summarise_text(self,text):

        messages = [
        SystemMessage(
            content=('You are a helpful assistant who summarises the text content that is given to you. '
                    'You are summarising the text to provide context and background information on the content that will be used in a RAG pipeline. '
                    'Use only the text below as your data source, and do not recall any other data from any other sources. '
                    'The context for the data is that they are used by the Singapore Government, hence the context is Singapore based'
                    'The summary should be a detailed and in depth description of the content. '
        )),
        HumanMessage(
            content=text
        )]

        summary = self.chat_model.invoke(messages).content

        return summary


    def caption_image(self,image_PIL):
        buffer = io.BytesIO()
        image_PIL.save(buffer, format="PNG")
        img_str = base64.b64encode(buffer.getvalue()).decode("utf-8")

        messages = [
        SystemMessage(
            content=('You are a helpful assistant who generates an image caption and summary of the content that is given to you. '
                     'You are captioning the image to provide context and background information on the content that will be used in a RAG pipeline. '
                     'Use only the information provided as your data source, and do not recall any other data from any other sources. '
                     'The context for the photos is that they are used by the Singapore Government, hence the context is Singapore based. '
        )),
        HumanMessage(
            content=[
                {"type": "text", "text": "Please caption this image for a RAG database. Please be brief and do not provide a long response. Respond with a maximum of 2 sentences."},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_str}"}}
            ]
        )]

        caption = self.chat_model.invoke(messages).content

        return caption



class EmbeddingModel:
    def __init__(self,model_name='Qwen/Qwen3-VL-Embedding-2B'):

        model_kwargs = {'device': 'cuda', 'trust_remote_code': True}
        encode_kwargs = {'normalize_embeddings': True}

        self.embedder = HuggingFaceEmbeddings(
            model_name=model_name,
            model_kwargs=model_kwargs,
            encode_kwargs=encode_kwargs,
        )


    def semantic_chunker(self,text):

        text_splitter = SemanticChunker(
            self.embedder, 
            breakpoint_threshold_type="percentile"
        )
        
        chunks = text_splitter.split_text(text)

        return chunks



class RecursiveSplitter:
    
    def __init__(self,chunk_size = 2000, chunk_overlap = 200):

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        self.recursive_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            #separators=["\n\n", "\n", "|", " ", ""]
        )

    
    def get_chunk_size(self):
        return self.chunk_size

    
    def split_text(self,text):

        return self.recursive_splitter.split_text(text)



class OpenClipModel:

    def __init__(self,model_id = "openai/clip-vit-large-patch14"):

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Load the model and the processor (which handles both images and text)
        self.model = CLIPModel.from_pretrained(model_id).to(self.device)
        self.processor = CLIPProcessor.from_pretrained(model_id)

        self.labels = [
            "A picture or photograph containing real-world objects or descriptive content", 
            "Background images, Images that do not contain any meaningful content"
        ]


    def classify_images(self,images,batch_size=64):

        results = []

        for i in range(0, len(images), batch_size):
            batch = images[i : i + batch_size]

            inputs = self.processor(
                text=self.labels, 
                images=batch, 
                return_tensors="pt", 
                padding=True
            ).to(self.device)

            with torch.no_grad(), torch.autocast("cuda"):
                outputs = self.model(**inputs)
                
                # Hugging Face models have built-in logit scaling
                # .logits_per_image gives you the similarity scores
                probs = outputs.logits_per_image.softmax(dim=1)

                results.extend(probs.cpu().tolist())

        return results

    def classify_one_image(self,image):

        result = []

        inputs = self.processor(
            text=self.labels, 
            images=image, 
            return_tensors="pt", 
            padding=True
        ).to(self.device)

        with torch.no_grad(), torch.autocast("cuda"):
            output = self.model(**inputs)
            probs = output.logits_per_image.softmax(dim=1)

            positive = probs.cpu().tolist()[0][0]

        return positive