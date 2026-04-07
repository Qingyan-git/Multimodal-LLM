from dotenv import load_dotenv
import os
import io
import base64
import torch
from PIL import Image
import open_clip
from transformers import CLIPModel, CLIPProcessor
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.messages import SystemMessage, HumanMessage
from langchain_experimental.text_splitter import SemanticChunker


class OpenAIModel:

    def __init__(self,chat_model='gpt-4o-mini',embedding_model='text-embedding-3-small'):

        api_key = os.getenv('openai_api_key')

        self.embedding_model = OpenAIEmbeddings(
            model=embedding_model,
            dimensions=256,
            api_key=api_key
        )

        self.text_splitter = SemanticChunker(
            self.embedding_model, 
            breakpoint_threshold_type="percentile",
            breakpoint_threshold_amount=85
        )

        self.chat_model = ChatOpenAI(
            model=chat_model,
            temperature=0,
            max_tokens=1000,
            timeout=None,
            max_retries=2,
            api_key=api_key,
        )


    def get_chat_model(self):
        return self.chat_model


    def get_embedding_model(self):
        return self.embedding_model


    def get_text_splitter(self):
        return self.text_splitter

    
    def semantic_chunker(self,text):

        semantic_chunks = self.text_splitter.split_text(text)

        return semantic_chunks

    
    def embed_texts(self,texts):

        vectors = self.embedding_model.embed_documents(texts)

        return vectors

    
    def get_context(self,document,chunk):

        system_instructions = f"You are an AI assistant specialising in document analysis. Your task is to provide brief, relevant context for a chunk of text from the given document."
        user_message = f"""
        Here is the main document:
        <document>
        {document}
        </document>

        Here is the chunk we want to situate within the whole document:
        <chunk>
        {chunk}
        </chunk>

        Provide a concise context (2-3 sentences) for this chunk, considering the following guidelines:
        1. Identify the main topic or concept discussed in the chunk.
        2. Mention any relevant information or comparisons from the broader document context.
        3. If applicable, note how this information relates to the overall theme or purpose of the document.
        4. Include any key figures, dates, or percentages that provide important context.
        5. Do not use phrases like "This chunk discusses" or "This section provides". Instead, directly state the context.

        Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk. Answer only with the succinct context and nothing else.
        """

        prompt = [
            {"role": "system", "content": system_instructions},
            {"role": "user", "content": user_message}
        ]

        response = self.chat_model.invoke(prompt).content
        return response


    def get_query_vector(self,user_query):

        query_vector = self.embedding_model.embed_query(user_query)

        return query_vector


    def answer_question(self,user_query,retrieved_context):

        messages = [
            SystemMessage(content=f"""
            You are a professional assistant. Your goal is to provide accurate answers based ONLY on the provided context chunks.

            RULES:
            1. GROUNDING: If the answer is not contained within the provided context, state clearly that you do not have enough information. Do not use external knowledge.
            2. CONTEXT INTEGRATION: Treat the chunks as a single unified knowledge base.
            3. RELEVANCE: Only use information from chunks that are relevant to answering the question.  
            4. TABLES: If context contains Markdown tables, interpret row-to-column relationships strictly to ensure data accuracy.
            5. FORMATTING: Use clear headings and bullet points for complex answers.                          
            """),

            HumanMessage(content=f"""

            The following context contains multiple labeled chunks from different pages of a document. 
            Use them to answer the question accurately.

            Context: 
            {retrieved_context}

            Question: {user_query}
            """)]

        answer = self.chat_model.invoke(messages).content

        return answer



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



class OpenClipModel:

    def __init__(self,model_id = "openai/clip-vit-large-patch14"):

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
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



class ContextMessage:
    def __init__(self,chunk):

        self.message = f"""
        This background context is taken from document {chunk['document_name']}, page(s) {chunk['pages']}
        This chunk has a similarity score of {chunk['score']}
        Information about the chunk's content : {chunk['context']}
        The chunk's contents itself : {chunk['content']}
        """


    def get_message(self):
        return self.message


