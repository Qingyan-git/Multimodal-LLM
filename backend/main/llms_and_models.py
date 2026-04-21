from dotenv import load_dotenv
import os
import io
import base64
import asyncio
import tiktoken
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.messages import SystemMessage, HumanMessage
from langchain_experimental.text_splitter import SemanticChunker
from langchain_text_splitters import RecursiveCharacterTextSplitter

from qdrant import format_embeddings


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
            breakpoint_threshold_type="standard_deviation",
            breakpoint_threshold_amount=1.0,
            buffer_size=1
        )

        self.chat_model = ChatOpenAI(
            model=chat_model,
            temperature=0,
            max_tokens=2048,
            timeout=None,
            max_retries=2,
            api_key=api_key,
        )

    
    def semantic_chunker(self,text):

        semantic_chunks = self.text_splitter.split_text(text)

        return semantic_chunks

    
    async def embed_texts(self,chunks):

        texts = []
        token_cost = 0
        money_cost = 0
        for chunk in chunks:
            text = f"""
            Chunk from document : {chunk.document_name}
            Chunk context : {chunk.context}
            Chunk content : {chunk.content}
            """
            texts.append(text)

        # --- TIKTOKEN INTEGRATION ---
        # 1. Initialize the encoding for text-embedding-3 models
        encoding = tiktoken.get_encoding("cl100k_base")
        
        # 2. Count tokens for each string in the list
        tokens = sum(len(encoding.encode(text)) for text in texts)
        
        # 3. Calculate cost ($0.02 per 1,000,000 tokens as of 2026)
        cost = (tokens / 1000000) * 0.02

        vectors = await self.embedding_model.aembed_documents(texts)
        total_cost = [tokens,cost]

        embeddings = format_embeddings(chunks,vectors)

        return (embeddings,total_cost)

    
    async def get_context(self,document,chunk):

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
            SystemMessage(content=system_instructions),
            HumanMessage(content=user_message)
        ]

        response = await self.chat_model.ainvoke(prompt)
        return response.content

    
    async def get_image_description(self,image):
        base64_image = base64.b64encode(image).decode('utf-8')

        system_message = f"""
        ### Role
        You are an expert Vision-to-Text Analyst specialized in preparing data for RAG (Retrieval-Augmented Generation) systems. Your goal is to transform visual information into comprehensive, semantically rich text that can be indexed and retrieved by a vector database.

        ### Task
        You will be provided with an image. Your objective is to generate a detailed, objective summary and description. This text must serve as a proxy for the image in a text-based system, allowing a user to "find" this image by searching for its contents, context, or specific data points.

        ### Output Requirements
        1. **High-Level Summary**: A concise 2-3 sentence overview of what the image represents (e.g., "An infographic showing the 2024 quarterly revenue growth for the APAC region").
        2. **Contextual Analysis**: Identify the setting, subjects, and intent of the image.
        3. **Detail Inventory**: 
        - **Text & OCR**: Extract all visible text, headers, and labels exactly as they appear.
        - **Visual Elements**: Describe charts, diagrams, colors, symbols, or artistic styles.
        - **Entities**: Identify specific people, brands, objects, or locations.
        4. **Relationship Mapping**: Explain how the elements relate (e.g., "The arrow points from the database icon to the cloud icon, indicating a migration process").

        ### Constraints
        - **Be Objective**: Describe what is physically present. Do not invent details or assume hidden meanings.
        - **Search-Friendly Language**: Use descriptive keywords and synonyms that a user might realistically use in a search query.
        - **No Markdown Formatting in Descriptions**: Avoid complex markdown that might interfere with vector embeddings (use plain text or simple bullet points).
        - **Tone**: Professional, analytical, and literal.
        """

        human_message = [
            {
                'type' : 'text',
                'text' : 'Here is the image. Please provide a summary and description of the image for future use in a RAG application'
            },
            {
                'type' : 'image_url',
                "image_url" : {
                    "url": f"data:image/jpeg;base64,{base64_image}",
                    "detail": "high" 
                }
            }
        ]

        prompt = [
            SystemMessage(content=system_message),
            HumanMessage(content=human_message)
        ]
        
        response = await self.chat_model.ainvoke(prompt)

        return response.content


    async def get_query_vector(self,user_query):

        query_vector = await self.embedding_model.aembed_query(user_query)

        return query_vector


    async def answer_question(self,user_query,retrieved_context):

        formatted_contexts = '\n\n'.join(
            [F"Chunk {i}: {content}" for i,content in enumerate(retrieved_context)]
        )

        messages = [
            SystemMessage(content=f"""
            You are a professional assistant. Your goal is to provide accurate answers based ONLY on the provided context chunks.

            RULES:
            1. GROUNDING: If the answer is not contained within the provided database, state clearly that you do not have enough information. Do not use external knowledge.
            2. DATABASE INTEGRATION: Treat the chunks as a unified database repository.
            3. RELEVANCE: If you receive irrelevant chunks, silently ignore any information that does not directly contribute to answering the user's question.
            4. TABLES: If the database contains Markdown tables, interpret row-to-column relationships strictly to ensure data accuracy.
            5. FORMATTING: Use clear headings and bullet points for complex answers.
            6. REASONING: Before answering, identify which specific chunks contain the facts needed for the answer. If multiple chunks provide different pieces of the answer, synthesise them into a single response.                          
            """),

            HumanMessage(content=f"""

            The following database contains multiple labelled chunks from different pages of a document. 
            Use them to answer the question accurately.
            The chunks are ordered by similarity scores to the question, and numbered numerically.
            <database>
            {formatted_contexts}
            </database>

            Question: {user_query}
            """)]

        answer = await self.chat_model.ainvoke(messages)

        return answer.content


class RecursiveSplitter:
    def __init__(self,max_chunk_size=1000,chunk_overlap=200):

        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=max_chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["#","##","###","\n\n", "\n", "."]
        )

        self.max_chunk_size = max_chunk_size

        self.chunk_overlap = chunk_overlap


    def get_max_chunk_size(self):

        return self.max_chunk_size

    def get_chunk_overlap(self):

        return self.chunk_overlap


    def recursive_split(self,text):
        splits = self.splitter.split_text(text)

        return splits


class ContextMessage:
    def __init__(self,chunk):

        metadata_tags = " ".join([f"<{key}>{value}</{key}>" for key, value in chunk['metadata'].items()])

        self.message = f"""
        <data_item>
            <similarity>
                <score>{chunk['score']}</score>
            </similarity>
            <metadata>
                <source>{chunk['document_name']}</source>
                {metadata_tags}
            </metadata>
            <context>
                {chunk['context']}
            </context>
            <content>
                {chunk['content']}
            </content>
        </data_item>
        """

    def get_message(self):
        return self.message





