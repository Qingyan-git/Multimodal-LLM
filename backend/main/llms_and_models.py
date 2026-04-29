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
from fastembed import SparseTextEmbedding, LateInteractionTextEmbedding

load_dotenv()

class OpenAIModel:

    def __init__(self,chat_model=os.getenv('openai_chat_model'),embedding_model=os.getenv('openai_embedding_model')):

        api_key = os.getenv('openai_api_key')

        self.embedding_model = OpenAIEmbeddings(
            model=embedding_model,
            dimensions=1536,
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

    
    async def embed_texts(self,chunks,cost_per_million=0.02):

        texts = []
        token_cost = 0
        money_cost = 0
        for chunk in chunks:
            text = f"""
            Chunk from document : {chunk.document_name}
            Chunk context : {chunk.context}
            Chunk content : {chunk.content}
            Chunk metadata : {chunk.metadata}
            """
            texts.append(text)

        # --- TIKTOKEN INTEGRATION ---
        # 1. Initialize the encoding for text-embedding-3 models
        encoding = tiktoken.get_encoding("cl100k_base")
        
        # 2. Count tokens for each string in the list
        tokens = sum(len(encoding.encode(text)) for text in texts)
        
        # 3. Calculate cost ($0.02 per 1,000,000 tokens as of 2026)
        cost = (tokens / 1000000) * cost_per_million

        vectors = await self.embedding_model.aembed_documents(texts)
        total_cost = [tokens,cost]

        return (vectors,total_cost)

    
    async def get_context(self,document,chunk):

        system_instructions = (
            "You are a specialized Document Indexing Engine. Your sole purpose is to provide "
            "semantic context for text chunks to improve their retrieval performance.\n\n"
            
            "OPERATIONAL MANDATES:\n"
            "1. NO META-DISCOURSE: You must never acknowledge the user or use phrases like 'Sure, here is the context' or 'This chunk is about'.\n"
            "2. STRICT BREVITY: Output must be exactly 2-3 declarative sentences.\n"
            "3. SEMANTIC ENRICHMENT: You must inject critical global context (document title, author, date, or primary objective) into the description so the chunk becomes self-contained.\n"
            "4. NO HALLUCINATION: Only use information explicitly present in the <document> tag.\n"
            "5. OUTPUT FORMAT: Return the succinct context as plain text. Do not use Markdown, headers, or tags."
        )

        user_message = user_message = f"""
        [TASK]
        Situate the provided <chunk> within the <document> for a vector search index.

        [DOCUMENT]
        {document}

        [CHUNK TO ENRICH]
        {chunk}

        [INSTRUCTIONS]
        Write a 2-3 sentence situational prefix. 
        - Sentence 1: Identify the document source and its high-level purpose.
        - Sentence 2: Explain the specific role of this chunk within that purpose.
        - Sentence 3: Include any necessary keywords from the document (dates, entities) that are absent from the chunk.

        [OUTPUT]
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
            1. GROUNDING: Answer ONLY using the provided chunks. If the information is missing, state: "I do not have enough information in the provided documents."
            2. DATABASE INTEGRATION: Treat the chunks as a unified repository.
            3. CITATIONS: Every answer must conclude with a "Sources Used" section. 
            - Identify the document name and page number provided in the context headers.
            - List them in a separate paragraph at the end of your response.
            - If multiple sources were used, list them as a comma-separated list.
            4. RELEVANCE: Ignore irrelevant chunks. Do not mention them.
            5. TABLES: Interpret Markdown tables strictly by mapping row-to-column relationships.
            6. FORMATTING: Use bold headings and bullet points for readability.
            7. REASONING: First, scan the chunks for facts; then, synthesize them into a coherent response.

            OUTPUT STRUCTURE:
            [Your detailed answer here]

            **Sources Used:** [Document Name], [Page Number]
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


    async def answer_questions_images(self,user_query,images):
        pass

        # 1. Construct the Image blocks for the Human Message
        image_content = []
        
        for item in images:
            # Add the text label so the LLM knows which image is which
            image_content.append({
                "type": "text", 
                "text": f"--- START OF IMAGE ({item['source']}) ---"
            })
            # Add the actual image
            image_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{item['image_data']}",
                    "detail": "high" # Ensures the LLM looks at the high-res version for small text
                }
            })

        # 2. Add the final user question
        image_content.append({
            "type": "text",
            "text": f"\nUser Query: {user_query}"
        })

        messages = [
            SystemMessage(content=f"""
            You are a specialized Document Analysis Assistant. You will be provided with several images of document pages.
            
            YOUR TASKS:
            1. Analyze the provided images (including text, tables, and charts) to answer the user's query.
            2. For every fact you state, you MUST cite the source (Document Name and Page Number) provided in the label above the image.
            3. If the answer is found in a chart or table, describe the visual evidence (e.g., 'As shown in the bar chart on Page 5...').
            4. If the images do not contain enough information to answer the question accurately, state that you cannot find the answer in the provided pages.
            
            FORMATTING:
            - Use clear, professional language.
            - Use bullet points for complex data.
            - Always use the citation format: [Document Name, Page X].
            """),
            HumanMessage(content=image_content)
        ]

        response = await self.chat_model.ainvoke(messages)
        return response.content


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



class SparseEmbedder:

    def __init__(self, model_name=os.getenv('sparse_embedding_model'), use_threads=8):

        self.model = SparseTextEmbedding(model_name=model_name, parallel=use_threads)

    
    def embed_texts(self, chunks):

        formatted_chunks = []
        for chunk in chunks:
            message = f"Context : {chunk.context} | Content : {chunk.content}"
            formatted_chunks.append(message)
        
        # Returns a list of sparse vectors for ingestion
        embeddings = list(self.model.embed(formatted_chunks))
        return [
            {"indices": e.indices.tolist(), "values": e.values.tolist()} 
            for e in embeddings
            ]


    def embed_query(self, text):

        # Helper for single query strings
        embedding = list(self.model.embed([text]))[0]
        return {
            "indices": embedding.indices.tolist(), 
            "values": embedding.values.tolist()
        }



class ColBERTEmbedder:

    def __init__(self, model_name=os.getenv('late_interaction_embedding_model'), use_threads=8):

        self.model = LateInteractionTextEmbedding(model_name, parallel=use_threads)


    def embed_texts(self, chunks):

        formatted_chunks = []
        for chunk in chunks:
            message = f"Context : {chunk.context} | Content : {chunk.content}"
            formatted_chunks.append(message)
        # FastEmbed returns a generator, we convert to a list of ndarrays
        # Qdrant accepts these numpy arrays directly
        return list(self.model.embed(formatted_chunks))


    def embed_query(self, query_text):

        # Use .query_embed for search queries
        # returns a generator, so we take the first item [0]
        return list(self.model.query_embed(query_text))[0]
