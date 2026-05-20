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
            api_key=api_key,
            chunk_size=16
        )

        self.text_splitter = SemanticChunker(
            self.embedding_model, 
            breakpoint_threshold_type="interquartile",
            breakpoint_threshold_amount=0.5,
            buffer_size=2
        )

        self.chat_model = ChatOpenAI(
            model=chat_model,
            temperature=0,
            max_tokens=2048,
            timeout=None,
            max_retries=2,
            api_key=api_key,
            reasoning_effort="minimal"
        )

    
    def semantic_chunker(self,text):

        semantic_chunks = self.text_splitter.split_text(text)

        return semantic_chunks

    
    async def embed_texts(self,chunks,cost_per_million=0.02):

        texts = []
        token_cost = 0
        money_cost = 0
        for chunk in chunks:
            text = format_chunk(chunk)
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

        user_message = f"""
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

        system_message = """
        You are an expert data and visual analyst. Your task is to analyze the provided image, which is primarily a chart, graph, table, or process diagram.

        Provide a comprehensive breakdown of the image by covering the following:
        1. **Overview**: State what kind of visual it is (e.g., line graph, flowchart, bar chart) and its main title or core subject.
        2. **Structure & Data Extraction**: Detail the components. For charts/graphs, identify the axes, legends, labels, and key data points or metrics. For diagrams/flowcharts, describe the steps, directional flow, and connections.
        3. **Analysis & Insights**: Analyze what the data or diagram is showing. Highlight prominent trends, significant spikes, anomalies, or the ultimate conclusion of the process.

        Keep your description objective, analytical, and literal. Base your analysis strictly on the visible information presented in the image.
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

        formatted_contexts = '\n\n'.join(retrieved_context)

        messages = [
            SystemMessage(content="""
            You are a professional assistant. Your goal is to provide accurate answers based ONLY on the provided context items.

            RULES:
            1. GROUNDING & FALLBACK: 
            - Try your best to answer the user's question directly using the facts found inside the <content> tags of the provided <data_item> elements.
            - FALLBACK: If you cannot find a direct answer, cannot glean a deeper meaning, or the exact information requested is missing, DO NOT say "I do not have enough information" or "I do not know". Instead, clearly state that a direct answer isn't available, and then repeat back, paraphrase, or summarize the related content that *is* present in the documents so the user can see the raw data.
            2. DATABASE INTEGRATION: Treat the data items as a unified repository.
            3. CITATIONS: You must extract and cite the source file name and page numbers found within the corresponding <metadata> tags for every fact or piece of text you reference or repeat.
            4. RELEVANCE: Prioritize relevant items, but use closest matching items for your fallback summary if a direct match isn't found.
            5. TABLES: Interpret Markdown tables strictly by mapping row-to-column relationships.
            6. FORMATTING: Use clean layout structures, bold headings, and bullet points for readability.

            OUTPUT FORMAT:
            [Provide your detailed direct answer OR your structured fallback summary of the provided text here]

            **Sources Used:** [Insert Document Names and Pages here]
            """),

            HumanMessage(content=f"""
            The following database contains multiple structured context data items.
            Use them to answer the question accurately or provide a summary breakdown if a direct answer cannot be deduced.
            
            <database>
            {formatted_contexts}
            </database>
            
            Question: {user_query}
            """)
        ]

        answer = await self.chat_model.ainvoke(messages)

        return answer.content


    async def answer_questions_images(self,user_query,images):

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
            <chunk_context>
                {chunk['context']}
            </chunk_context>
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
            message = format_chunk(chunk)
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
            message = format_chunk(chunk)
            formatted_chunks.append(message)
        # FastEmbed returns a generator, we convert to a list of ndarrays
        # Qdrant accepts these numpy arrays directly
        return list(self.model.embed(formatted_chunks))


    def embed_query(self, query_text):

        # Use .query_embed for search queries
        # returns a generator, so we take the first item [0]
        return list(self.model.query_embed(query_text))[0]


def format_chunk(chunk):

    message = f"""
    Chunk from document : {chunk.document_name}
    Chunk context : {chunk.context}
    Chunk content : {chunk.content}
    Chunk metadata : {chunk.metadata}
    """

    return message