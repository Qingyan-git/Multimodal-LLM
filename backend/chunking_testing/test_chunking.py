"""
Idea : 
    Entire pdf document data split into multiple piplines : 1. Text pipeline, 2. Image pipeline, 3. Tables pipeline and so on

    Text pipeline: 
        1. Extract out all the text from a pdf using pymupdf
        2. Run it through an LLM model to summarise and retrieve back context of entire document
        3. This context will be used to "add-on" to every chunk to provide better semantic quality
        4. Extract out the text from the pdf again but with .get_text('html') to get a structured representation of pdf text
        5. Remove all non-text elements

        Here occurs a branch in methodology.

        Methdology A: Semantic aware chunking that destroys reading order but prioritises semantics
            6. Try to split by sections (perhaps at h1 level)
            7. Pass the whole h1 chunk to a LLM to get back more context
            8. For each section, split into sentences
            9. For each sentence, attach the summarised context
            10. Get embeddings for each chunk, and attach to chunk
            11. Do semantic grouping of each chunk to get chunks similar to each other within a threshold
            12. Concatenate the text for the similar chunks together, and store the full chunk in postgreSQL
            13. Upload chunk with only content text to Qdrant for faster retrieval

        Methodology B: Sliding window chunking with context that provides additional context for each chunk and preserves reading order,
                        but is worse in semantics
            6. Try to split by sections (perhaps at h1 level) (perhaps just at page level)
            7. Pass the whole h1/page chunk to a LLM to get back more context
            8. For each section, split into sentences
            9. Perform sliding window chunking on sentences by section
            10. For each chunk, attach context data
            11. Get embeddings for each chunk
            12. Store chunk in postgreSQL and upload to Qdrant

        Notes:
            Try to use LangChain, use PyMuPDF to chunk, try to see if LangChain has PyMuPDF integration
            Embeddings use Qwen3-VL, check if there is LangChain integration through HuggingFace

    Image pipeline:
        1. Extract out the images from pdf using pymupdf
        2. Filter images out for bad quality images
        3. For each image, pass into a LLM to generate a short summary, and attach the image summary to the chunk under context
        4. Attach pdf summary to the tables chunk under context
        5. Identify the page/section from which the image comes from, and attach the section summary to the image chunk under context
        6. Identify the previous and following text paragraph from the image, and attach together under image content
        7. OCR the image for any text present in the image. If no text, return an empty string
        8. Attach OCR text to image
        9. Embed the image
        10. Store image in postgreSQL and upload to Qdrant

    Tables pipeline:
        1. Extract out tables using pymupdf
        2. Use pymupdf to convert the tables to markdown format for text processing
        3. Attach pdf summary to the tables chunk under context
        4. Identify the page/section from which the image comes from, and attach the section summary to the tables chunk under context
        5. Identify the previous and following text paragraph from the table, and attach together under table content
        6. Embed the table
        7. Store the table in postgreSQL and Qdrant

        """


"""
Findings : 

"""


import pymupdf
from bs4 import BeautifulSoup
from pathlib import Path
from langchain_text_splitters import HTMLHeaderTextSplitter, RecursiveCharacterTextSplitter
from llms import GenerationLLM, EmbeddingModel
import re
import pymupdf4llm
from langchain_text_splitters import MarkdownTextSplitter

def chunk_text(file):

    try:
        if file.is_file() and file.suffix.lower() == '.pdf':
            with pymupdf.open(file) as doc:
                all_text = []
                all_html = f"<html><head><title>{file.stem}</title></head><body>"
                for page in doc:
                    all_text += str(page.get_text('text'))
                    all_html += str(page.get_text('html'))

            summarise_llm = GenerationLLM()
            summary = summarise_llm.summarise_text(all_text)

            print(f'All text : {all_text}\n\n')

            print(f'Summary : {summary}\n\n')

            context = f"""
            This document is : {file.stem}
            This is the context of the document : {summary}
            """

    
    except Exception as e:
        print(f'Error when extracting chunks from {file}, error {e}\n')
        raise


def remove_image_elements(text):
    

    image_chunk_identifier = r"\*\*==> picture \[\d* x \d*\] intentionally omitted <==\*\*"
    picture_text_pattern = r"\*\*----- (Start|End) of picture text -----\*\*"
    break_patterns = r"\<br\>+"
    cleaned_text = re.sub(image_chunk_identifier, '', text)
    cleaned_text = re.sub(picture_text_pattern, '', cleaned_text)
    cleaned_text = re.sub(break_patterns, '', cleaned_text)

    return cleaned_text


def save_to_file(content,filepath=r'C:\Users\Chu Qingyan\Documents\WFH\Multimodal-LLM\chunktesting.md'):

    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')
    
    print(f"Successfully saved to {filepath}")


def markdown_chunk_test(file):
    
    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, ignore_images=True)

    cleaned_markdown_text = remove_image_elements(markdown_text) #type:ignore

    save_to_file(cleaned_markdown_text)

    semantic_chunks = EmbeddingModel().semantic_chunker(cleaned_markdown_text)

    final_chunks = []
    chunk_size = 2000
    chunk_overlap = 200

    recursive_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        #separators=["\n\n", "\n", "|", " ", ""]
    )

    for chunk in semantic_chunks:
        if len(chunk) > chunk_size:
            smaller_chunks = recursive_splitter.split_text(chunk)
            final_chunks.extend(smaller_chunks)
        else:
            final_chunks.append(chunk)
    
    for chunk in final_chunks:
        print(f'Chunk : {chunk}\n\n\n')

    return semantic_chunks


def html_chunk_test(file):
    try:
        if file.is_file() and file.suffix.lower() == '.pdf':
            with pymupdf.open(file) as doc:
                all_html = f"<html><head><title>{file.stem}</title></head><body>"
                for page in doc:
                    all_html += page.get_text('html') #type:ignore

                soup = BeautifulSoup(all_html, 'html.parser')
                for img in soup.find_all('img'):
                    img.decompose() 
                cleaned_html = str(soup)

                print(f'Cleaned HTML : {cleaned_html}\n\n')

                splitter = HTMLHeaderTextSplitter(
                    headers_to_split_on=[('h1','Main Topic')],
                    return_each_element=False
                )

                html_chunks = splitter.split_text(all_html)

                for chunk in html_chunks:
                    print(f'Chunk : {chunk}\n')

    
    except Exception as e:
        print(f'Error when extracting chunks from {file}, error {e}\n')
        raise


def table_extraction_test(file):
    import pymupdf
    
    with pymupdf.open(file) as doc:
        all_tables = []
        for page in doc:
            page_tables = page.find_tables()
            for table in page_tables.tables: #type:ignore
                all_tables.append(table.to_markdown())

        for table in all_tables:
            print(f'Table found : {table}\n\n')


markdown_chunk_test(Path(r'C:\Users\Chu Qingyan\Documents\WFH\Multimodal-LLM\data\raw\government-data-security-policies.pdf'))
