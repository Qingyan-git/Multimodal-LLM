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
import pymupdf4llm
from bs4 import BeautifulSoup
from langchain_text_splitters import HTMLHeaderTextSplitter, MarkdownTextSplitter, RecursiveCharacterTextSplitter
from llms_and_models import GenerationLLM, EmbeddingModel, RecursiveSplitter, OpenClipModel
from pathlib import Path
import re
import io
import cv2
import numpy as np
from chunks import TextChunk,ImageChunk
from PIL import Image
import pytesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


test_file = Path(r'C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\smartnation2-report.pdf')



def delete_files_in_folder(folder_path):

    folder_path = Path(folder_path)

    for item in Path(folder_path).iterdir():
        if item.is_file():
            item.unlink()

    print(f"All files in {folder_path.name} have been deleted.\n")


def extract_text(block):

    block_text = f""

    for line in block['lines']:
        for span in line['spans']:
            block_text += ' ' + span['text']

    block_text = clean_text(block_text)

    return block_text


def clean_text(text):

    image_chunk_identifier = r"\*\*==> picture \[\d* x \d*\] intentionally omitted <==\*\*"
    picture_text_pattern = r"\*\*----- (Start|End) of picture text -----\*\*"
    break_patterns = r"\<br\>+"
    cleaned_text = re.sub(image_chunk_identifier, '', text)
    cleaned_text = re.sub(picture_text_pattern, '', cleaned_text)
    cleaned_text = re.sub(break_patterns, '', cleaned_text)
    cleaned_text = re.sub(r'\n{3,}','\n\n',cleaned_text)

    return cleaned_text.strip()


def save_to_file(content,filepath=r'C:\Users\UserAdmin\Documents\Multimodal-LLM\chunk_test.md'):

    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        f.writelines(content)


def get_text_summary(text):

    summariser = GenerationLLM()
    summary = summariser.summarise_text(text)

    return summary


def get_image_caption(image_PIL):

    captioning = GenerationLLM()
    caption = captioning.caption_image(image_PIL)

    return caption


def prepare_for_cv2(image_PIL):

    image_RGB = image_PIL.convert('RGB') #Convert image to RGB 

    image_numpy = np.array(image_RGB) #Convert image to np array so that cv2 can understand

    image_cv2 = cv2.cvtColor(image_numpy, cv2.COLOR_RGB2BGR) #Convert image from RGB to BGR to grayscale
    image_grayscale = cv2.cvtColor(image_cv2, cv2.COLOR_BGR2GRAY)
    
    return image_grayscale


def get_edge_density(image_PIL,low_threshold=100,high_threshold=200):
    image_grayscale = prepare_for_cv2(image_PIL)
    
    # blurred = cv2.GaussianBlur(image_grayscale, (5, 5), 0) # Apply Gaussian Blur to reduce noise (helps avoid "fake" edges)

    edges = cv2.Canny(image_grayscale, low_threshold, high_threshold)
    
    edge_pixel_count = np.sum(edges > 0) # Calculate Density: (Number of edge pixels) / (Total pixels)
    total_pixels = edges.shape[0] * edges.shape[1]
    
    return edge_pixel_count / total_pixels


def useable_image(image, min_dim=100, min_color=32, maxcolors=256, min_buffer=0.03, min_edge_density=0.03):

    if image.height < min_dim or image.width < min_dim:
        return False

    ratio = image.height / image.width 
    if ratio > 5 or ratio < 0.2:
        return False

    colors = image.getcolors(maxcolors=maxcolors)
    if colors is not None and len(colors) < min_color:
        return False

    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=80)
    buffer_size = len(buffer.getvalue())
    num_pixels = image.width * image.height
    bytes_per_pixel = buffer_size / num_pixels
    if bytes_per_pixel < min_buffer: #if low image_bytes:pixel ratio means image content is mostly uniform
        return False

    image_edge_density = get_edge_density(image) #if low image edge density means that image is mostly empty/plain
    if image_edge_density < min_edge_density:
        return False

    return True


def format_for_sql(image):

    buffer = io.BytesIO()

    formatted_image = image.save(buffer,format='PNG')

    return buffer.getvalue()


def ocr_image(image):

    text = pytesseract.image_to_string(image)
    text = clean_text(text)

    return text


def get_image_from_block(page,block,zoom=2):
    rect = pymupdf.Rect(block["bbox"])
    matrix = pymupdf.Matrix(zoom, zoom)
    image_PIL = page.get_pixmap(matrix=matrix, clip=rect).pil_image().convert('RGB')

    use_image = useable_image(image_PIL)

    if use_image:

        # image_PIL.save(rf"C:\Users\UserAdmin\Documents\Multimodal-LLM\backend\test-chunking\pdf_images\{page.number}_{block['number']}_img.png", format='png') # Save image

        # image_PIL.show() # Show image 

        image_chunk = ImageChunk()
        image_chunk.content['text'] = ocr_image(image_PIL)
        image_chunk.content['image'] = image_PIL

        return image_chunk

    else:
        return use_image


def image_chunk_test(file):

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

    cleaned_markdown_text = clean_text(markdown_text)
    summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"

    image_chunks = []
    with pymupdf.open(file) as doc:
        for page_no, page in enumerate(doc):
            blocks = page.get_text("dict")["blocks"]
            for i, block in enumerate(blocks):
                if block['type'] == 1:
                    image = get_image_from_block(page,block)
                    if image :
                        chunk_context = f'Chunk Context : '
                        for block_no in range(i,-1,-1):
                            if blocks[block_no]['type'] == 0:
                                chunk_context += f"{extract_text(blocks[block_no])}"
                                break
                        for _ in range(i+1,len(blocks)):
                            if blocks[block_no]['type'] == 0:
                                chunk_context += f"{extract_text(blocks[block_no])}"
                                break
                        total_context = summary + '\n' + chunk_context

                        image.context = summary
                        image.metadata['document_name'] = file.name
                        image.metadata['pages'] = page_no
                        image_chunks.append(image)

    clip_model = OpenClipModel()
    scores = clip_model.classify_images([image.content['image'] for image in image_chunks], batch_size=32)
    
    final_chunks = []
    for i, score in enumerate(scores):
        if score[0] > 0.85: 
            final_chunks.append(image_chunks[i])
            image_chunks[i].content['image'].save(rf"C:\Users\UserAdmin\Documents\Multimodal-LLM\backend\test-chunking\pdf_images\img_{i}_.png", format='png')

        print(f"Score number {i} : {score}\n\n")

    return final_chunks


def extract_pictures(file):

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

        cleaned_markdown_text = clean_text(markdown_text)
        summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"
        print(f"Summary : {summary}\n\n")

    image_chunks = []
    xrefs_pages = {}
    filter_model = OpenClipModel()

    with pymupdf.open(file) as doc:
        for page_no, page in enumerate(doc):
            page_images = page.get_images(page_no)
            for image in page_images:
                xref = image[0]
                mask = image[1]
                if mask > 0:
                    continue
                if xref not in xrefs_pages:
                    image_PIL = pymupdf.Pixmap(doc.extract_image(xref)["image"]).pil_image()

                    """
                    image filtering still needs to be improved on
                    """

                    use = filter_model.classify_one_image(image_PIL)
                    if use > 0.6:
                        xrefs_pages[xref] = [page_no]
                else:
                    xrefs_pages[xref].append(page_no+1)

        for xref, pages in xrefs_pages.items():
            print(f'Processing image {xref} now\n')
            image = doc.extract_image(xref)
            image_PIL = pymupdf.Pixmap(doc.extract_image(xref)["image"]).pil_image()

            image_chunk = ImageChunk()
            image_chunk.context = summary
            image_chunk.content['text'] = get_image_caption(image_PIL)
            image_chunk.content['image'] = image_PIL
            image_chunk.metadata['document_name'] = file.name
            image_chunk.metadata['pages'] = pages

            image_chunks.append(image_chunk)

    return image_chunks



def extract_all_images(file):

    """
    Need to get better image chunking, current image chunks have weird filters and masks on them, need to figure out how to only get raw images

    Filter images for usefulness as well

    Redo saving to postgres

    """

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

        cleaned_markdown_text = clean_text(markdown_text)
        summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"
        print(f"Summary : {summary}\n\n")

    image_chunks = []
    xrefs_pages = {}
    mask_xrefs = set()

    with pymupdf.open(file) as doc:
        for page_no, page in enumerate(doc):
            page_images = page.get_images(page_no)
            for image in page_images:
                xref = image[0]
                mask = image[1]
                if xref not in xrefs_pages:
                    xrefs_pages[xref] = []
                xrefs_pages[xref].append(page_no+1)
                if mask > 0:
                    mask_xrefs.add(mask)
    
        final_xrefs = [xref for xref in xrefs_pages.keys() if xref not in mask_xrefs]
        for xref in final_xrefs:
            image = doc.extract_image(xref)
            mask = image.get('smask',0)
            if mask > 0:
                base_pix = pymupdf.Pixmap(doc.extract_image(xref)["image"])    # (1) pixmap of image w/o alpha
                mask_pix = pymupdf.Pixmap(doc.extract_image(mask)["image"])    # (2) mask pixmap

                if (base_pix.width != mask_pix.width) or (base_pix.height != mask_pix.height):

                    temp_mask = pymupdf.Pixmap(mask_pix, base_pix.width, base_pix.height)

                    pix = pymupdf.Pixmap(base_pix, temp_mask)              # (3) copy of pix1, image mask added
                else:
                    pix = pymupdf.Pixmap(base_pix, mask_pix)               # (3) copy of pix1, image mask added
                     
                if pix.colorspace.n > 3:
                    pix = pymupdf.Pixmap(pymupdf.csRGB, pix)
                image_bytes = pix.tobytes("png")
            else:
                image_bytes = image['image']
            image_PIL = Image.open(io.BytesIO(image_bytes)).convert('RGB')

            image_chunk = ImageChunk()
            image_chunk.context = summary
            image_chunk.content['text'] = ocr_image(image_PIL)
            image_chunk.content['image'] = image_PIL
            image_chunk.metadata['document_name'] = file.name
            image_chunk.metadata['pages'] = xrefs_pages[xref]

            image_chunks.append(image_chunk)

    return image_chunks

# print(f'\nImage chunk test running...\n')
# delete_files_in_folder(r"C:\Users\UserAdmin\Documents\Multimodal-LLM\backend\test-chunking\pdf_images")
# scanned_pages = extract_pictures(Path(r'C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\smartnation2-report.pdf'))
# print(f'Image chunk test finished, printing chunks now...\n')
# for i,page in enumerate(scanned_pages):
#     image = page.content['image'].save(rf"C:\Users\UserAdmin\Documents\Multimodal-LLM\backend\test-chunking\pdf_images\img{i}.png")
#     print(f"Image number : {i}")
#     print(f"Image OCR text : {page.content['text']}")
#     print(f"Image metadata : {page.metadata}\n")



def markdown_chunk_test(file):

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

    cleaned_markdown_text = clean_text(markdown_text)
    save_to_file([cleaned_markdown_text],filepath=r'C:\Users\UserAdmin\Documents\Multimodal-LLM\backend\test-chunking\markdown_chunk_test.md')
    summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"

    print(f"SUMMARY : {summary}")

    semantic_chunks = EmbeddingModel().semantic_chunker(cleaned_markdown_text)

    recursive_splitter = RecursiveSplitter()
    cleaned_chunks = []
    for chunk in semantic_chunks:
        if len(chunk) > recursive_splitter.get_chunk_size():
            smaller_chunks = recursive_splitter.split_text(chunk)
            cleaned_chunks.extend(smaller_chunks)
        else:
            cleaned_chunks.append(chunk)
    
    final_text_chunks = []
    current_page_number = 1
    for chunk in cleaned_chunks:
        chunk = chunk.strip()
        page_pattern = r"\s*--- end of page\.page_number=(\d+) ---\s*"
        content = re.sub(page_pattern,'\n\n',chunk).strip()
        if len(content) != 0:
            text_chunk = TextChunk()
            document_name = file.name
            pages = set()
            for match in re.finditer(page_pattern, chunk):
                page = int(match.group(1))
                current_page_number = page + 1
                if match.start() != 0:
                    pages.add(page)
                if match.end() != len(chunk):
                    pages.add(page+1)
            if not pages:
                pages = [current_page_number]

            text_chunk.context = summary
            text_chunk.content = content
            text_chunk.metadata['document_name'] = document_name
            text_chunk.metadata['pages'] = list(pages)

            final_text_chunks.append(text_chunk)

    return final_text_chunks


def page_chunk_test(file):

    with pymupdf.open(file) as doc:
        markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

        cleaned_markdown_text = clean_text(markdown_text)
        summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"
        print(f"Summary : {summary}\n\n")

    scanned_pages = []
    with pymupdf.open(file) as doc:
        for page_no, page in enumerate(doc):
            page_chunk = ImageChunk()

            page_scan = page.get_pixmap(dpi=300).pil_image().convert('RGB')
            page_ocr_text = ocr_image(page_scan)

            page_chunk.context = summary
            page_chunk.content['text'] = page_ocr_text
            page_chunk.content['image'] = page_scan
            page_chunk.metadata['document_name'] = file.name
            page_chunk.metadata['pages'] = [page_no]

            scanned_pages.append(page_chunk)

    return scanned_pages


def html_chunk_test(file):
    try:
        if file.is_file() and file.suffix.lower() == '.pdf':
            with pymupdf.open(file) as doc:
                all_html = ""
                for page in doc:
                    all_html += page.get_text('html') #type:ignore

                soup = BeautifulSoup(all_html, 'html.parser')
                for img in soup.find_all('img'):
                    img.decompose() 
                cleaned_html = str(soup)

                save_to_file([cleaned_html],filepath=r'C:\Users\UserAdmin\Documents\Multimodal-LLM\html_chunk_test.html')

                splitter = HTMLHeaderTextSplitter(
                    headers_to_split_on=[('h1','Main Topic'),('h2','Subtopic'),('h3','Heading'),('h4','Subheading')],
                    return_each_element=False
                )

                html_chunks = splitter.split_text(all_html)

                for chunk in html_chunks:
                    print(f'Chunk : {chunk}\n')

    
    except Exception as e:
        print(f'Error when extracting chunks from {file}, error {e}\n')
        raise



def table_extraction_test(file):
    
    with pymupdf.open(file) as doc:
        all_tables = []
        for page in doc:
            page_tables = page.find_tables()
            for table in page_tables.tables: #type:ignore
                all_tables.append(table.to_markdown())

        for table in all_tables:
            print(f'Table found : {table}\n\n')


markdown_chunk_test(Path(r"C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\Guide to Data Protection Practices for ICT Systems.pdf"))
# html_chunk_test(test_file)
# table_extraction_test(test_file)


