# import pymupdf
# import pymupdf4llm
# from bs4 import BeautifulSoup
# from langchain_text_splitters import HTMLHeaderTextSplitter, MarkdownTextSplitter, RecursiveCharacterTextSplitter
# from llms_and_models import GenerationLLM, EmbeddingModel, RecursiveSplitter, OpenClipModel
# from pathlib import Path
# import re
# import io
# import cv2
# import numpy as np
# from chunks import TextChunk,ImageChunk
# from PIL import Image
# import pytesseract
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


# test_file = Path(r'C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\smartnation2-report.pdf')



# def delete_files_in_folder(folder_path):

#     folder_path = Path(folder_path)

#     for item in Path(folder_path).iterdir():
#         if item.is_file():
#             item.unlink()

#     print(f"All files in {folder_path.name} have been deleted.\n")


# def extract_text(block):

#     block_text = f""

#     for line in block['lines']:
#         for span in line['spans']:
#             block_text += ' ' + span['text']

#     block_text = clean_text(block_text)

#     return block_text


# def clean_text(text):

#     image_chunk_identifier = r"\*\*==> picture \[\d* x \d*\] intentionally omitted <==\*\*"
#     picture_text_pattern = r"\*\*----- (Start|End) of picture text -----\*\*"
#     break_patterns = r"\<br\>+"
#     cleaned_text = re.sub(image_chunk_identifier, '', text)
#     cleaned_text = re.sub(picture_text_pattern, '', cleaned_text)
#     cleaned_text = re.sub(break_patterns, '', cleaned_text)
#     cleaned_text = re.sub(r'\n{3,}','\n\n',cleaned_text)

#     return cleaned_text.strip()


# def save_to_file(content,filepath=r'C:\Users\UserAdmin\Documents\Multimodal-LLM\chunk_test.md'):

#     path = Path(filepath)
#     path.parent.mkdir(parents=True, exist_ok=True)
#     with path.open('w', encoding='utf-8') as f:
#         f.writelines(content)


# def get_text_summary(text):

#     summariser = GenerationLLM()
#     summary = summariser.summarise_text(text)

#     return summary


# def get_image_caption(image_PIL):

#     captioning = GenerationLLM()
#     caption = captioning.caption_image(image_PIL)

#     return caption


# def prepare_for_cv2(image_PIL):

#     image_RGB = image_PIL.convert('RGB') #Convert image to RGB 

#     image_numpy = np.array(image_RGB) #Convert image to np array so that cv2 can understand

#     image_cv2 = cv2.cvtColor(image_numpy, cv2.COLOR_RGB2BGR) #Convert image from RGB to BGR to grayscale
#     image_grayscale = cv2.cvtColor(image_cv2, cv2.COLOR_BGR2GRAY)
    
#     return image_grayscale


# def get_edge_density(image_PIL,low_threshold=100,high_threshold=200):
#     image_grayscale = prepare_for_cv2(image_PIL)
    
#     # blurred = cv2.GaussianBlur(image_grayscale, (5, 5), 0) # Apply Gaussian Blur to reduce noise (helps avoid "fake" edges)

#     edges = cv2.Canny(image_grayscale, low_threshold, high_threshold)
    
#     edge_pixel_count = np.sum(edges > 0) # Calculate Density: (Number of edge pixels) / (Total pixels)
#     total_pixels = edges.shape[0] * edges.shape[1]
    
#     return edge_pixel_count / total_pixels


# def useable_image(image, min_dim=100, min_color=32, maxcolors=256, min_buffer=0.03, min_edge_density=0.03):

#     if image.height < min_dim or image.width < min_dim:
#         return False

#     ratio = image.height / image.width 
#     if ratio > 5 or ratio < 0.2:
#         return False

#     colors = image.getcolors(maxcolors=maxcolors)
#     if colors is not None and len(colors) < min_color:
#         return False

#     buffer = io.BytesIO()
#     image.save(buffer, format="JPEG", quality=80)
#     buffer_size = len(buffer.getvalue())
#     num_pixels = image.width * image.height
#     bytes_per_pixel = buffer_size / num_pixels
#     if bytes_per_pixel < min_buffer: #if low image_bytes:pixel ratio means image content is mostly uniform
#         return False

#     image_edge_density = get_edge_density(image) #if low image edge density means that image is mostly empty/plain
#     if image_edge_density < min_edge_density:
#         return False

#     return True


# def ocr_image(image):

#     text = pytesseract.image_to_string(image)
#     text = clean_text(text)

#     return text


# def extract_pictures(file):

#     with pymupdf.open(file) as doc:
#         markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

#         cleaned_markdown_text = clean_text(markdown_text)
#         summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"
#         print(f"Summary : {summary}\n\n")

#     image_chunks = []
#     xrefs_pages = {}
#     filter_model = OpenClipModel()

#     with pymupdf.open(file) as doc:
#         for page_no, page in enumerate(doc):
#             page_images = page.get_images(page_no)
#             for image in page_images:
#                 xref = image[0]
#                 mask = image[1]
#                 if mask > 0:
#                     continue
#                 if xref not in xrefs_pages:
#                     image_PIL = pymupdf.Pixmap(doc.extract_image(xref)["image"]).pil_image()

#                     """
#                     image filtering still needs to be improved on
#                     """

#                     use = filter_model.classify_one_image(image_PIL)
#                     if use > 0.6:
#                         xrefs_pages[xref] = [page_no]
#                 else:
#                     xrefs_pages[xref].append(page_no+1)

#         for xref, pages in xrefs_pages.items():
#             print(f'Processing image {xref} now\n')
#             image = doc.extract_image(xref)
#             image_PIL = pymupdf.Pixmap(doc.extract_image(xref)["image"]).pil_image()

#             image_chunk = ImageChunk()
#             image_chunk.context = summary
#             image_chunk.content['text'] = get_image_caption(image_PIL)
#             image_chunk.content['image'] = image_PIL
#             image_chunk.metadata['document_name'] = file.name
#             image_chunk.metadata['pages'] = pages

#             image_chunks.append(image_chunk)

#     return image_chunks



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


# def page_chunk_test(file):

#     with pymupdf.open(file) as doc:
#         markdown_text = pymupdf4llm.to_markdown(doc, header=False, footer=False, page_separators=True, force_ocr=True)

#         cleaned_markdown_text = clean_text(markdown_text)
#         summary = f"Summary of document {file.name}: {get_text_summary(cleaned_markdown_text)}"
#         print(f"Summary : {summary}\n\n")

#     scanned_pages = []
#     with pymupdf.open(file) as doc:
#         for page_no, page in enumerate(doc):
#             page_chunk = ImageChunk()

#             page_scan = page.get_pixmap(dpi=300).pil_image().convert('RGB')
#             page_ocr_text = ocr_image(page_scan)

#             page_chunk.context = summary
#             page_chunk.content['text'] = page_ocr_text
#             page_chunk.content['image'] = page_scan
#             page_chunk.metadata['document_name'] = file.name
#             page_chunk.metadata['pages'] = [page_no]

#             scanned_pages.append(page_chunk)

#     return scanned_pages


# def html_chunk_test(file):
#     try:
#         if file.is_file() and file.suffix.lower() == '.pdf':
#             with pymupdf.open(file) as doc:
#                 all_html = ""
#                 for page in doc:
#                     all_html += page.get_text('html') #type:ignore

#                 soup = BeautifulSoup(all_html, 'html.parser')
#                 for img in soup.find_all('img'):
#                     img.decompose() 
#                 cleaned_html = str(soup)

#                 save_to_file([cleaned_html],filepath=r'C:\Users\UserAdmin\Documents\Multimodal-LLM\html_chunk_test.html')

#                 splitter = HTMLHeaderTextSplitter(
#                     headers_to_split_on=[('h1','Main Topic'),('h2','Subtopic'),('h3','Heading'),('h4','Subheading')],
#                     return_each_element=False
#                 )

#                 html_chunks = splitter.split_text(all_html)

#                 for chunk in html_chunks:
#                     print(f'Chunk : {chunk}\n')

    
#     except Exception as e:
#         print(f'Error when extracting chunks from {file}, error {e}\n')
#         raise



# def table_extraction_test(file):
    
#     with pymupdf.open(file) as doc:
#         all_tables = []
#         for page in doc:
#             page_tables = page.find_tables()
#             for table in page_tables.tables: #type:ignore
#                 all_tables.append(table.to_markdown())

#         for table in all_tables:
#             print(f'Table found : {table}\n\n')


# markdown_chunk_test(Path(r"C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\Guide to Data Protection Practices for ICT Systems.pdf"))
# # html_chunk_test(test_file)
# # table_extraction_test(test_file)


