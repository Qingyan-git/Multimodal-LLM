from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import traceback
import pandas as pd
from tabulate import tabulate

from langchain_text_splitters import RecursiveCharacterTextSplitter

from llms_and_models import OpenAIModel
from chunks import TableChunk
from postgres import save_document_chunks, insert_pdf
from qdrant import upload_to_qdrant, format_embeddings
from text_chunking import save_to_file



def same_table(prev_page,prev_table,curr_page,curr_table,tolerance=5,max_words=10):

    if prev_table.col_count != curr_table.col_count:
        return False

    prev_bbox = prev_table.bbox
    curr_bbox = curr_table.bbox

    if abs(prev_bbox[0]-curr_bbox[0]) > tolerance or abs(prev_bbox[2]-curr_bbox[2]) > tolerance:
        return False

    bottom_clip = pymupdf.Rect(0,prev_bbox[3],prev_page.rect.width,prev_page.rect.height)
    prev_text = prev_page.get_text('text',clip=bottom_clip).strip().split()

    top_clip = pymupdf.Rect(0,0,curr_page.rect.width,curr_bbox[1])
    curr_text = curr_page.get_text('text',clip=top_clip).strip().split()

    total_text = prev_text + curr_text

    if len(total_text) > max_words:
        return False

    return True


def get_table_caption(page,table):
    
    top_text = ""
    bottom_text = ""

    table_top = table.bbox[1]
    table_bottom = table.bbox[3]
    closest_block_top = float('inf')
    closest_block_bottom = float('inf')

    blocks = page.get_text('blocks',sort=True)

    for block in blocks:
        block_text = block[4]
        block_top = block[1]
        block_bottom = block[3]
        dist_top = table_top-block_bottom
        dist_bottom = block_top-table_bottom
        if 0 < dist_top < closest_block_top:
            top_text = block_text
            closest_block_top = dist_top
        if 0 < dist_bottom < closest_block_bottom:
            bottom_text = block_text
            closest_block_bottom = dist_bottom

    table_texts = top_text + bottom_text

    return table_texts



async def extract_tables(filepath):

    all_tables = []
    last_seen_table = -1

    with pymupdf.open(filepath) as doc:

        for page_no, page in enumerate(doc):

            found_tables = page.find_tables()

            for i,table in enumerate(found_tables.tables):

                print(f"\n{table.extract()}\n")

                item = {
                        'table_texts' : [],
                        'tables' : [],
                        'pages' : []
                    }
                
                table_texts = get_table_caption(page,table)
                is_possible_continuation = (page_no - last_seen_table == 1 and i == 0)

                if not is_possible_continuation:
                    #Not possible for tables to be continutation of the previous page
                    item['table_texts'].append(table_texts)
                    item['tables'].append(table)
                    item['pages'].append(page_no)
                    all_tables.append(item)
                    last_seen_table = page_no

                else:
                    #Possible for tables to be continutations of each other
                    prev_table_item = all_tables[-1]
                    most_recent_table = prev_table_item['tables'][-1]
                    is_same_table = same_table(doc[last_seen_table], most_recent_table, page, table)

                    if is_same_table:
                        #The previous table and the current one is deemed to be the same table split amongst pages
                        prev_table_item['table_texts'].append(table_texts)
                        prev_table_item['tables'].append(table)
                        prev_table_item['pages'].append(page_no)
                        last_seen_table = page_no

                    else:
                        #Table on previous page is distinct from table on current page
                        item['table_texts'].append(table_texts)
                        item['tables'].append(table)
                        item['pages'].append(page_no)
                        all_tables.append(item)
                        last_seen_table = page_no

    return all_tables


async def format_tables(filepath,all_tables):

    with pymupdf.open(filepath) as doc:
        full_text = pymupdf4llm.to_markdown(doc)

        model = OpenAIModel()
        doc_name = filepath.name
        table_chunks = []

        for item in all_tables:
            #One item is one final table
            table_headers = []
            table_data = []
            table_pages = item['pages']
            
            for i,table in enumerate(item['tables']):        

                # headers = table.header

                # print('\n')
                # print(f"headers : {headers.names}, external : {headers.external}")
                # print('\n')


                # rows = table.extract()
                # first_two = rows[:2]

                # for i in range(len(first_two)):
                #     print('\n')
                #     print(f"row {i}: {first_two[i]}")
                #     print('\n')


                num_cols = table.col_count
                table_page = table_pages[i]

                headers = [
                    " ".join(str(header).replace('<br>', " ").split()).strip()
                for header in table.header.names
                ]

                bboxs = table.cells
                text = [
                    " ".join(doc[table_page].get_text("text",clip=bbox).replace("<br>"," ").split()).strip()
                    for bbox in bboxs
                    ]

                num_rows = len(text)//len(headers)
                columns = [text[i * num_rows : (i + 1) * num_rows] for i in range(len(headers))]
                data = [list(row) for row in zip(*columns)]

                if i == 0:
                    #First joining table need to extract headers to set table headers
                    table_headers = headers

                #Other continuous tables in the sequence need to extract data
                first_row_text = data[0]
                if [str(row).strip() for row in first_row_text] == [str(head).strip() for head in headers]:
                    #First row has same data as the header column, hence the first row is not relevant
                    table_data.extend(data[1:])
                else:
                    #First row is different data then header column, first row is real data
                    table_data.extend(data)

            combined_table = pd.DataFrame(table_data,columns=table_headers).to_markdown(index=False)

            combined_text = "\n".join(item['table_texts'])

            contents = f"\nTexts around tables : {combined_text}\nTables :\n{combined_table}\n"
            table_context = await model.get_context(full_text, contents)

            table_chunk = TableChunk()
            table_chunk.document_name = doc_name
            table_chunk.context = table_context
            table_chunk.content = contents
            table_chunk.metadata = {'pages' : item['pages']}

            table_chunks.append(table_chunk)

    return table_chunks


async def process_tables(folder_path):

    try:
        if folder_path.is_dir():

            model = OpenAIModel()

            for file in folder_path.iterdir():

                chunks = await extract_tables(file)

                print(f'\tFinished extracting tables\n\n')

                returned_chunks = save_document_chunks(file.name,chunks)

                print(f'\tFinished saving chunks into postgresdb\n\n')

                embeddings = await model.embed_texts(returned_chunks)

                print(f'\tFinished getting embeddings\n\n')

                upload_to_qdrant(embeddings)

                print(f'\tFinished uploading chunk embeddings to qdrant\n\n')

                print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to process tables, error {e}')
        traceback.print_exc() 
        raise



if __name__ == "__main__":

    async def main():

        test_pdf_path = Path(r"C:\Users\UserAdmin\Documents\Multimodal-LLM\pdfs\tables\Project Guardian FX workstream Transaction Banking.pdf")
        results_path = Path(os.getenv('test_results_path'))
        file = results_path / "tables-test.txt"




        filename = 'table-chunk-test'
        filepath = os.getenv('table_results_path')

        # with pymupdf.open(test_pdf_path) as doc:
        #     save_to_file(filename,pymupdf4llm.to_markdown(doc),filepath)

        all_table_chunks = await extract_tables(test_pdf_path)
        clean_chunks = await format_tables(test_pdf_path, all_table_chunks)

        # filename = 'table-chunk-test'
        # filepath = os.getenv('table_results_path')

        # for table in clean_chunks:
        #     save_to_file(filename,table.content,filepath)


    asyncio.run(main())