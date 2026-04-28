
from docling.document_converter import DocumentConverter
from docling_core.types.doc.document import DocItemLabel
import asyncio
from pathlib import Path
import os
import dotenv
import pandas as pd
import numpy as np
from langchain_community.callbacks.manager import get_openai_callback

from text_chunking import save_to_file,delete_all_files_in_folder
from docling_image_chunking import get_surrounding_text
from llms_and_models import OpenAIModel, SparseEmbedder, ColBERTEmbedder
from chunks import TableChunk
from postgres import save_document_chunks, insert_pdfs
from qdrant import upload_to_qdrant



def text_between(document,prev,curr):

    prev_index = prev.self_ref
    curr_index = curr.self_ref
    prev_page = prev.prov[0].page_no
    curr_page = curr.prov[0].page_no

    ignored_text_types = [
        DocItemLabel.CAPTION,
        DocItemLabel.REFERENCE,
        DocItemLabel.FOOTNOTE,
        DocItemLabel.PAGE_FOOTER,
        DocItemLabel.PAGE_HEADER]

    for item,_level in reversed(list(document.iterate_items(page_no=prev_page))):
        if item.self_ref == prev_index:
            break
        if item.label not in ignored_text_types and hasattr(item, "text") and item.text.strip():
            return True
        
    for item,_ in document.iterate_items(page_no=curr_page):
        if item.self_ref == curr_index:
            break
        if item.label not in ignored_text_types and hasattr(item, "text") and item.text.strip():
            return True

    return False


def same_table(document,prev_table,curr_table,tolerance=2):

    #Column check (same number of columns)
    prev_table_cols = prev_table.data.num_cols
    curr_table_cols = curr_table.data.num_cols
    if prev_table_cols != curr_table_cols:
        return False

    #Size check (x-coordinates alignment)
    prev_table_x_coords = [prev_table.prov[0].bbox.l,prev_table.prov[0].bbox.r]
    curr_table_x_coords = [curr_table.prov[0].bbox.l,curr_table.prov[0].bbox.r]
    if abs(prev_table_x_coords[0]-curr_table_x_coords[0])>tolerance and abs(prev_table_x_coords[1]-curr_table_x_coords[1])>tolerance:
        return False

    #Text check (any text between tables excluding headers,footers etc)
    if text_between(document,prev_table,curr_table):
        return False

    return True


async def format_tables(tables,document):

    model = OpenAIModel()
    all_tables = []

    for table_idx, item in enumerate(tables):
        #One item is one table
        table_data = []
        for i,table in enumerate(item['tables']):

            df = table.export_to_dataframe(doc=document)

            if i > 0:
                
                """
                Instead of only checking whether the first row should be a continuation of the previous page's table, 
                should check while the first row only has 1 row of data to combine with the previous table
                essentially broadening the table similarity definition
                """

                table_headers = table_data[0].columns.tolist()
                df.columns = table_headers

                #Check if first row is headers
                first_row = df.iloc[0].tolist()
                headers = df.columns
                if first_row == headers:
                    #Remove headers
                    df = df.iloc[1:]

                while not df.empty:
                    text_row = df.iloc[0].tolist()
                    valid_rows = [i for i, val in enumerate(text_row) if str(val).strip()]
                    text_counter = len(valid_rows)

                    if text_counter == 1:
                        # 1. Access the previous DataFrame in the list
                        prev_df = table_data[-1]
                        col = valid_rows[0]

                        # 2. Safely get the existing value (handle NaNs/None)
                        # .iat is fast, but we need to ensure we don't f-string 'nan'
                        raw_prev_val = prev_df.iat[-1, col]
                        prev_val = str(raw_prev_val) if pd.notna(raw_prev_val) else ""

                        # 3. Merge the text and update the last row of the previous DF
                        # Using .strip() to avoid leading spaces if prev_val was empty
                        new_val = f"{prev_val} {str(text_row[col])}".strip()
                        prev_df.iat[-1, col] = new_val

                        # 4. Remove the straggler row from the current fragment
                        df = df.iloc[1:]

                    else:
                        break

            table_data.append(df)
        
        combined_df = pd.concat(table_data,axis=0,ignore_index=True)
        markdown_table = combined_df.to_markdown()

        raw_name = document.origin.filename
        document_name = Path(raw_name).stem
        clean_filename = f"{document_name}_Table_{table_idx}.csv"
        save_path = Path(os.getenv('table_results_path')) / clean_filename
        combined_df.to_csv(save_path, index=False)

        caption = " ".join(item['caption']).strip()
        chunk_text = f"Text before table : \n{" ".join(item['prev_text']).strip() if item['prev_text'] else "No text before table"}\n Table : \n{markdown_table if markdown_table.strip() else "No table found"}\n Caption : \n{caption if caption.strip() else "No caption found"}\n Text after table : \n{" ".join(item['post_text']) if item['post_text'] else "No text after table"}\n"
        chunk_context = await model.get_context(document.export_to_markdown(),chunk_text)

        chunk = TableChunk()
        chunk.document_name = document.origin.filename
        chunk.context = chunk_context
        chunk.content = chunk_text
        chunk.metadata = {
            'pages' : list(item['pages'])
            }

        all_tables.append(chunk)

    return all_tables


async def extract_tables(filepath):

    docling = DocumentConverter()

    #Object that holds the document
    document = docling.convert(filepath).document

    #All document tables
    document_tables = document.tables

    all_tables = []
    last_seen_table = float('inf')

    for table in document_tables:

        page_no = table.prov[0].page_no
        prev_text,post_text = get_surrounding_text(document,table)
        caption = table.caption_text(doc=document)

        """
        Perhaps the condition to check of is_continuation table should be different.
        Because even docling can have detection failures on non line demarcated table border tables.
        Docling detects that there is one data "column" on the left and multiple data "rows" on the right, but sometimes doesnt assign the data
        on the right to the column on the left still.
        Perhaps check that if the current row detected only has 1 row of data, merge with previously detected table.
        """

        #One item is one final table
        item = {
            'tables' : [table],
            'caption' : [caption],
            'prev_text' : [prev_text],
            'post_text' : [post_text],
            'pages' : [page_no],
        }

        prev_table = all_tables[-1]['tables'][-1] if all_tables else None
        is_same_table = prev_table and (page_no-last_seen_table<=1) and same_table(document,prev_table,table)

        if is_same_table:
            #Same table, just continutation
            prev_item = all_tables.pop()
            for key in prev_item.keys():
                prev_item[key].extend(item[key])
            all_tables.append(prev_item)
        else:
            #Different table
            all_tables.append(item)

        last_seen_table = page_no

    cleaned_chunks = await format_tables(all_tables,document)

    return cleaned_chunks


async def process_tables(folder_path):
    try:

        model = OpenAIModel()
        sparse_embedder = SparseEmbedder()
        late_embedder = ColBERTEmbedder()

        if folder_path.is_dir():

            for file in folder_path.iterdir():

                if file.is_file():

                    insert_pdfs(file)

                    print(f'Finished inserting pdf to postgresdb\n\n')

                    with get_openai_callback() as cb:

                        chunks = await extract_tables(file)

                        print(f'\tFinished getting text chunks\n\n')

                        token_cost = f"Token cost to TEXT chunk {file.name} : {cb.total_tokens}"
                        money_cost = f"Money cost to TEXT chunk {file.name} : {cb.total_cost}"
                        total_cost = [token_cost,money_cost]

                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    if chunks:

                        returned_chunks = save_document_chunks(file.name,chunks,type='table')

                        print(f'\tFinished saving chunks into postgresdb\n\n')

                        dense_embeddings,cost = await model.embed_texts(returned_chunks)
                        token_cost = f"Token cost to EMBED TEXT {file.name} : {cost[0]}"
                        money_cost = f"Money cost to EMBED TEXT {file.name} : {cost[1]}"
                        total_cost = [token_cost,money_cost]
                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                        sparse_embeddings = await asyncio.to_thread(sparse_embedder.embed_texts, returned_chunks)
                        late_embeddings = await asyncio.to_thread(late_embedder.embed_texts, returned_chunks)

                        print(f'\tFinished getting embeddings\n\n')

                        upload_to_qdrant(returned_chunks,dense_embeddings,sparse_embeddings,late_embeddings)

                        print(f'\tFinished uploading embeddings to qdrant\n\n')

                    print(f'Finished processing\n\n')

            print(f'\nFinished processing all files\n')

    except Exception as e:
        print(f'Unable to ingest all pdfs, error {e}')
        raise


if __name__ == "__main__":

    print(f'Ingestion running\n\n\n')
    table_pdfs_path = Path(os.getenv('all_pdfs_path'))
    table_results_path = Path(os.getenv('table_results_path'))

    delete_all_files_in_folder(table_results_path)

    asyncio.run(process_tables(table_pdfs_path))