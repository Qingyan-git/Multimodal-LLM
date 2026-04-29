
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



def text_between(document,prev,curr, threshold=50):

    prev_ref = prev.self_ref
    curr_ref = curr.self_ref

    ignored_text_types = {
        DocItemLabel.CAPTION, DocItemLabel.REFERENCE, 
        DocItemLabel.FOOTNOTE, DocItemLabel.PAGE_FOOTER,
        DocItemLabel.PAGE_HEADER, DocItemLabel.FORMULA, 
        DocItemLabel.LIST_ITEM
    }
    
    red_flags = {
        DocItemLabel.TITLE, DocItemLabel.SECTION_HEADER,
        DocItemLabel.PICTURE
    }

    # Start checking only AFTER we see the previous table
    found_prev = False
    textlen = 0
    for item, _ in document.iterate_items():

        if not found_prev and item.prov[0].page_no < prev.prov[0].page_no:
            continue

        if item.self_ref == prev_ref:
            found_prev = True
            continue
            
        if item.self_ref == curr_ref:
            # We reached the current table without finding real text
            break
            
        if found_prev:
            # We are now in the 'gap'
            if item.label in red_flags:
                return True
            elif item.label not in ignored_text_types:
                item_text = getattr(item, "text", "")
                textlen += len(item.text.strip())
                if textlen > threshold:
                    return True # This is real body text! Split the table.

    return False


def same_headers(prev,curr):

    # Extract text from the first row of each table
    header_a = [cell.text.strip() for cell in prev.data.grid[0]]
    header_b = [cell.text.strip() for cell in curr.data.grid[0]]

    # Compare the lists
    return header_a == header_b


def same_table(document,prev_table,curr_table,tolerance=3):

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


def merge_wrapped_rows(df):
    if df.empty or len(df) < 2:
        return df

    new_rows = []
    columns = df.columns
    # Start with the first row as the anchor
    anchor_row = df.iloc[0].copy()

    for i in range(1, len(df)):
        current_row = df.iloc[i]
        
        # Identify columns that actually have non-empty text
        text_cols = [c for c in df.columns if str(current_row[c]).strip()]
        
        # If the row has data in only ONE column, it's a wrap-around
        if len(text_cols) == 1:
            col_idx = text_cols[0]
            
            # Append text to the anchor row in the correct column
            prev_text = str(anchor_row[col_idx])
            wrap_text = str(current_row[col_idx])
            anchor_row[col_idx] = f"{prev_text} {wrap_text}".strip()

        else:
            # It's a full row of data, so save the previous anchor and start a new one
            new_rows.append(anchor_row)
            anchor_row = current_row.copy()

    # Append the final anchor
    new_rows.append(anchor_row)
    
    return pd.DataFrame([r.to_dict() for r in new_rows], columns=columns)


async def format_tables(tables,document):

    model = OpenAIModel()
    all_tables = []

    for table_idx, item in enumerate(tables):
        #One item is one table
        table_data = []
        table_headers = []
        for i,table in enumerate(item['tables']):

            df = table.export_to_dataframe(doc=document)

            if i == 0:
                table_headers = df.columns.tolist()

            else:
                # For continuation fragments, force them to use the master headers
                # This prevents the first row of data from being treated as a header
                df.columns = table_headers 
                
                # Check if the first row of this fragment is just a repetition of the header
                first_row_text = df.iloc[0].astype(str).tolist()
                if first_row_text == table_headers:
                    # It's a repeated header, safe to remove
                    df = df.iloc[1:]
            
            table_data.append(df)

            # df = table.export_to_dataframe(doc=document)

            # if i > 0:
                
            #     """
            #     Instead of only checking whether the first row should be a continuation of the previous page's table, 
            #     should check while the first row only has 1 row of data to combine with the previous table
            #     essentially broadening the table similarity definition
            #     """

            #     table_headers = table_data[0].columns.tolist()
            #     df.columns = table_headers

            #     #Check if first row is headers
            #     first_row = df.iloc[0].tolist()
            #     headers = df.columns
            #     if first_row == headers.tolist():
            #         #Remove headers
            #         df = df.iloc[1:]

            #     while not df.empty:
            #         text_row = df.iloc[0].tolist()
            #         valid_rows = [i for i, val in enumerate(text_row) if str(val).strip()]
            #         text_counter = len(valid_rows)

            #         if text_counter == 1:
            #             # 1. Access the previous DataFrame in the list
            #             prev_df = table_data[-1]
            #             col = valid_rows[0]

            #             # 2. Safely get the existing value (handle NaNs/None)
            #             # .iat is fast, but we need to ensure we don't f-string 'nan'
            #             raw_prev_val = prev_df.iat[-1, col]
            #             prev_val = str(raw_prev_val) if pd.notna(raw_prev_val) else ""

            #             # 3. Merge the text and update the last row of the previous DF
            #             # Using .strip() to avoid leading spaces if prev_val was empty
            #             new_val = f"{prev_val} {str(text_row[col])}".strip()
            #             prev_df.iat[-1, col] = new_val

            #             # 4. Remove the straggler row from the current fragment
            #             df = df.iloc[1:]

            #         else:
            #             break

            # table_data.append(df)
        
        combined_df = pd.concat(table_data,axis=0,ignore_index=True)
        final_df = merge_wrapped_rows(combined_df)
        final_df = final_df.drop_duplicates().reset_index(drop=True)
        final_df.columns = table_headers
        markdown_table = final_df.to_markdown()

        raw_name = document.origin.filename
        document_name = Path(raw_name).stem
        clean_filename = f"{document_name}_Table_{table_idx}.csv"
        save_path = Path(os.getenv('table_results_path')) / clean_filename
        final_df.to_csv(save_path, index=False)

        caption = " ".join(item['caption']).strip()
        # chunk_text = f"Text before table : \n{" ".join(item['prev_text']).strip() if item['prev_text'] else "No text before table"}\n Table : \n{markdown_table if markdown_table.strip() else "No table found"}\n Caption : \n{caption if caption.strip() else "No caption found"}\n Text after table : \n{" ".join(item['post_text']) if item['post_text'] else "No text after table"}\n"
        chunk_text = f"Table : {markdown_table}\nCaption : {caption}"
        chunk_context = await model.get_context(document.export_to_markdown(),chunk_text)

        chunk = TableChunk()
        chunk.document_name = document.origin.filename
        chunk.context = chunk_context
        chunk.content = chunk_text
        chunk.metadata = {'pages' : list(item['pages'])}

        all_tables.append(chunk)

    return all_tables


async def extract_tables(filepath):
    """
    Case 1 : Two tables are detected on the same page. Need to determine whether they are the same table or not
    How? : Check whether the two table headers are the same. If they are not the same, furthermore docling already detected them as separate,
    then the tables are probably different already.

    Case 2 : Table 1 on Page A, Table 2 on Page B. 
    How? : Because the Table 2 fragment may or may not have a table header, check for same number of columns and x alignment.
    Check whether there is any text between the tables on the separate pages. 
    If there is text, most likely different tables. Else, more likely same table.

    Case 3 : Tables separated by more than 1 page.
    How? : Most likely unrelated tables

    """

    docling = DocumentConverter()

    #Object that holds the document
    document = docling.convert(filepath).document

    #All document tables
    document_tables = document.tables

    all_tables = []
    last_seen_table = float('inf')

    for curr_table in document_tables:

        page_no = curr_table.prov[0].page_no
        # prev_text,post_text = get_surrounding_text(document,table)
        caption = curr_table.caption_text(doc=document)

        #One item is one final table
        item = {
            'tables' : [curr_table],
            'caption' : [caption],
            # 'prev_text' : [prev_text],
            # 'post_text' : [post_text],
            'pages' : [page_no],
        }

        should_merge = False
        if all_tables:
            prev_table = all_tables[-1]['tables'][-1]
            page_diff = page_no - last_seen_page

            if page_diff == 0:
                # Case 1: Same page - merge only if headers match
                should_merge = same_headers(prev_table, curr_table)
            
            elif page_diff == 1:
                # Case 2: Consecutive pages - use spatial/text logic
                should_merge = same_table(document, prev_table, curr_table)

            else:
                # Case 3: page_diff > 1, should_merge remains False
                pass

        if should_merge:
            # Unified Merge Logic
            for key in item:
                all_tables[-1][key].extend(item[key])
        else:
            # Unified New Table Logic
            all_tables.append(item)

        last_seen_page = page_no

    cleaned_chunks = await format_tables(all_tables,document)

    return cleaned_chunks


        # prev_table = all_tables[-1]['tables'][-1] if all_tables else None
        # is_same_table = prev_table and (page_no-last_seen_table<=1) and same_table(document,prev_table,table)

        # if is_same_table:
        #     #Same table, just continutation
        #     prev_item = all_tables.pop()
        #     for key in prev_item.keys():
        #         prev_item[key].extend(item[key])
        #     all_tables.append(prev_item)
        # else:
        #     #Different table
        #     all_tables.append(item)

        # last_seen_table = page_no


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

                        print(f'\tFinished getting table chunks\n\n')

                        token_cost = f"Token cost to TABLE chunk {file.name} : {cb.total_tokens}"
                        money_cost = f"Money cost to TABLE chunk {file.name} : {cb.total_cost}"
                        total_cost = [token_cost,money_cost]

                        save_to_file(filename=f'{file.stem}',content=total_cost,filepath=os.getenv('api_costs_path'),method='a')

                    if chunks:

                        returned_chunks = save_document_chunks(file.name,chunks,type='table')

                        print(f'\tFinished saving chunks into postgresdb\n\n')

                        dense_embeddings,cost = await model.embed_texts(returned_chunks)
                        token_cost = f"Token cost to EMBED TABLE {file.name} : {cost[0]}"
                        money_cost = f"Money cost to EMBED TABLE {file.name} : {cost[1]}"
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