from pathlib import Path
import re
import os
import dotenv
import pymupdf4llm
import pymupdf
import asyncio
import traceback

from langchain_text_splitters import RecursiveCharacterTextSplitter

from llms_and_models import OpenAIModel
from chunks import TextChunk,ImageChunk
from postgres import save_document_chunks, insert_pdf
from qdrant import upload_to_qdrant, format_embeddings



def same_table(prev_page,prev_table,curr_page,curr_table,tolerance=5):

    if prev_table.col_count != curr_table.col_count:
        return False

    prev_bbox = prev_table.bbox[0]
    curr_bbox = curr_table.bbox[0]

    if abs(prev_bbox[0]-curr_bbox[0]) > tolerance or abs(prev_bbox[2]-curr_bbox[2]) > tolerance:
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

    table_texts = top_text + "\n" + bottom_text

    return table_texts



def extract_tables(filepath):

    all_tables = []
    last_seen_table = 0

    with pymupdf.open(filepath) as doc:

        full_text = pymupdf4llm.to_markdown(doc)

        for page_no, page in enumerate(doc):  # iterate over the pages

            tables.tables = page.find_tables()

            for i,table in enumerate(tables):

                item = {
                        'table_texts' : [],
                        'table' : [],
                    }

                if page_no - last_seen_table == 0 or page_no - last_seen_table > 1 or i == 0:
                    #Not possible for tables to be continutation of the previous page
                    table_texts = get_table_caption(page,table)
                    last_seen_tables = page_no

                    item.table_texts.append(table_texts)
                    item.table.append(table)
                    all_tables.append(item)

                else:
                    #Possible for tables to be continutations of each other
                    prev_table_item = all_tables.pop()
                    most_recent_table = prev_table_item['table'][-1]
                    same_table = same_table(last_seen_tables, most_recent_table, page_no, table)

                    if same_table:
                        #The previous table and the current one is deemed to be the same table split amongst pages

                        """
                        store the table texts and table in their respective locations
                        cannot concatenate as pandas because need to do the same_table() function on pymupdf Page
                        objects, so only append on the most recent table 
                        """
                        
                        pass
                    else:
                        #Table on previous page is distinct than table on current page
                        table_texts = get_table_caption(page,table)
                        last_seen_tables = page_no

                        item.table_texts.append(table_texts)
                        item.table.append(table)
                        all_tables.append(item)

    """
    for each table in all_tables convert to pandas and concatenate same tables
    markdown the concatenated pandas tables for each table so that it processed as text and as one table
    format into TableChunk data structure for sql insertion
    """
