import psycopg
import os
import json
from pathlib import Path

from chunks import Chunk

def get_connection():

    '''
    Gets and returns connection object for the llm database
    '''

    try :

        user = os.getenv('postgres_user')
        password = os.getenv('postgres_password')
        db_name = os.getenv('postgres_llm_db_name')

        if not user or not password or not db_name:
            raise AttributeError("Environment variable not found, please check your environment files\n\n")

        host = 'localhost'
        port = 5432

        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=db_name,
            user=user,
            password=password
        )

        return conn

    except  psycopg.Error as e: 
        print(f'Database connection failed, error : {e}\n\n')
        raise


def create_llm_db():

    '''
    Checks for the existence of the db_name in postgresql. If not exist, create the db
    '''

    try :

        user = os.getenv('postgres_user')
        password = os.getenv('postgres_password')
        admin_db_name = os.getenv('postgres_admin_db_name')
        llm_db_name = os.getenv('postgres_llm_db_name')

        if not user or not password or not admin_db_name:
            raise AttributeError("Environment variable(s) not found, please check your environment files\n\n")
        
        host = 'localhost'
        port = 5432

        with psycopg.connect(
            host=host,
            port=port,
            dbname=admin_db_name,
            user=user,
            password=password
        ) as conn:
            
            print(f'Connected to db {admin_db_name}\n')

            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT 1 FROM pg_database WHERE datname = %s',
                    (llm_db_name,)
                )

                exists = cursor.fetchone()

                if not exists:
                    print(f'Database {llm_db_name} does not exist, creating database now \n')
                    cursor.execute(f'CREATE DATABASE "{llm_db_name}"') #type:ignore
                    print('Database created \n\n')

                else:
                    print(f'Database {llm_db_name} already exists\n\n')


    except psycopg.Error as e:
        print(f'Cannot create the llm_db, error : {e}\n\n')
        raise


def create_db_tables():

    """
    Creates tables for database
    """

    try : 
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                """
                DROP TABLE pdfs CASCADE
                """) 

                cur.execute(
                """
                DROP TABLE chunks CASCADE
                """)

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pdfs(
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    path TEXT NOT NULL
                    )
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chunks(
                    id SERIAL PRIMARY KEY,
                    document_name TEXT REFERENCES pdfs(name),
                    type TEXT,
                    context TEXT,
                    content TEXT,
                    metadata JSONB DEFAULT '{}'
                    )
                    """
                )

        print(f'Create tables successful \n\n')

    except psycopg.Error as e:
        print(f'Failed to create table, {e}\n\n')
        raise


def delete_rows():

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                """
                TRUNCATE TABLE pdfs CASCADE
                """) 

                cur.execute(
                """
                TRUNCATE TABLE chunks CASCADE
                """)

                print(f'\nAll rows from tables deleted\n\n')

    except psycopg.Error as e:
        print(f'Failed to delete rows from tables, {e}\n\n')


def delete_chunks(type):
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    DELETE FROM chunks WHERE type = %s
                    """,
                    (type,)
                )

                print(f'\nAll chunks of type : {type} deleted\n\n')

    except psycopg.Error as e:
        print(f'Failed to delete rows from tables, {e}\n\n')



#Execution functions

def insert_pdfs(filepath):

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                name = filepath.name
                path = str(filepath)

                cur.execute(
                    """
                    INSERT INTO pdfs (name,path) VALUES (%s,%s) ON CONFLICT (name) DO NOTHING
                    """,
                    (name,path)
                )

    except psycopg.Error as e:
        print(f'Failed to insert pdfs into database, error {e}\n\n')
        raise


def retrieve_pdf(document_name):
    try:
        with get_conection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    SELECT path FROM pdfs WHERE name = %s
                    """,
                    (document_name,)
                )

                result = cur.fetchone()

                if result:
                    return Path(result[0])

                else:
                    print(f"Warning: Document '{document_name}' not found in database.")
                    return None

    except psycopg.Error as e:
        print(f'Unable to retrieve pdf {document_name}, error {e}\n\n')
        raise


def save_document_chunks(document_name,document_chunks,type):

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                prepared_chunks = [(
                    chunk.document_name,
                    chunk.type,
                    chunk.context,
                    chunk.content,
                    json.dumps(chunk.metadata),
                    ) for chunk in document_chunks]

                cur.executemany(
                    """
                    INSERT INTO chunks(document_name,type,context,content,metadata)
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    prepared_chunks
                )

        retrieved_chunks = retrieve_document_chunks(document_name,type)

        return retrieved_chunks

    except psycopg.Error as e:
        print(f'Unable to save chunks into postgres database, error {e}')
        raise


def retrieve_document_chunks(document_name,type):

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                    SELECT id,document_name,type,context,content,metadata FROM chunks WHERE document_name = %s AND type = %s
                    """,
                    (document_name,type,)
                )

                results = cur.fetchall()
                chunks = []
                for result in results:
                    chunk = Chunk()
                    chunk.id = result[0]
                    chunk.document_name = result[1]
                    chunk.type = result[2]
                    chunk.context = result[3]
                    chunk.content = result[4]
                    chunk.metadata = result[5]
                    chunks.append(chunk)

                return chunks

    except psycopg.Error as e:
        print(f'Unable to retrieve chunks from postgres, error {e}')
        raise


if __name__ == '__main__':

    reformat = 1
    sure = input('Are you sure? Enter Y to continue : ')

    if sure == 'Y':

        if reformat:
            create_db_tables()

        chunk_type = 0
        chunks = {
            0:None,
            1:'text',
            2:'image',
            3:'tables'
        }
        if chunks[chunk_type]:
            delete_chunks(chunk_type)

    else:
        print('Aborted\n\n')


