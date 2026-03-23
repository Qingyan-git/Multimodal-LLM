import psycopg
import os
import pymupdf
import json
from models.chunk import Chunk


# Setting up functions

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
                    DROP TABLE pdfs CASCADE;
                    """
                )

                cur.execute(
                    """
                    DROP TABLE chunks CASCADE;
                    """
                )


                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pdfs(
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    path TEXT NOT NULL,
                    metadata JSONB NOT NULL, 
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chunks(
                    id SERIAL PRIMARY KEY,
                    document_id INT REFERENCES pdfs(id),
                    type TEXT,
                    content_text TEXT,
                    content_image_data BYTEA,
                    pages INT[],
                    document_name TEXT
                    )
                    """
                )

        print(f'Create tables successful \n\n')

    except psycopg.Error as e:
        print(f'Failed to create table, {e}\n\n')
        raise



#Execution functions

def insert_pdfs(folder_path):

    '''
    Inserts the pdfs found under the directory at folder_path into the postgresql database
    '''

    try:

        if not folder_path.exists():
                raise FileNotFoundError("Please check that the path input is correct")

        with get_connection() as conn:
            with conn.cursor() as cur:
                for file in folder_path.iterdir():
                    if file.is_file() and file.suffix.lower() == '.pdf':
                        print(f'Inserting {file.name} now\n')
                        with pymupdf.open(file) as doc:

                            doc_name = file.stem
                            doc_path = str(file.resolve())
                            doc_metadata = (doc.metadata or {}).copy()

                            cur.execute(
                                """
                                INSERT INTO pdfs (name,path,metadata) VALUES (%s,%s,%s)
                                """,

                                (doc_name,doc_path,json.dumps(doc_metadata))
                            )

        print(f'All pdfs inserted, all done\n\n')

    except psycopg.Error as e:
        print(f'Failed to insert pdf references into database, error {e}\n\n')
        raise



def save_document_chunks(filename,document_chunks):
    """
    Saves chunks into postgres database
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                    SELECT id FROM pdfs WHERE name = %s
                    """,
                    (filename,)
                )

                result = cur.fetchone()
                if result is None:
                    raise ValueError(f"No document found with name: {filename} \n\n")

                document_id = result[0]
                prepared_chunks = [(
                    document_id,
                    chunk.type,
                    chunk.text,
                    chunk.image_data,
                    chunk.pages,
                    filename
                    ) for chunk in document_chunks]

                cur.executemany(
                    """
                    INSERT INTO chunks(document_id,type,content_text,content_image_data,pages,document_name)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    prepared_chunks
                )

    except psycopg.Error as e:
        print(f'Unable to save chunks into postgres database, error {e}')
        raise


def retrieve_chunks():
    """
    Returns the chunks stored in the postgres db
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    SELECT DISTINCT document_id FROM chunks ORDER BY document_id
                    """
                )
                document_ids = cur.fetchall()

                all_chunks = []

                for (document_id,) in document_ids:

                    pdf_chunks = []

                    cur.execute(
                        """
                        SELECT id,type,content_text,content_image_data,pages,document_name FROM chunks WHERE document_id = %s ORDER BY id
                        """,
                        (document_id,)
                    )

                    chunks = cur.fetchall()

                    for row in chunks:
                        chunk = Chunk(
                            id=row[0],
                            type=row[1],
                            text=row[2],
                            image_data=row[3],
                            pages=row[4],
                            document_name=row[5]
                        )
                        pdf_chunks.append(chunk)
                    
                    all_chunks.append(pdf_chunks)
                
                return all_chunks

    except psycopg.Error as e:
        print(f'Unable to retrieve chunks, error {e}')
        raise


