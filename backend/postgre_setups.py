import psycopg
import dotenv
import os
from pathlib import Path
import pymupdf
import json


# Setting up functions

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



def create_db_tables():

    """
    Creates tables for database
    """

    try : 
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pdfs(
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
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
                    text TEXT NOT NULL,
                    pages INT[] NOT NULL
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


def save_chunks(all_chunks):

    """
    Saves chunks into postgres database
    """

    try : 
        with get_connection() as conn:
            with conn.cursor() as cur:
                for doc_chunks in all_chunks:
                    
                    document_name = doc_chunks['name']

                    cur.execute(
                        """
                        SELECT id FROM pdfs WHERE name = %s
                        """,
                        (document_name,)
                    )

                    result = cur.fetchone()

                    if result is None:
                        raise ValueError(f"No document found with name: {document_name} \n\n")

                    document_id = result[0]
                    document_chunks = doc_chunks['chunks']
                    prepared_chunks = [(document_id, chunk['text'], chunk['pages'])
                                       for chunk in document_chunks
                                        ]
                    
                    print(f'Inserting chunks for document  {document_name}\n')
                    
                    cur.executemany(
                        """
                        INSERT INTO chunks(document_id, text, pages) VALUES (%s,%s,%s)
                        """,
                        prepared_chunks
                    )

                    print(f'Chunks inserted\n\n')


    except psycopg.Error as e:
        print(f'Failed to save chunks into database, error : {e}')
        raise


def retrieve_chunks():
    """
    Retrieves chunks from postgres database
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    SELECT DISTINCT document_id FROM chunks ORDER BY document_id
                    """
                )
                document_ids = [row[0] for row in cur.fetchall()]

                documents = []
                for document_id in document_ids:

                    document = {
                        'name' : "",
                        'metadata' : {},
                        'chunks' : [],
                    }

                    cur.execute(
                        """
                        SELECT name,metadata FROM pdfs WHERE id = %s
                        """,
                        (document_id,)
                    )
                    document_ref = cur.fetchone()

                    if document_ref is None:
                        raise ValueError(f"No document found with id: {document_id} \n\n")

                    document_name,document_metadata = document_ref
                    document['name'] = document_name
                    document['metadata'] = document_metadata

                    print(f'Chunks retrieved for document {document_name}\n')

                    cur.execute(
                        """
                        SELECT id, text, pages FROM chunks WHERE document_id = %s ORDER BY id
                        """,
                        (document_id,)
                    )
                    for chunk in cur:
                        chunk_id,text,pages = chunk

                        document['chunks'].append({
                            'id' : chunk_id,
                            'text' : text,
                            'pages' : pages
                        })

                    documents.append(document)

        print(f'All chunks retrieved\n\n')
        return documents
    

    except psycopg.Error as e:
        print(f'Failed to retrive chunks from database, error : {e}')
        raise



# if __name__ == '__main__':

dotenv.load_dotenv()
create_llm_db()
create_db_tables()

raw_dataset_path=os.getenv('raw_dataset_path')
if raw_dataset_path is None:
    raise ValueError("Environment variable not found. Please check your environment variables")

insert_pdfs(Path(raw_dataset_path))
