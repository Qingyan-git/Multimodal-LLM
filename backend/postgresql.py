import psycopg
import dotenv
import os
from pathlib import Path
import pymupdf
#from psycopg.extras import execute_values


def create_llm_db(user,password,db_name,host='localhost',port=5432):

    '''
    Checks for the existence of the db_name in postgresql. If not exist, create the db.
    Returns the conn for the db
    '''

    with psycopg.connect(
        host=host,
        port=port,
        dbname='postgres',
        user=user,
        password=password
    ) as conn:
        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT 1 FROM pg_database WHERE datname = %s',
                (db_name,)
            )

            exists = cursor.fetchone()

            if not exists:
                print(f'Database {db_name} does not exist, creating database now \n')
                cursor.execute(
                    'CREATE DATABASE {db_name}'
                )
                print('Database created \n\n')



def get_connection(user:str,password:str,db_name:str,host:str='localhost',port:int=5432):

    '''
    Gets and returns connection object for the database as specified
    '''

    try :
        conn = psycopg.connect(
            host=host,
            post=port,
            dbname=db_name,
            user=user,
            password=password
        )

        return conn

    except  psycopg.Error as e: 
        print(f'Database connection failed, error : {e}')
        raise



def create_db_tables():
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
                    pages INT[] NOT NULL,
                    )
                    """
                )

            conn.commit()

        print(f'Create tables successful \n\n')

    except psycopg.Error as e:
        print(f'Failed to create table, {e}')
        raise


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
                        print(f'Inserting {file.name}.pdf now\n')
                        with pymupdf.open(file) as doc:

                            doc_name = file.stem
                            doc_path = str(file.resolve())
                            doc_metadata = doc.metadata.copy()

                            cur.execute(
                                """
                                INSERT INTO pdfs (name,path,metadata) VALUES (%s,%s,%s)
                                """,

                                (doc_name,doc_path,doc_metadata)
                            )

                
                conn.commit()

        print(f'All pdfs inserted, all done\n\n')

    except psycopg.Error as e:
        print(f'Failed to insert pdf references into database, error {e}\n\n')
        raise



def save_chunks(all_chunks):
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

                    document_id = cur.fetchone()[0]
                    document_chunks = doc_chunks['chunks']
                    prepared_chunks = [(document_id, chunk['text'], chunk['pages'])
                                       for chunk in document_chunks
                                        ]
                    
                    psycopg.extras.execute_values(
                        cur,
                        """
                        INSERT INTO chunks(document_id, text, pages) VALUES %s
                        """,
                        prepared_chunks
                    )


    except psycopg.Error as e:
        print(f'Failed to save chunks into database, error : {e}')
        raise




# if __name__ == '__main__':

dotenv.load_dotenv()

postgresql_user = os.getenv('postgresql_user')
postgresql_password = os.getenv('postgresql_password')
