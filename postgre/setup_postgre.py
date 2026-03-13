import psycopg
import dotenv
import os


def get_llm_db(user,password,db_name='multimodal_llm',host='localhost',port=5432):

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
                print(f'Database {db_name} does not exist, creating database now'\n)
                cursor.execute(
                    'CREATE DATABASE {db_name}'
                )
                print('Database created'\n\n)
                      
    
    db_conn = psycopg.connect(
        host=host,
        port=port,
        dbname=db_name,
        user=user,
        password=password
    )

    return db_conn






# if __name__ == '__main__':

dotenv.load_dotenv()

postgresql_user = os.getenv('postgresql_user')
postgresql_password = os.getenv('postgresql_password')
