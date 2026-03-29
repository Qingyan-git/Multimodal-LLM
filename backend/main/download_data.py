from pathlib import Path
import os
import dotenv
import gdown
from postgre_setups import insert_pdfs

dotenv.load_dotenv()

def delete_all_files_in_folder(folder_path):
    '''
    Deletes all files in the folder specified by folder_path
    '''

    if not Path.exists(folder_path):
        raise FileNotFoundError("Please check that the path entered is correct")

    count = 0
    for file in folder_path.iterdir():
        if file.is_file():
            filename = file.name
            print(f'Removing {filename}...\n')
            file.unlink()
            count += 1

    if count == 0:
        print(f'Folder was empty.')

    else:
        print(f'{count} files removed.\n\n')


def download_folder():
    '''
    Downloads a folder from google drive with the url argument and into the directory 
    specified at output_path. If directory found, deletes all files inside directory
    else, creates directory at path
    '''

    dataset_gdrive_url = os.getenv('dataset_gdrive_url')
    raw_dataset_path = os.getenv('raw_dataset_path')

    if not (dataset_gdrive_url and raw_dataset_path):
        print(f'Environment variables not found, please check your environment files\n')
        raise
    
    raw_dataset_path = Path(raw_dataset_path)

    if not Path.is_dir(raw_dataset_path):
        raw_dataset_path.mkdir(parents=True)
    else:
        delete_all_files_in_folder(raw_dataset_path)

    gdown.download_folder(
        url = dataset_gdrive_url,
        output = str(raw_dataset_path),
        quiet = True,
        use_cookies = True
    )

    print(f'Download successful. {sum(1 for _ in raw_dataset_path.iterdir())} files downloaded.\n\n')

    print(f'Saving all pdfs to postgresql now\n')
    insert_pdfs(raw_dataset_path)
    print(f'Finished saving pdfs to postgresql\n\n')
