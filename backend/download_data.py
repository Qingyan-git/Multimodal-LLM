from pathlib import Path
import os
import dotenv
import gdown

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



def download_folder(url,output_path):
    '''
    Downloads a folder from google drive with the url argument and into the directory 
    specified at output_path. If directory found, deletes all files inside directory
    else, creates directory at path
    '''

    if not Path.is_dir(output_path):
        output_path.mkdir(parents=True)
    else:
        delete_all_files_in_folder(output_path)

    gdown.download_folder(
        url = url,
        output = str(output_path),
        quiet = True,
        use_cookies = True
    )

    print(f'Download successful. {sum(1 for _ in output_path.iterdir())} files downloaded.\n\n')



# if __name__ == '__main__':

dotenv.load_dotenv()
dataset_gdrive_url = os.getenv('dataset_gdrive_url')
raw_dataset_path = os.getenv('raw_dataset_path')

if raw_dataset_path is None:
    raise AttributeError("Environment variable not found, please check your env files \n\n")


download_folder(dataset_gdrive_url,Path(raw_dataset_path))
