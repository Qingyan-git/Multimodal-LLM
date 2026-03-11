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

    print(f'{count} files removed.\n\n') 



def download_folder(url,output_path):
    '''
    Downloads a folder from google drive with the url argument and into the folder 
    specified at output_path 
    '''

    if not Path.is_dir(output_path):
        print(f"Directory not found at {output_path}, creating directory now\n")
        output_path.mkdir(parents=True)

    old_files = len(list(Path.iterdir(output_path)))

    gdown.download_folder(
        url = url,
        output = output_path,
        quiet = True,
        use_cookies = True
    )

    new_files = len(list(Path.iterdir(output_path)))
    uploaded_files = new_files-old_files

    print(f'Download successful. {uploaded_files} files downloaded.\n\n')



# if __name__ == '__main__':

dotenv.load_dotenv()
dataset_url = os.getenv('dataset_path')
dataset_output = Path(os.getenv('dataset_download_path'))

delete_all_files_in_folder(dataset_output)
download_folder(dataset_url,dataset_output)
