import os
import dotenv
import gdown


def download_folder(url,output_path):

    '''
    downloads a folder from google drive with the url argument and downloads into the output_path 
    argument
    '''

    try:

        old_files = len(os.listdir(output_path))

        gdown.download_folder(
            url = url,
            output = output_path,
            quiet = True,
            use_cookies = True
        )

        new_files = len(os.listdir(output_path))
        uploaded_files = new_files-old_files

        if os.path.exists(output_path) and os.listdir(output_path):
            print(f'Download successful. {uploaded_files} files uploaded.') 
        else:
            raise FileNotFoundError('Please double check the path to the dataset dump folder')

    except Exception as e:
        print(f'An error occured : {e}')

    return

if __name__ == '__main__':

    dotenv.load_dotenv()
    dataset_url = os.getenv('dataset_path')
    dataset_output = os.getenv('dataset_download_path')

    download_folder(dataset_url,dataset_output)
